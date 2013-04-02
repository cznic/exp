// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package dbm

//DONE +Top level Sync? Optional? (Measure it)
//	Too slow. Added db.Sync() instead.

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"runtime"
	"sync"

	"github.com/cznic/exp/lldb"
)

// AboveUnicode is a scalar list collating after any valid UTF-8 string.
//
//BUG(jnml) This is a temporary hack and should probably go away when Slice is
//properly implemented.
var AboveUnicode = []interface{}{"\xff"}

const (
	aCacheSize = 500
	fCacheSize = 500
	sCacheSize = 50

	rname        = "2remove" // Array shredder
	arraysPrefix = "a"
	filesPrefix  = "f"
	systemPrefix = "s"
)

var (
	compress = true // Curious developer hooks
)

type DB struct {
	_root      *Array          // Root directory, do not access directly
	acache     treeCache       // Arrays cache
	alloc      *lldb.Allocator // The machinery. Wraps filer
	bkl        sync.Mutex      // Big Kernel Lock
	closeMu    sync.Mutex      // Close() coordination
	closed     bool            // it was
	emptySize  int64           // Any header size including FLT.
	f          *os.File        // Underlying file. Potentially nil (if filer is lldb.MemFiler)
	fcache     treeCache       // Files cache
	filer      lldb.Filer      // Wraps f
	removing   map[int64]bool  // BTrees being removed
	removingMu sync.Mutex      // Remove() coordination
	scache     treeCache       // System arrays cache
	stop       chan int        // Remove() coordination
	wg         sync.WaitGroup  // Remove() coordination
}

// Create creates the named DB file mode 0666 (before umask). The file must not
// already exist. If successful, methods on the returned DB can be used for
// I/O; the associated file descriptor has mode os.O_RDWR. If there is an
// error, it will be of type *os.PathError.
func Create(name string) (db *DB, err error) {
	return create(os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666))
}

func create(f *os.File, e error) (db *DB, err error) {
	if err = e; err != nil {
		return
	}

	return create2(f, lldb.NewSimpleFileFiler(f))
}

func create2(f *os.File, filer lldb.Filer) (db *DB, err error) {
	b := [16]byte{0x90, 0xdb, 0xf1, 0x1e, 0x00} // ver 0x00
	if n, err := filer.WriteAt(b[:], 0); n != 16 {
		return nil, &os.PathError{Op: "dbm.Create.WriteAt", Path: filer.Name(), Err: err}
	}

	db = &DB{emptySize: 128, f: f, filer: filer}

	if db.alloc, err = lldb.NewFLTAllocator(lldb.NewInnerFiler(filer, 16), lldb.FLTPowersOf2); err != nil {
		db, err = nil, &os.PathError{Op: "dbm.Create", Path: filer.Name(), Err: err}
	}

	db.alloc.Compress = compress

	return
}

// CreateMem creates an in-memory DB not backed by a disk file.  Memory DBs are
// resource limited as they are completely held in memory and are not
// automatically persisted.
func CreateMem() (db *DB, err error) {
	return create2(nil, lldb.NewMemFiler())
}

// CreateTemp creates a new temporary DB in the directory dir with a name
// beginning with prefix. If dir is the empty string, CreateTemp uses the
// default directory for temporary files (see os.TempDir). Multiple programs
// calling CreateTemp simultaneously will not choose the same file name for the
// DB. The caller can use Name() to find the pathname of the DB file. It is the
// caller's responsibility to remove the file when no longer needed.
func CreateTemp(dir, prefix string) (db *DB, err error) {
	return create(ioutil.TempFile(dir, prefix))
}

// Open opens the named DB file for reading/writing. If successful, methods on
// the returned DB can be used for I/O; the associated file descriptor has mode
// os.O_RDWR. If there is an error, it will be of type *os.PathError.
func Open(name string) (db *DB, err error) {
	f, err := os.OpenFile(name, os.O_RDWR, 0666)
	if err != nil {
		return
	}

	filer := lldb.NewSimpleFileFiler(f)

	if sz := filer.Size(); sz%16 != 0 {
		return nil, &os.PathError{Op: "dbm.Open:", Path: name, Err: fmt.Errorf("file size %d(%#x) is not 0 (mod 16)", sz, sz)}
	}

	var b [16]byte
	if n, err := filer.ReadAt(b[:], 0); n != 16 || err != nil {
		return nil, &os.PathError{Op: "dbm.Open.ReadAt", Path: name, Err: err}
	}

	var h header
	if err = h.rd(b[:]); err != nil {
		return nil, &os.PathError{Op: "dbm.Open:validate header", Path: name, Err: err}
	}

	db = &DB{f: f, filer: filer}
	switch h.ver {
	default:
		return nil, &os.PathError{Op: "dbm.Open", Path: name, Err: fmt.Errorf("unknown dbm file format version %#x", h.ver)}
	case 0x00:
		return open00(name, db)
	}

}

// Close closes the DB, rendering it unusable for I/O. It returns an error, if
// any. Failing to call Close before exiting a program can render the DB
// unusable.  A dbm client should install signal handlers and ensure Close is
// called on receiving of, for example, the SIGINT signal.
//
// Close is idempotent.
func (db *DB) Close() (err error) {
	db.enter()

	if db.closed {
		db.leave()
		return
	}

	db.closed = true
	db.closeMu.Lock()
	defer db.closeMu.Unlock()

	db.leave()
	return db.close()
}

func (db *DB) close() (err error) {
	if db.stop != nil {
		close(db.stop)
		db.wg.Wait()
		db.stop = nil
	}

	if db.f == nil { // lldb.MemFiler
		return
	}

	err = db.f.Sync()
	if err2 := db.filer.Close(); err2 != nil && err == nil {
		err = err2
	}
	return
}

func (db *DB) root() (r *Array, err error) {
	if r = db._root; r != nil {
		return
	}

	sz := db.filer.Size()
	switch {
	case sz < db.emptySize:
		panic(fmt.Errorf("internal error: %d", sz))
	case sz == db.emptySize:
		tree, h, err := lldb.CreateBTree(db.alloc, collate)
		if err != nil {
			return nil, err
		}

		if h != 1 {
			panic("internal error")
		}

		r = &Array{db, tree, nil, "", 0}
		db._root = r
		return r, nil
	default:
		tree, err := lldb.OpenBTree(db.alloc, collate, 1)
		if err != nil {
			return nil, err
		}

		r = &Array{db, tree, nil, "", 0}
		db._root = r
		return r, nil
	}
}

// Array returns an Array associated with a subtree of array, determined by
// subscripts.
func (db *DB) Array(array string, subscripts ...interface{}) (a Array, err error) {
	db.enter()
	defer db.leave()

	return db.array_(false, array, subscripts...)
}

func (db *DB) array_(canCreate bool, array string, subscripts ...interface{}) (a Array, err error) {
	a.db = db
	if a, err = a.array(subscripts...); err != nil {
		return
	}

	a.tree, err = db.acache.getTree(db, arraysPrefix, array, canCreate, aCacheSize)
	a.name = array
	a.namespace = 'a'
	return
}

func (db *DB) sysArray(canCreate bool, array string) (a Array, err error) {
	a.db = db
	a.tree, err = db.scache.getTree(db, systemPrefix, array, canCreate, sCacheSize)
	a.name = array
	a.namespace = 's'
	return a, err
}

func (db *DB) fileArray(canCreate bool, name string) (f File, err error) {
	var a Array
	a.db = db
	a.tree, err = db.fcache.getTree(db, filesPrefix, name, canCreate, fCacheSize)
	a.name = name
	a.namespace = 'f'
	return File(a), err
}

// Set sets the value at subscripts in array. Any previous value, if existed,
// is overwritten by the new one.
func (db *DB) Set(value interface{}, array string, subscripts ...interface{}) (err error) {
	db.enter()
	defer db.leave()

	a, err := db.array_(true, array, subscripts...)
	if err != nil {
		return
	}

	return a.set(value)
}

// Get returns the value at subscripts in array, or nil if no such value
// exists.
func (db *DB) Get(array string, subscripts ...interface{}) (value interface{}, err error) {
	db.enter()
	defer db.leave()

	a, err := db.array_(false, array, subscripts...)
	if a.tree == nil || err != nil {
		return
	}

	return a.get()
}

// Slice returns a new Slice of array, with a subscripts range of [from, to].
// If from is nil it works as 'from lowest existing key'.  If to is nil it
// works as 'to highest existing key'.
//
//BUG(jnml) Semantics of an empty limit differs for non prefixed arrays vs
//prefixed ones.
func (db *DB) Slice(array string, subscripts, from, to []interface{}) (s *Slice, err error) {
	db.enter()
	defer db.leave()

	a, err := db.array_(false, array, subscripts...)
	if a.tree == nil || err != nil {
		return
	}

	return a.Slice(from, to)
}

// Delete deletes the value at subscripts in array.
func (db *DB) Delete(array string, subscripts ...interface{}) (err error) {
	db.enter()
	defer db.leave()

	a, err := db.array_(false, array, subscripts...)
	if a.tree == nil || err != nil {
		return
	}

	return a.delete(subscripts...)
}

// Clear empties the subtree at subscripts in array.
func (db *DB) Clear(array string, subscripts ...interface{}) (err error) {
	db.enter()

	a, err := db.array_(false, array, subscripts...)
	if a.tree == nil || err != nil {
		db.leave()
		return
	}

	db.leave()
	return a.Clear()
}

// Name returns the name of the DB file.
func (db *DB) Name() string {
	return db.filer.Name()
}

func (db *DB) setRemoving(h int64, flag bool) (r bool) {
	db.removingMu.Lock()
	defer db.removingMu.Unlock()

	if db.removing == nil {
		db.removing = map[int64]bool{h: flag}
		return
	}

	r = db.removing[h]
	db.removing[h] = flag
	return
}

// RemoveArray removes array from the DB.
func (db *DB) RemoveArray(array string) (err error) {
	db.enter()
	defer db.leave()

	return db.removeArray(arraysPrefix, array)
}

// RemoveFile removes file from the DB.
func (db *DB) RemoveFile(file string) (err error) {
	db.enter()
	defer db.leave()

	return db.removeArray(filesPrefix, file)
}

func (db *DB) removeArray(prefix, array string) (err error) {
	if db.stop == nil {
		db.stop = make(chan int)
	}

	t, err := db.acache.getTree(db, prefix, array, false, aCacheSize)
	if t == nil || err != nil {
		return
	}

	h := t.Handle()
	if db.setRemoving(h, true) {
		return
	}

	delete(db.acache, array)

	root, err := db.root()
	if err != nil {
		return
	}

	removes, err := db.sysArray(true, rname)
	if err != nil {
		return
	}

	if err = removes.set(nil, h); err != nil {
		return
	}

	if err = root.delete(prefix, array); err != nil {
		return
	}

	db.wg.Add(1)
	go db.victor(removes, h)

	return
}

func (db *DB) boot() (err error) {
	//TODO+ wiping of {"a", "/tmp"}, {"f", "/tmp"}
	removes, err := db.sysArray(false, rname)
	if removes.tree == nil || err != nil {
		return
	}

	s, err := removes.Slice(nil, nil)
	if err != nil {
		return noEof(err)
	}

	var a []int64
	s.Do(func(subscripts, value []interface{}) (r bool, err error) {
		r = true
		switch {
		case len(subscripts) == 1:
			h, ok := subscripts[0].(int64)
			if ok {
				a = append(a, h)
				return
			}

			fallthrough
		default:
			err = removes.Delete(subscripts)
			return
		}
	})

	if db.stop == nil {
		db.stop = make(chan int)
	}

	for _, h := range a {
		if db.setRemoving(h, true) {
			continue
		}

		db.wg.Add(1)
		go db.victor(removes, h)
	}

	return
}

func (db *DB) victor(removes Array, h int64) {
	var finished bool
	defer func() {
		if finished {
			db.enter()
			lldb.RemoveBTree(db.alloc, h)
			removes.delete(h)
			db.setRemoving(h, false)
			db.leave()
		}
		db.wg.Done()
	}()

	db.bkl.Lock()
	t, err := lldb.OpenBTree(db.alloc, nil, h)
	if err != nil {
		db.bkl.Unlock()
		finished = true
		return
	}

	//TODO(jnml) Later/performance: Add lldb.DeleteAnyKey()
	db.bkl.Unlock()
	for {
		runtime.Gosched()
		select {
		case _, ok := <-db.stop:
			if !ok {
				return
			}
		default:
		}

		db.bkl.Lock()
		k, v, err := t.Last()
		if err != nil {
			db.bkl.Unlock()
			finished = err == io.EOF
			return
		}

		if k == nil && v == nil {
			db.bkl.Unlock()
			finished = true
			return
		}

		if err := t.Delete(k); err != nil {
			db.leave()
			return
		}

		db.leave()
	}
}

// Arrays returns a read-only meta array which registers other arrays by name
// as its keys. The associated values are meaningless but non-nil if the value
// exists.
func (db *DB) Arrays() (a Array, err error) {
	db.enter()
	defer db.leave()

	p, err := db.root()
	if err != nil {
		return a, err
	}

	return p.array(arraysPrefix)
}

// Files returns a read-only meta array which registers all Files in the DB by
// name as its keys. The associated values are meaningless but non-nil if the
// value exists.
func (db *DB) Files() (a Array, err error) {
	db.enter()
	defer db.leave()

	p, err := db.root()
	if err != nil {
		return a, err
	}

	return p.array(filesPrefix)
}

func (db *DB) enter() {
	db.bkl.Lock()
}

func (db *DB) leave() {
	db.bkl.Unlock()
}

// Sync commits the current contents of the DB file to stable storage.
// Typically, this means flushing the file system's in-memory copy of recently
// written data to disk.
func (db *DB) Sync() (err error) {
	if db.f != nil {
		db.enter()
		defer db.leave()
		err = db.f.Sync()
	}
	return
}

// File returns a File associated with name.
func (db *DB) File(name string) (f File) {
	f, err := db.fileArray(false, name)
	if err != nil {
		panic("internal error")
	}

	return
}

// Inc atomically increments the value at subscripts of array by delta and
// returns the new value. If the value doesn't exists before calling Inc or if
// the value is not an integer then the value is considered to be zero.
func (db *DB) Inc(delta int64, array string, subscripts ...interface{}) (val int64, err error) {
	db.enter()
	defer db.leave()

	a, err := db.array_(true, array, subscripts...)
	if err != nil {
		return
	}

	return a.inc(delta)
}
