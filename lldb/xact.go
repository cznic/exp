// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Structural transactions.

package lldb

import (
	"fmt"
	"io"

	"github.com/cznic/mathutil"
)

var bitmask = [8]byte{1, 2, 4, 8, 16, 32, 64, 128}

const (
	bfBits = 8 //TODO benchmark tune
	bfSize = 1 << bfBits
	bfMask = bfSize - 1
)

var (
	_           Filer = &bitFiler{} // Ensure bitFiler is a Filer.
	bitZeroPage bitPage
)

type (
	bitPage struct {
		data  [bfSize]byte
		dirty [bfSize >> 3]byte
	}

	bitFilerMap map[int64]*bitPage

	bitFiler struct {
		m    bitFilerMap
		size int64
	}
)

func newBitFiler() *bitFiler {
	return &bitFiler{m: bitFilerMap{}}
}

func (f *bitFiler) BeginUpdate()                          { panic("internal error") }
func (f *bitFiler) Close() (err error)                    { return }
func (f *bitFiler) EndUpdate() (err error)                { panic("internal error") }
func (f *bitFiler) Name() string                          { return fmt.Sprintf("%p.bitfiler", f) }
func (f *bitFiler) PunchHole(off, size int64) (err error) { panic("internal error") } //TODO think about this more
func (f *bitFiler) Rollback() (err error)                 { panic("internal error") }

func (f *bitFiler) ReadAt(b []byte, off int64) (n int, err error) {
	avail := f.size - off
	pgI := off >> bfBits
	pgO := int(off & bfMask)
	rem := len(b)
	if int64(rem) >= avail {
		rem = int(avail)
		err = io.EOF
	}
	for rem != 0 && avail > 0 {
		pg := f.m[pgI]
		if pg == nil {
			pg = &bitZeroPage
		}
		nc := copy(b[:mathutil.Min(rem, bfSize)], pg.data[pgO:])
		pgI++
		pgO = 0
		rem -= nc
		n += nc
		b = b[nc:]
	}
	return
}

func (f *bitFiler) Size() (int64, error) { return f.size, nil }

func (f *bitFiler) Truncate(size int64) (err error) {
	switch {
	case size < 0:
		return &ErrINVAL{"Truncate size", size}
	case size == 0:
		f.m = bitFilerMap{}
	}

	first := size >> bfBits
	if size&bfMask != 0 {
		first++
	}
	last := f.size >> bfBits
	if f.size&bfMask != 0 {
		last++
	}
	for ; first < last; first++ {
		delete(f.m, first)
	}

	f.size = size
	return
}

func (f *bitFiler) writeAt(b []byte, off int64, dirty bool) (n int, err error) {
	pgI := off >> bfBits
	pgO := int(off & bfMask)
	n = len(b)
	rem := n
	var nc int
	for rem != 0 {
		pg := f.m[pgI]
		if pg == nil {
			pg = &bitPage{}
			f.m[pgI] = pg
		}
		nc = copy(pg.data[pgO:], b)
		pgI++
		pgO = 0
		rem -= nc
		b = b[nc:]
		if dirty {
			for i := pgO; i < pgO+nc; i++ {
				pg.dirty[i>>3] |= bitmask[i&7]
			}
		}
	}
	f.size = mathutil.MaxInt64(f.size, off+int64(n))
	return
}
func (f *bitFiler) WriteAt(b []byte, off int64) (n int, err error) {
	return f.writeAt(b, off, true)
}

// uRollbackFiler is a Filer implementing structural transaction handling.
// Structural transactions should be small and fast because all non committed
// data are held in memory until committed or discarded by a Rollback.
//
// First approximation: uRollbackFiler starts in non transactional mode
// and it updates are written directly through the Filer it wraps. On
// BeginUpdate a transaction is initiated and all updates are held in memory,
// they're not anymore written to the wrapped Filer. On a matching EndUpdate
// the updates held in memory are actually written to the wrapped Filer.
//
// Going into more details: Transactions can nest. The above described rollback
// mechanism works the same for every nesting level, but the physical write to
// the wrapped Filer happens only when the outer most transaction nesting
// level is closed (or if there are writes outside of any transaction).
//
// Invoking Rollback is an alternative to EndUpdate. It discards all changes
// made at the current transaction level and returns the "state" of the Filer
// to what it was before the corresponding BeginUpdate.
//
// During an open transaction, all reads (using ReadAt) are "dirty" reads,
// seeing the uncommited changes made to the Filer's data.
//
// Lldb databases should be based upon uRollbackFiler. With a MemFiler one gets
// transactional memory. With a disk based SimpleFileFiler it protects against
// at least some HW errors - if Rollback is properly invoked on such failures.
//
// The "real" writes to the wrapped Filer goes through the method value Writer.
// It can be replaced by other value when for example a write ahead log is
// desired and/or to implement a two phase commit etc.
//
// List of functions/methods which are a good candidate to wrap in a
// BeginUpdate/EndUpdate structural transaction:
//
// 	Allocator.Alloc
// 	Allocator.Free
// 	Allocator.Realloc
//
// 	RemoveBTree
// 	BTree.Clear
// 	BTree.Delete
// 	BTree.DeleteAny
// 	BTree.Clear
// 	BTree.Extract
// 	BTree.Get (it can mutate the DB)
// 	BTree.Put
// 	BTree.Set
//
// NOTE: uRollbackFiler is a generic solution intended to wrap Filers provided
// by this package which do not implement any of the transactional methods.
// uRollbackFiler thus _does not_ invoke any of the transactional methods of its
// wrapped Filer.
//
// NOTE2: Using uRollbackFiler, but failing to ever invoke BeginUpdate/EndUpdate
// or Rollback will cause Checkpoint to be never called. However, there's the
// possibility to call uRollbackFiler's Checkpoint from e.g. a timer tick
// mandated goroutine.
type uRollbackFiler struct {
	// Checkpoint, if non nil, is called after closing (by EndUpdate) the
	// upper most level open transaction if all calls to Writer were
	// sucessfull and the DB is this now in a consistent state (in the
	// ideal world with no write caches, no HW failures, no process
	// crashes, ...). The sz parameter defines the Filer's size should be
	// at this checkpoint.  Calling Filer's Truncate(sz), as the last thing
	// should be normally sufficient.
	Checkpoint func(f Filer, sz int64) error
	// Writer is used to do the real updating of the wrapped Filer.
	Writer func(b []byte, off int64) (int, error)
	f      Filer // Always the original one
	parent *uRollbackFiler
	size   int64
	closed bool
	//TODO m      map[int64]rPage
}

// NewRollbackFiler returns a uRollbackFiler wrapping f.
func uNewRollbackFiler(f Filer) (r *uRollbackFiler, err error) {
	sz, err := f.Size()
	if err != nil {
		return
	}

	return &uRollbackFiler{f: f, Writer: f.WriteAt, size: sz}, nil
}

func (r *uRollbackFiler) inTransaction() bool {
	return r.parent != nil
}

// Implements Filer.
func (r *uRollbackFiler) BeginUpdate() {
	panic("TODO")
}

// Implements Filer.
//
// Close will return an error if not invoked at nesting level 0.  However, to
// allow emergency closing from eg. a signal handler; if Close is invoked
// within an open transaction, it rollbacks it first.
func (r *uRollbackFiler) Close() (err error) {
	r.parent = nil
	if !r.closed {
		r.closed = true
		err = r.f.Close()
		if err != nil {
			return
		}
	}

	if r.inTransaction() {
		return &ErrPERM{(r.f.Name() + ":Close")}
	}

	return r.f.Close()
}

// Implements Filer.
func (r *uRollbackFiler) EndUpdate() error {
	if !r.inTransaction() {
		return &ErrPERM{(r.f.Name() + ":EndUpdate")}
	}

	panic("TODO")
}

// Implements Filer.
func (r *uRollbackFiler) Name() string { return r.f.Name() }

// Implements Filer.
func (r *uRollbackFiler) PunchHole(off, size int64) error { panic("TODO") }

// Implements Filer.
func (r *uRollbackFiler) ReadAt(b []byte, off int64) (n int, err error) {
	if !r.inTransaction() {
		return r.f.ReadAt(b, off)
	}

	panic("TODO")
}

// Implements Filer.
func (r *uRollbackFiler) Rollback() error { panic("TODO") }

// Implements Filer.
func (r *uRollbackFiler) Size() (int64, error) { panic("TODO") }

// Implements Filer.
func (r *uRollbackFiler) Truncate(size int64) error { panic("TODO") }

// Implements Filer.
func (r *uRollbackFiler) WriteAt(b []byte, off int64) (n int, err error) {
	if !r.inTransaction() {
		return r.Writer(b, off)
	}

	panic("TODO")
}
