// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Structural transactions.

package lldb

//TODO+ TransactionalMemoryFiler

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
		parent Filer
		m      bitFilerMap
		size   int64
	}
)

func newBitFiler(parent Filer) *bitFiler {
	return &bitFiler{parent: parent, m: bitFilerMap{}}
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
			pg = &bitZeroPage // Safe, only reading from it
			if f.parent != nil {
				_, err = f.parent.ReadAt(pg.data[:], off&^bfMask)
				if err != nil && err != io.EOF {
					return
				}
			}
		}
		nc := copy(b[:mathutil.Min(rem, bfSize)], pg.data[pgO:])
		pgI++
		pgO = 0
		rem -= nc
		n += nc
		b = b[nc:]
		off += int64(nc)
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
		f.size = 0
		return
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
			if f.parent != nil {
				_, err = f.parent.ReadAt(pg.data[:], off&^bfMask)
				if err != nil && err != io.EOF {
					return
				}
			}
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

// RollbackFiler is a Filer implementing structural transaction handling.
// Structural transactions should be small and fast because all non committed
// data are held in memory until committed or discarded by a Rollback.
//
// While using RollbackFiler, every intended update of the wrapped Filler, by
// WriteAt or Truncate, _must_ be made within a transaction. Attempts to do it
// outside of a transaction will return lldb.ErrPERM. OTOH, Invoking ReadAt
// outside of a transaction is not a problem.
//
// No nested transactions: All updates within a transaction are held in memory.
// On a matching EndUpdate the updates held in memory are actually written to
// the wrapped Filer.
//
// Nested transactions: Correct data will be seen from RollbackFiler when any
// level of a nested transaction is rollbacked. The actual writing to the
// wrapped Filer happens only when the outer most transaction nesting level is
// closed.
//
// Invoking Rollback is an alternative to EndUpdate. It discards all changes
// made at the current transaction level and returns the "state" (possibly not
// yet persisted) of the Filer to what it was before the corresponding
// BeginUpdate.
//
// During an open transaction, all reads (using ReadAt) are "dirty" reads,
// seeing the uncommitted changes made to the Filer's data.
//
// Lldb databases should be based upon a RollbackFiler. FileFiler (TODO) is a
// ready made RollbackFiler backed by an os.File.
//
// With a wrapped MemFiler one gets transactional memory (TODO). With, for
// example a wrapped disk based SimpleFileFiler it protects against at least
// some HW errors - if Rollback is properly invoked on such failures and/or if
// there's some WAL or 2PC or whatever other safe mechanism based recovery
// procedure used by the lldb client.
//
// The "real" writes to the wrapped Filer goes through the writerAt supplied to
// NewRollbackFiler.
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
// NOTE: RollbackFiler is a generic solution intended to wrap Filers provided
// by this package which do not implement any of the transactional methods.
// RollbackFiler thus _does not_ invoke any of the transactional methods of its
// wrapped Filer.
type RollbackFiler struct {
	bitFiler   *bitFiler
	checkpoint func() error
	closed     bool
	f          Filer
	nest       int
	parent     Filer
	size       int64
	writerAt   io.WriterAt
}

// NewRollbackFiler returns a RollbackFiler wrapping f.
//
// The checkpoint parameter
//
// The checkpoint function is called after closing (by EndUpdate) the upper
// most level open transaction if all calls of writerAt were sucessfull and the
// DB is thus now in a consistent state (virtually, in the ideal world with no
// write caches, no HW failures, no process crashes, ...).
//
// NOTE: In, for example, a 2PC it is necessary to reflect also the Size at the
// time of the checkpoint. All changes were successfully writen already by
// writerAt before invoking checkpoint.
//
// The writerAt parameter
//
// The writerAt interface is used to commit the updates of the wrapped Filer.
// If any invocation of writerAt fails then a non nil error will be returned
// from EndUpdate and checkpoint will _not_ ne called.  Neither is necessary to
// call Rollback. The rule of thumb: The [structural] transaction [level] is
// closed by invoking exactly once one of EndUpdate _or_ Rollback.
//
// It is presumed that writerAt uses WAL or 2PC or whatever other safe
// mechanism to physically commit the updates.
//
// Updates performed by invocations of writerAt are byte-precise, but not
// necessarily maximum possible length precise. IOW, for example an update
// crossing page boundaries may be performed by more than one writerAt
// invocation.  No offset sorting is performed.  This may change if it proves
// to be a problem. Such change would be considered backward compatible.
//
// NOTE: Using RollbackFiler, but failing to ever invoke BeginUpdate/EndUpdate
// or Rollback will cause neither writerAt or checkpoint to be ever called.
//
//TODO WIP: Incomplete implementation. Not yet functional.
func NewRollbackFiler(f Filer, checkpoint func() error, writerAt io.WriterAt) (r *RollbackFiler, err error) {
	if f == nil || checkpoint == nil || writerAt == nil {
		return nil, &ErrINVAL{Src: "lldb.NewRollbackFiler, nil argument"}
	}

	sz, err := f.Size()
	if err != nil {
		return
	}

	return &RollbackFiler{
		bitFiler:   newBitFiler(f),
		checkpoint: checkpoint,
		f:          f,
		size:       sz,
		writerAt:   writerAt,
	}, nil
}

func (r *RollbackFiler) inTransaction() bool {
	return r.parent != nil
}

// Implements Filer.
func (r *RollbackFiler) BeginUpdate() {
	panic("TODO")
}

// Implements Filer.
//
// Close will return an error if not invoked at nesting level 0.  However, to
// allow emergency closing from eg. a signal handler; if Close is invoked
// within an open transaction, it rollbacks it first.
func (r *RollbackFiler) Close() (err error) {
	panic("TODO")
	r.parent = nil //TODO this is wrong.
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
func (r *RollbackFiler) EndUpdate() error {
	if !r.inTransaction() {
		return &ErrPERM{(r.f.Name() + ":EndUpdate")}
	}

	panic("TODO")
}

// Implements Filer.
func (r *RollbackFiler) Name() string { return r.f.Name() }

// Implements Filer.
func (r *RollbackFiler) PunchHole(off, size int64) error { panic("TODO") }

// Implements Filer.
func (r *RollbackFiler) ReadAt(b []byte, off int64) (n int, err error) {
	return r.f.ReadAt(b, off)
}

// Implements Filer.
func (r *RollbackFiler) Rollback() error { panic("TODO") }

// Implements Filer.
func (r *RollbackFiler) Size() (int64, error) { panic("TODO") }

// Implements Filer.
func (r *RollbackFiler) Truncate(size int64) error { panic("TODO") }

// Implements Filer.
func (r *RollbackFiler) WriteAt(b []byte, off int64) (n int, err error) {
	if !r.inTransaction() {
		return 0, &ErrPERM{"lldb.RollbackFiler.WriteAt - non in transaction"}
	}

	return r.writerAt.WriteAt(b, off)
}
