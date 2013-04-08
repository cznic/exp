// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package lldb

import (
	"sort"
)

//TODO(later) there's no possibility to implement rollback for FLTs!
//	Solution: Wrap a filler, hook on the rollback with the new FLT?
//	2: Add transactional methods to Allocator.

// A FLTSlot represents a Head of one of the doubly linked lists of free blocks
// with sizes in atoms >= MinSize.
type FLTSlot interface {
	// MinSize returns the minimal size of the free blocks in the list
	// starting at Head.
	MinSize() int64

	// Head returns the start (atom address) of the list with free blocks
	// of size >= MinSize.
	Head() int64

	// SetHead sets the value of the start of the list. The old value of
	// the head is discarded and replaced by the new value. If the related
	// backing Filer is a persistent storage, the change must be recorded
	// to allow for it to be retrieved when re-opening the database "file".
	SetHead(a int64) error
}

// FLT is an API for accessing an implementation specific free list table.
// This table is organized in slots (buckets) which keep the heads of the
// doubly linked free block lists. Every slot is assigned to a specific range
// of block sizes starting at some minimal size.
//
// The purpose of FLT is to isolate any specific implementation details. Those
// include the presumed way how to make the free lists table persistent.
// Typically there will be one slot in the FLT assigned for a handle of the
// head of the list of all free blocks of size 4112 and above.  Another option
// is to have yet another separate slot for free blocks of size, say 1MB+ and
// automatically punch holes in the backing storage for such sized free blocks
// - of course while keeping the head and tail block fields in place, only the
// "leak" (see Allocator documentation) field may be discarded by hole
// punching.
//
// Some ready to use types implementing FLT are provided by (TODO).
type FLT interface {
	// Report non-destructively reports a list of handles of heads of free
	// block lists belonging to buckets of size >= MinSize. The list items
	// may appear in any order. All slots MUST be included, including those
	// where Head is 0.
	Report() ([]FLTSlot, error)
}

// FLT types for NewFLTAllocator.
const (
	FLTPowersOf2 = iota // 1, 2, 4, 8, ...
	FLTFib              // 1, 2, 3, 5, 8, 13, ...
	FLTFull             // 1, 2, 3, ... 4110, 4111, 4112
	fltInvalidKind
)

type fltSlot struct {
	filer   Filer
	minSize int64
	head    int64
	off     int64
}

func (f *fltSlot) MinSize() int64 { return f.minSize }
func (f *fltSlot) Head() int64    { return f.head }
func (f *fltSlot) SetHead(h int64) error {
	f.head = h
	var b7 [7]byte
	if n, err := f.filer.WriteAt(h2b(b7[:], h), f.off); n != 7 {
		return err
	}

	return nil
}

func newCannedFLT(f Filer, kind int) (ft *flt, err error) {
	ft = &flt{}
	switch kind {
	default:
		panic(kind)
	case FLTPowersOf2:
		for i, v := range []int64{
			1,
			2,
			4,
			8,
			16,
			32,
			64,
			128,
			256,
			512,
			1024,
			2048,
			4096,
			4112,
		} {
			ft.slots = append(ft.slots, &fltSlot{filer: f, minSize: v, off: int64(7 * i)})
		}
	case FLTFib:
		for i, v := range []int64{
			1,
			2,
			3,
			5,
			8,
			13,
			21,
			34,
			55,
			89,
			144,
			233,
			377,
			610,
			987,
			1597,
			2584,
			4112,
		} {
			ft.slots = append(ft.slots, &fltSlot{filer: f, minSize: v, off: int64(7 * i)})
		}
	case FLTFull:
		ft.slots = make([]FLTSlot, maxFLTRq)
		for i := range ft.slots {
			ft.slots[i] = &fltSlot{filer: f, minSize: int64(i + 1), off: int64(7 * i)}
		}
	}

	ft.size = (7*int64(len(ft.slots)) + 15) &^ 15
	sz, err := f.Size()
	if err != nil {
		return
	}

	switch {
	case sz == 0:
		// new DB, fill the empty on-disk FLT
		b := make([]byte, ft.size)
		if n, err := f.WriteAt(b, 0); n != len(b) {
			return nil, err
		}
	case sz >= ft.size:
		// existing DB, load the on-disk FLT
		b := make([]byte, ft.size)
		if n, err := f.ReadAt(b, 0); n != len(b) {
			return nil, err
		}
		for i := range ft.slots {
			ft.slots[i].(*fltSlot).head = b2h(b[7*i:])
		}
	default:
		return nil, &ErrILSEQ{Type: ErrFLTLoad, Off: sz, Arg: ft.size}
	}

	return
}

// NewFLTAllocator returns an Allocator using a simple FLT implementation
// selected by fltKind, which must be one of the FLT* constants.
//
// If the Filer is zero sized on invocation of NewFLTAllocator then the initial
// FLT data are filed to the Filer. If the Filer size is non zero and properly
// big enough, it is assumed that the Allocator is for an existing DB and the
// FLT data are loaded from the Filer.
//
// The FLT data are simply rounded up to a 16 byte boundary and prepended in
// front of the Filer.
func NewFLTAllocator(f Filer, fltKind int) (a *Allocator, err error) {
	if fltKind < 0 || fltKind >= fltInvalidKind {
		return nil, &ErrINVAL{"Invalid FLT type", fltKind}
	}

	flt, err := newCannedFLT(f, fltKind)
	if err != nil {
		return
	}

	inner := NewInnerFiler(f, int64(flt.size))
	a, err = NewAllocator(inner, flt)
	return
}

// The flt type builds a real free list table from the limited FLT
// functionality.
type flt struct {
	size     int64
	slots    []FLTSlot
	get, put [maxFLTRq + 1]int16
}

// Implement sort.Interface
func (f *flt) Len() int           { return len(f.slots) }
func (f *flt) Less(i, j int) bool { return f.slots[i].MinSize() < f.slots[j].MinSize() }
func (f *flt) Swap(i, j int)      { f.slots[i], f.slots[j] = f.slots[j], f.slots[i] }

func newFlt(f FLT) (t *flt, err error) {
	r := &flt{}
	r.get[0], r.put[0] = -1, -1
	if r.slots, err = f.Report(); err != nil {
		return
	}

	nslots := len(r.slots)
	if nslots < 2 {
		return nil, &ErrINVAL{"Invalid FLT.Report length:", nslots}
	}

	sort.Sort(r)

	heads, minSizes := map[int64]bool{}, map[int64]bool{}
	for _, slot := range r.slots {
		sz := slot.MinSize()
		if sz := slot.MinSize(); sz < 1 {
			return nil, &ErrINVAL{"Invalid free list table slot MinSize:", sz}
		}

		if minSizes[sz] {
			return nil, &ErrINVAL{"Duplicate free list table MinSize:", sz}
		}

		minSizes[sz] = true
		h := slot.Head()
		if h < 0 {
			return nil, &ErrINVAL{"Invalid free list table head value:", h}
		}

		if h != 0 && heads[h] {
			return nil, &ErrINVAL{"Duplicate free list table head value:", h}
		}

		heads[h] = true
	}

	if sz := r.slots[0].MinSize(); sz != 1 {
		return nil, &ErrINVAL{"Invalid FLT, missing slot with MinSize == 1.", ""}
	}

	//TODO implementation is inferior wrt the specs - bigger slots are allowed
	if sz := r.slots[nslots-1].MinSize(); sz != maxFLTRq {
		return nil, &ErrINVAL{"Invalid FLT, last slot must have MinSize", maxFLTRq}
	}

	rq := 1
	for si, v := range r.slots {
		for ; int64(rq) <= v.MinSize(); rq++ {
			r.get[rq] = int16(si)
		}
	}

	rq = 1
	for si := range r.slots {
		if si == nslots-1 {
			r.put[maxFLTRq] = int16(si)
			break
		}

		for ; int64(rq) <= r.slots[si+1].MinSize()-1; rq++ {
			r.put[rq] = int16(si)
		}
	}

	return r, nil
}

func (f flt) Report() ([]FLTSlot, error) {
	return f.slots, nil
}

func (f *flt) find(needAtoms int64) (h int64, err error) {
	var ix int16

	switch {
	case needAtoms < 1:
		panic(needAtoms)
	case needAtoms >= maxFLTRq:
		ix = int16(len(f.slots) - 1)
	default:
		ix = f.get[needAtoms]
	}
	for _, slot := range f.slots[ix:] {
		if h = slot.Head(); h != 0 {
			err = slot.SetHead(0)
			return
		}
	}

	return 0, nil
}

func (f *flt) head(atoms int64) int64 {
	var ix int16

	switch {
	case atoms < 1:
		panic(atoms)
	case atoms >= maxFLTRq:
		ix = int16(len(f.slots) - 1)
	default:
		ix = f.put[atoms]
	}
	return f.slots[ix].Head()
}

func (f *flt) setHead(h, atoms int64) (err error) {
	var ix int16

	switch {
	case atoms < 1:
		panic(atoms)
	case atoms >= maxFLTRq:
		ix = int16(len(f.slots) - 1)
	default:
		ix = f.put[atoms]
	}
	return f.slots[ix].SetHead(h)
}
