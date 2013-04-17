// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Two Phase Commit & Structural ACID

package lldb

/*
import (
	"bytes"
	"encoding/binary"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"

	"github.com/cznic/mathutil"
)

var _ Filer = &truncFiler{}

type truncFiler struct {
	f            Filer
	fake         *MemFiler
	totalWritten int // Including silently dropped
	realWritten  int
	limit        int // -1: unlimited, n: silently stop writing after limit bytes
}

func NewTruncFiler(f Filer, limit int) *truncFiler {
	return &truncFiler{f: f, fake: NewMemFiler(), limit: limit}
}

func (f *truncFiler) BeginUpdate() error                      { panic("internal error") }
func (f *truncFiler) Close() error                            { return f.f.Close() }
func (f *truncFiler) EndUpdate() error                        { panic("internal error") }
func (f *truncFiler) Name() string                            { return f.f.Name() }
func (f *truncFiler) PunchHole(off, sz int64) error           { panic("internal error") }
func (f *truncFiler) ReadAt(b []byte, off int64) (int, error) { return f.fake.ReadAt(b, off) }
func (f *truncFiler) Rollback() error                         { panic("internal error") }
func (f *truncFiler) Size() (int64, error)                    { return f.fake.Size() }
func (f *truncFiler) Sync() error                             { return f.f.Sync() }

func (f *truncFiler) Truncate(sz int64) error {
	f.fake.Truncate(sz)
	return f.f.Truncate(sz)
}

func (f *truncFiler) WriteAt(b []byte, off int64) (n int, err error) {
	rq := len(b)
	n = f.totalWritten
	if lim := f.limit; lim >= 0 && n+rq > lim {
		over := n + rq - lim
		rq -= over
		rq = mathutil.Max(rq, 0)
	}

	if n, err = f.fake.WriteAt(b, off); err != nil {
		return
	}

	f.totalWritten += n
	if rq != 0 {
		n, err := f.f.WriteAt(b[:rq], off)
		if err != nil {
			return n, err
		}
		f.realWritten += n
	}
	return
}

// Verify memory BTrees don't have maxRq limits.
func TestACID0MemBTreeCaps(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	tr := NewBTree(nil)
	b := make([]byte, 2*maxRq)
	for i := range b {
		b[i] = byte(rng.Int())
	}

	if err := tr.Set(nil, b); err != nil {
		t.Fatal(len(b), err)
	}

	g, err := tr.Get(nil)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(g, b) {
		t.Fatal("data mismatach")
	}
}

func TestACIDFiler0(t *testing.T) {
	const SZ = 1e3 //TODO 1e6 ?

	wal, err := ioutil.TempFile("", "test-acidfiler0-wal")
	if err != nil {
		t.Fatal(err)
	}

	if !*oKeep {
		defer os.Remove(wal.Name())
	}

	db, err := ioutil.TempFile("", "test-acidfiler0-db")
	if err != nil {
		t.Fatal(err)
	}

	if !*oKeep {
		defer os.Remove(db.Name())
	}

	realFiler := NewSimpleFileFiler(db)
	truncFiler := NewTruncFiler(realFiler, -1)
	acidFiler, err := NewACIDFiler(truncFiler, wal)
	if err != nil {
		t.Error(err)
		return
	}

	if err = acidFiler.BeginUpdate(); err != nil {
		t.Error(err)
		return
	}

	a, err := NewFLTAllocator(acidFiler, FLTPowersOf2)
	if err != nil {
		t.Error(err)
		return
	}

	a.Compress = true

	tr, h, err := CreateBTree(a, nil)
	if err != nil {
		t.Error(err)
		return
	}

	rng := rand.New(rand.NewSource(42))
	var key, val [8]byte
	ref := map[int64]int64{}

	npairs := 0
	for {
		sz, err := acidFiler.Size()
		if err != nil {
			t.Error(err)
			return
		}

		if sz > SZ {
			break
		}

		dbg("npairs %d, truncFiler.totalWritten %d", npairs, truncFiler.totalWritten)
		k, v := rng.Int63(), rng.Int63()
		ref[k] = v
		binary.BigEndian.PutUint64(key[:], uint64(k))
		binary.BigEndian.PutUint64(val[:], uint64(v))
		if err := tr.Set(key[:], val[:]); err != nil {
			t.Error(err)
			return
		}

		npairs++
	}

	if err := acidFiler.EndUpdate(); err != nil {
		t.Error(err)
		return
	}

	if err := acidFiler.Close(); err != nil {
		t.Error(err)
		return
	}

	dbg("npairs %d, truncFiler.totalWritten %d, truncFiler.realWritten %d", npairs, truncFiler.totalWritten, truncFiler.realWritten)
	_ = h
}
*/
