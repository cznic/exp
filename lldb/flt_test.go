// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package lldb

import (
	"testing"

	"github.com/cznic/mathutil"
)

func TestNewFLTAllocator(t *testing.T) {
	_, err := NewFLTAllocator(nil, -1)
	if err == nil {
		t.Fatal(err)
	}

	_, err = NewFLTAllocator(nil, fltInvalidKind)
	if err == nil {
		t.Fatal(err)
	}

	for kind := 0; kind < fltInvalidKind; kind++ {
		f := NewMemFiler()
		a, err := NewFLTAllocator(f, kind)
		if err != nil {
			t.Fatal(kind, err)
		}

		flt := a.flt
		rep0, err := flt.Report()
		if err != nil {
			t.Fatal(err)
		}

		sz, err := f.Size()
		if err != nil {
			t.Fatal(err)
		}

		if g, e := sz, int64((7*len(rep0)+15)&^15); g != e {
			t.Fatal(kind, len(rep0), g, e, rep0)
		}

		var b [maxRq + 1]byte
		var bar [14]byte
		for i := range b {
			b[i] = byte(i)
		}
		for i := range bar {
			bar[i] = 0xff
		}

		m := map[int64]struct{}{}
		var i int
	loop:
		for {
			h, err := a.Alloc(b[:i])
			if err != nil {
				t.Fatal(err)
			}
			m[h] = struct{}{}

			if _, err = a.Alloc(bar[:]); err != nil {
				t.Fatal(err)
			}

			switch {
			case i == 0:
				i = 15
			case i == maxRq:
				break loop
			default:
				i += 16
			}

			i = mathutil.Min(i, maxRq)
		}

		t.Log(f.Size())

		for h := range m {
			if err = a.Free(h); err != nil {
				t.Fatal(err)
			}
		}

		rep1, err := flt.Report()
		if err != nil || len(rep1) != len(rep0) {
			t.Fatal(err)
		}

		var stats AllocStats
		if err = a.Verify(NewMemFiler(), func(err error) bool { t.Fatal(err); return false }, &stats); err != nil {
			t.Fatal(err)
		}

		t.Logf("%+v", stats)

		a, err = NewFLTAllocator(f, kind)
		if err != nil {
			t.Fatal(kind, err)
		}

		flt = a.flt
		rep2, err := flt.Report()
		if err != nil || len(rep1) != len(rep2) {
			t.Fatal(err)
		}

		for i, v1 := range rep1 {
			v2 := rep2[i]
			if g, e := v1.MinSize(), v2.MinSize(); g != e {
				t.Fatal(v1, v2)
			}

			g, err := v1.Head()
			if err != nil {
				t.Fatal(err)
			}

			e, err := v2.Head()
			if err != nil {
				t.Fatal(err)
			}

			if g != e {
				t.Fatal(v1, v2)
			}
		}

	}
}

type testFLTSlot struct {
	minsize int64
	head    int64
}

func (s *testFLTSlot) MinSize() int64        { return s.minsize }
func (s *testFLTSlot) Head() (int64, error)  { return s.head, nil }
func (s *testFLTSlot) SetHead(a int64) error { s.head = a; return nil }

type testFLT []FLTSlot

func (t testFLT) Report() ([]FLTSlot, error) { return t, nil }

func TestFLTReject(t *testing.T) {
	if _, err := NewAllocator(NewMemFiler(), testFLT(nil)); err == nil {
		t.Fatal(0)
	}

	if _, err := NewAllocator(NewMemFiler(), testFLT{&testFLTSlot{1, 0}}); err == nil {
		t.Fatal(0)
	}

	if _, err := NewAllocator(
		NewMemFiler(),
		testFLT{
			&testFLTSlot{1, 0},
			&testFLTSlot{maxFLTRq, 0},
		},
	); err != nil {
		t.Fatal(0)
	}

	if _, err := NewAllocator(
		NewMemFiler(),
		testFLT{
			&testFLTSlot{1, 0},
			&testFLTSlot{0, 0},
			&testFLTSlot{maxFLTRq, 0},
		},
	); err == nil {
		t.Fatal(0)
	}

	if _, err := NewAllocator(
		NewMemFiler(),
		testFLT{
			&testFLTSlot{1, 0},
			&testFLTSlot{1, 0},
			&testFLTSlot{maxFLTRq, 0},
		},
	); err == nil {
		t.Fatal(0)
	}

	if _, err := NewAllocator(
		NewMemFiler(),
		testFLT{
			&testFLTSlot{1, -1},
			&testFLTSlot{maxFLTRq, 0},
		},
	); err == nil {
		t.Fatal(0)
	}

	if _, err := NewAllocator(
		NewMemFiler(),
		testFLT{
			&testFLTSlot{1, 2},
			&testFLTSlot{maxFLTRq, 2},
		},
	); err == nil {
		t.Fatal(0)
	}

	if _, err := NewAllocator(
		NewMemFiler(),
		testFLT{
			&testFLTSlot{2, 0},
			&testFLTSlot{maxFLTRq, 0},
		},
	); err == nil {
		t.Fatal(0)
	}

	if _, err := NewAllocator(
		NewMemFiler(),
		testFLT{
			&testFLTSlot{1, 0},
			&testFLTSlot{maxFLTRq - 1, 0},
		},
	); err == nil {
		t.Fatal(0)
	}

	if _, err := NewAllocator(
		NewMemFiler(),
		testFLT{
			&testFLTSlot{1, 0},
			&testFLTSlot{maxFLTRq + 1, 0},
		},
	); err == nil {
		t.Fatal(0)
	}

}
