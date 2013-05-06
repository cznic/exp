// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package lldb

import (
	"bytes"
	"encoding/hex"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/cznic/sortutil"
)

var (
	allocRndTestLimit     = flag.Uint("lim", 2*maxShort, "Allocator rnd test initial blocks size limit")
	allocRndTestHardLimit = flag.Uint("hlim", 0, "Allocator rnd test initial blocks size hard limit")
	testN                 = flag.Int("N", 128, "Allocator rnd test block count")
	allocRndDump          = flag.Bool("dump", false, "Produce dump files on TestAllocatorRnd crash")
	oKeep                 = flag.Bool("keep", false, "do not delete testing DB/WAL (where applicable)")
)

func mfBytes(f Filer) []byte {
	var b bytes.Buffer
	if _, err := f.(*MemFiler).WriteTo(&b); err != nil {
		panic(err)
	}

	return b.Bytes()
}

// Paranoid Allocator, automatically verifies whenever possible.
type pAllocator struct {
	*Allocator
	errors           []error
	logger           func(error) bool
	lastKnownGood    *MemFiler
	lastKnownGoodFLT []FLTSlot
	lastOp           string
	stats            AllocStats
}

func newPAllocator(f Filer, flt FLT) (*pAllocator, error) {
	a, err := NewAllocator(f, flt)
	if err != nil {
		return nil, err
	}

	r := &pAllocator{Allocator: a, lastKnownGood: NewMemFiler()}
	r.logger = func(err error) bool {
		r.errors = append(r.errors, err)
		return len(r.errors) < 100
	}

	return r, nil
}

func (a *pAllocator) err() error {
	var n int
	if n = len(a.errors); n == 0 {
		return nil
	}

	s := make([]string, n)
	for i, e := range a.errors {
		s[i] = e.Error()
	}
	return fmt.Errorf("\n%s", strings.Join(s, "\n"))
}

func (a *pAllocator) preMortem(s string) {
	var e error
	if e := a.lastKnownGood.Truncate(0); e != nil {
		panic(e)
	}
	b := mfBytes(a.Allocator.f)
	if _, e = a.lastKnownGood.WriteAt(b, 0); e != nil {
		return
	}
	var rep []FLTSlot
	if rep, e = a.Allocator.flt.Report(); e != nil {
		panic(e)
	}
	a.lastKnownGoodFLT = make([]FLTSlot, len(rep))
	copy(a.lastKnownGoodFLT, rep)
	a.lastOp = s
}

func (a *pAllocator) Alloc(b []byte) (handle int64, err error) {
	//if err = a.Allocator.Verify(NewMemFiler(), a.logger, nil); err != nil {
	//err = fmt.Errorf("Alloc: pre-dump check fail! '%s'", err)
	//return
	//}

	if *allocRndDump {
		a.preMortem("")
		defer func() { a.lastOp = fmt.Sprintf("Alloc(%d bytes): h %#x", len(b), handle) }()
	}

	if handle, err = a.Allocator.Alloc(b); err != nil {
		return
	}

	if err = a.Allocator.Verify(NewMemFiler(), a.logger, &a.stats); err != nil {
		err = fmt.Errorf("'%s': %v", err, a.err())
		return
	}

	err = a.err()
	return
}

func (a *pAllocator) Free(handle int64) (err error) {
	//if err = a.Allocator.Verify(NewMemFiler(), a.logger, nil); err != nil {
	//err = fmt.Errorf("Free: pre-dump check fail! '%s'", err)
	//return
	//}

	if *allocRndDump {
		a.preMortem(fmt.Sprintf("Free(h %#x)", handle))
	}

	if err = a.Allocator.Free(handle); err != nil {
		return
	}

	if err = a.Allocator.Verify(NewMemFiler(), a.logger, &a.stats); err != nil {
		err = fmt.Errorf("'%s': %v", err, a.err())
		return
	}

	err = a.err()
	return
}

func (a *pAllocator) Realloc(handle int64, b []byte) (err error) {
	//if err = a.Allocator.Verify(NewMemFiler(), a.logger, nil); err != nil {
	//err = fmt.Errorf("Realloc: pre-dump check fail! '%s'", err)
	//return
	//}

	if *allocRndDump {
		a.preMortem(fmt.Sprintf("Realloc(h %#x, %d bytes)", handle, len(b)))
	}

	if err = a.Allocator.Realloc(handle, b); err != nil {
		return
	}

	if err = a.Allocator.Verify(NewMemFiler(), a.logger, &a.stats); err != nil {
		err = fmt.Errorf("'%s': %v", err, a.err())
		return
	}

	err = a.err()
	return
}

func dump(a *pAllocator, t *testing.T) {
	m := a.f.(*MemFiler)
	sz, err := m.Size()
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("MemFiler.Size() == %d(%#x)", sz, sz)
	if !*allocRndDump {
		return
	}

	fn := "good-dump"
	f, err := os.Create(fn)
	if err != nil {
		t.Fatal(err)
	}

	defer f.Close()
	sz, err = a.lastKnownGood.WriteTo(f)
	if err != nil {
		t.Error(err)
		return
	}

	t.Logf("%d(%#x) writen to %q", sz, sz, fn)

	fn = "bad-dump"
	g, err := os.Create(fn)
	if err != nil {
		t.Fatal(err)
	}

	defer g.Close()
	sz, err = m.WriteTo(g)
	if err != nil {
		t.Error(err)
		return
	}

	t.Logf("%d(%#x) writen to %q", sz, sz, fn)

	t.Log("Last known good FLT")
	for _, slot := range a.lastKnownGoodFLT {
		h, err := slot.Head()
		if err != nil {
			t.Error(err)
			return
		}

		if h != 0 {
			t.Logf("min %d head %#x off %#x", slot.MinSize(), h, h2off(h))
		}
	}

	t.Log("Current FLT")
	r, err := a.flt.Report()
	if err != nil {
		t.Error(err)
		return
	}

	for _, slot := range r {
		h, err := slot.Head()
		if err != nil {
			t.Error(err)
			return
		}

		if h != 0 {
			t.Logf("min %d head %#x off %#x", slot.MinSize(), h, h2off(h))
		}
	}
	t.Logf("Last op: %q", a.lastOp)
}

func init() {
	if *testN <= 0 {
		*testN = 1
	}
}

func TestVerify0(t *testing.T) {
	// All must fail
	tab := []string{

		// 0: Reloc, links beyond EOF
		"" +
			"fd 00 00 00 00 00 00 02 00 00 00 00 00 00 00 00",
		// 1: Reloc, links beyond EOF
		"" +
			"fd 00 00 00 00 00 00 03 00 00 00 00 00 00 00 00" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00",
		// 2: Reloc, broken target
		"" +
			"fd 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00",
		// 3: Free block at file tail
		"" +
			"fe 00 00 00 00 00 00 00 00 00 00 00 00 00 00 fe",
		// 4: Free block at file tail
		"" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00" +
			"fe 00 00 00 00 00 00 00 00 00 00 00 00 00 00 fe",
		// 5: Reloc, invalid target 0xfe
		"" +
			"fd 00 00 00 00 00 00 02 00 00 00 00 00 00 00 00" +
			"fe 00 00 00 00 00 00 00 00 00 00 00 00 00 00 fe" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00",
		// 6: Reloc, invalid target 0xfd
		"" +
			"fd 00 00 00 00 00 00 02 00 00 00 00 00 00 00 00" +
			"fd 00 00 00 00 00 00 01 00 00 00 00 00 00 00 00" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00",
		// 7: Lost free block @ 0x00
		"" +
			"fe 00 00 00 00 00 00 02 00 00 00 00 00 00 00 fe" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00",
		// 8: Lost free block @ 0x10
		"" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00" +
			"fe 00 00 00 00 00 00 02 00 00 00 00 00 00 00 fe" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00",
		// 9: Invalid padding
		"" +
			"00 01 00 00 00 00 00 00 00 00 00 00 00 00 00 00",
		// 10: Invalid padding
		"" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 01 00",
		// 11: Invalid padding
		"" +
			"01 00 01 00 00 00 00 00 00 00 00 00 00 00 00 00",
		// 12: Invalid padding
		"" +
			"01 00 00 00 00 00 00 00 00 00 00 00 00 00 01 00",
		// 13: Invalid padding
		"" +
			"0d 00 00 00 00 00 00 00 00 00 00 00 00 00 01 00",
		// 14: Invalid CC (tail tag)
		"" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 02",
		// 15: Invalid CC (tail tag)
		"" +
			"fd 00 00 00 00 00 00 02 00 00 00 00 00 00 00 01",
		// 16: Cannot decompress
		"" +
			"0e 00 00 00 00 00 00 00 00 00 00 00 00 00 00 01",
		// 17: Invalid reloc target
		"" +
			"fd 00 00 00 00 00 00 03 00 00 00 00 00 00 00 00" +
			"ff 00 00 00 00 00 00 02 00 00 00 00 00 00 00 00" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 02 ff" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00",
		// 18: Invalid tail tag @1
		"" +
			"fe 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00",
		// 19: Invalid size @1
		"" +
			"ff 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 ff" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00",
		// 20: Invalid size @1
		"" +
			"ff 00 00 00 00 00 00 01 00 00 00 00 00 00 00 00" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 01 ff" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00",
		// 21: Invalid size @1
		"" +
			"ff 00 00 00 00 00 00 04 00 00 00 00 00 00 00 00" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 02 ff" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00",
		// 22: Invalid .next @1
		"" +
			"ff 00 00 00 00 00 00 02 00 00 00 00 00 00 00 00" +
			"00 00 00 00 00 04 00 00 00 00 00 00 00 00 02 ff" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00",
		// 23: Invalid .prev @1
		"" +
			"ff 00 00 00 00 00 00 02 00 00 00 00 00 00 04 00" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 02 ff" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00",
		// 24: Invalid tail tag @1
		"" +
			"ff 00 00 00 00 00 00 02 00 00 00 00 00 00 00 00" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 02 fe" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00",
		// 25: Invalid tail size @1
		"" +
			"ff 00 00 00 00 00 00 02 00 00 00 00 00 00 00 00" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 ff" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00",
		// 26: Invalid tail size @1
		"" +
			"ff 00 00 00 00 00 00 02 00 00 00 00 00 00 00 00" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 01 ff" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00",
		// 27: Invalid tail size @1
		"" +
			"ff 00 00 00 00 00 00 02 00 00 00 00 00 00 00 00" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 03 ff" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00",
		//       00 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f
	}

	for kind := 0; kind < fltInvalidKind; kind++ {
		for i, test := range tab {
			errors := []error{}

			f := NewMemFiler()
			b := s2b(test)
			n := len(b)
			if n == 0 {
				t.Fatal(n)
			}

			if m, err := f.ReadFrom(bytes.NewBuffer(b)); m != int64(n) || err != nil {
				t.Fatal(m, err)
			}

			sz, err := f.Size()
			if err != nil {
				t.Fatal(err)
			}

			if g, e := sz, int64(n); g != e {
				t.Fatal(g, e)
			}

			flt, err := newCannedFLT(NewMemFiler(), kind)
			if err != nil {
				t.Fatal(err)
			}

			a, err := newPAllocator(f, flt)
			if err != nil {
				t.Fatal(err)
			}

			err = a.Verify(
				NewMemFiler(),
				func(err error) bool {
					if err == nil {
						t.Fatal("nil error")
					}
					errors = append(errors, err)
					return false
				},
				nil,
			)
			if err == nil {
				t.Fatal(i, "unexpected success")
			}

			t.Log(i, err, errors)
		}
	}
}

func TestVerify1(t *testing.T) {
	for kind := 0; kind < fltInvalidKind; kind++ {
		f := NewMemFiler()
		bitmap := NewMemFiler()
		if n, err := bitmap.WriteAt([]byte{0}, 0); n != 1 || err != nil {
			t.Fatal(n, err)
		}

		flt, err := newCannedFLT(NewMemFiler(), kind)
		if err != nil {
			t.Fatal(err)
		}

		a, err := newPAllocator(f, flt)
		if err != nil {
			t.Fatal(err)
		}

		if err := a.Verify(
			bitmap,
			func(error) bool {
				t.Fatal("should not be reachable")
				panic("unreachable")
			},
			nil,
		); err == nil {
			t.Fatal("unexpected success")
		}
	}
}

func repDump(a []FLTSlot) string {
	b := []string{}
	for _, v := range a {
		h, err := v.Head()
		if err != nil {
			panic(err)
		}

		if h != 0 {
			b = append(b, fmt.Sprintf("min:%d, h:%d", v.MinSize(), h))
		}
	}
	return strings.Join(b, ";")
}

func TestVerify2(t *testing.T) {
	// All must fail for the fixed (see bellow) FLT.Report()
	tab := []string{

		// 0: FLT broken linkage (missing free blocks @2,4)
		"" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00",
		// 1: FLT broken linkage (missing free block @4)
		"" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00" +
			"fe 00 00 00 00 00 00 00 00 00 00 00 00 00 00 fe" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00",
		// 2: bad size @4
		"" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00" +
			"fe 00 00 00 00 00 00 00 00 00 00 00 00 00 00 fe" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00" +
			"fe 00 00 00 00 00 00 00 00 00 00 00 00 00 00 fe" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00",
		//		// 3: bad size @4
		"" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00" +
			"fe 00 00 00 00 00 00 00 00 00 00 00 00 00 04 fe" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00" +
			"ff 00 00 00 00 00 00 01 00 00 00 00 00 00 00 00" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 01 ff" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00",
		//		// 4: bad .next @6 from @2
		"" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00" +
			"fe 00 00 00 00 00 00 00 00 00 00 00 00 00 06 fe" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00" +
			"ff 00 00 00 00 00 00 02 00 00 00 00 00 00 00 00" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 02 ff" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00" +
			"fe 00 00 00 00 00 00 00 00 00 00 00 00 00 00 fe" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00",
		//		// 5: bad .prev @7
		"" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00" +
			"fe 00 00 00 00 00 00 00 00 00 00 00 00 00 07 fe" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00" +
			"ff 00 00 00 00 00 00 02 00 00 00 00 00 00 00 00" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 02 ff" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00" +
			"fe 00 00 00 00 00 00 00 00 00 00 00 00 00 00 fe" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00",
		//		// 6: bad .next @7
		"" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00" +
			"fe 00 00 00 00 00 00 00 00 00 00 00 00 00 07 fe" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00" +
			"ff 00 00 00 00 00 00 02 00 00 00 00 00 00 00 00" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 02 ff" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00" +
			"fe 00 00 00 00 00 00 02 00 00 00 00 00 00 07 fe" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00",
		//		// 7: bad .next @5
		"" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00" +
			"fe 00 00 00 00 00 00 00 00 00 00 00 00 00 07 fe" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00" +
			"ff 00 00 00 00 00 00 02 00 00 00 00 00 00 00 00" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 02 ff" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00" +
			"fe 00 00 00 00 00 00 02 00 00 00 00 00 00 01 fe" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00",
		//		// 8: bad chaining
		"" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00" +
			"fe 00 00 00 00 00 00 00 00 00 00 00 00 00 07 fe" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00" +
			"ff 00 00 00 00 00 00 02 00 00 00 00 00 00 00 00" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 02 ff" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00" +
			"fe 00 00 00 00 00 00 02 00 00 00 00 00 00 01 fe" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00",
		//		// 9: lost free block @8
		"" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00" +
			"fe 00 00 00 00 00 00 00 00 00 00 00 00 00 0f fe" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00" +
			"ff 00 00 00 00 00 00 02 00 00 00 00 00 00 00 00" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 02 ff" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00" +
			"fe 00 00 00 00 00 00 00 00 00 00 00 00 00 00 fe" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00" +
			"fe 00 00 00 00 00 00 02 00 00 00 00 00 00 00 fe" +
			"00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00",
		//       00 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f
	}

	for i, test := range tab {
		errors := []error{}

		f := NewMemFiler()
		b := s2b(test)
		n := len(b)
		if n == 0 {
			t.Fatal(n)
		}

		if m, err := f.ReadFrom(bytes.NewBuffer(b)); m != int64(n) || err != nil {
			t.Fatal(m, err)
		}

		sz, err := f.Size()
		if err != nil {
			t.Fatal(err)
		}

		if g, e := sz, int64(n); g != e {
			t.Fatal(g, e)
		}

		flt, err := newCannedFLT(NewMemFiler(), FLTFull)
		if err != nil {
			t.Fatal(err)
		}

		flt.slots[0].SetHead(2) // minSize:1, head: 2
		flt.slots[1].SetHead(4) // minSize:2, head: 4
		a, err := newPAllocator(f, flt)
		if err != nil {
			t.Fatal(err)
		}

		err = a.Verify(
			NewMemFiler(),
			func(err error) bool {
				if err == nil {
					t.Fatal("nil error")
				}
				t.Log(i, "logged: ", err)
				errors = append(errors, err)
				return true
			},
			nil,
		)
		if err == nil {
			t.Fatal(i, "unexpected success")
		}

		t.Log(i, err, errors)
	}
}

// Allocation in an empty DB.
func TestAllocatorAlloc0(t *testing.T) {
	tab := []struct {
		h        int64
		b, f, fc string
	}{
		{1, // len 0
			"" +
				"",
			"" +
				"00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00",
			"" +
				"00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00"},
		{1, // len 1
			"" +
				"42",
			"" +
				"01 42 00 00 00 00 00 00 00 00 00 00 00 00 00 00",
			"" +
				"01 42 00 00 00 00 00 00 00 00 00 00 00 00 00 00"},
		{1, // max single atom, not compressible
			"" +
				"01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e",
			"" +
				"0e 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 00",
			"" +
				"0e 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 00"},
		{1, // max single atom, compressible, but not eligible for it
			"" +
				"01 02 03 04 05 06 07 08 99 01 02 03 04 05",
			"" +
				"0e 01 02 03 04 05 06 07 08 99 01 02 03 04 05 00",
			"" +
				"0e 01 02 03 04 05 06 07 08 99 01 02 03 04 05 00"},
		{1, // > 1 atom, not compressible
			"" +
				"01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f",
			"" +
				"0f 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f" +
				"00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00",
			"" +
				"0f 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f" +
				"00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00"},
		{1, // > 1 atom, compressible
			"" +
				"01 02 03 04 05 06 07 08 99 01 02 03 04 05 06 07" +
				"08",
			"" +
				"11 01 02 03 04 05 06 07 08 99 01 02 03 04 05 06" +
				"07 08 00 00 00 00 00 00 00 00 00 00 00 00 00 00",
			"" +
				"0e 11 12 01 02 03 04 05 06 07 08 99 01 0d 09 01"},
		{1, // longest short
			"" +
				"00 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f" +
				"10 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f" +
				"20 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f" +
				"30 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f" +
				"40 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f" +
				"50 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f" +
				"60 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f" +
				"70 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f" +
				"80 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f" +
				"90 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f" +
				"a0 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f" +
				"b0 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f" +
				"c0 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f" +
				"d0 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f" +
				"e0 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f" +
				"f0 01 02 03 04 05 06 07 08 09 0a",
			"" +
				"" +
				"fb 00 01 02 03 04 05 06  07 08 09 0a 0b 0c 0d 0e" +
				"0f 10 01 02 03 04 05 06  07 08 09 0a 0b 0c 0d 0e" +
				"0f 20 01 02 03 04 05 06  07 08 09 0a 0b 0c 0d 0e" +
				"0f 30 01 02 03 04 05 06  07 08 09 0a 0b 0c 0d 0e" +
				"0f 40 01 02 03 04 05 06  07 08 09 0a 0b 0c 0d 0e" +
				"0f 50 01 02 03 04 05 06  07 08 09 0a 0b 0c 0d 0e" +
				"0f 60 01 02 03 04 05 06  07 08 09 0a 0b 0c 0d 0e" +
				"0f 70 01 02 03 04 05 06  07 08 09 0a 0b 0c 0d 0e" +
				"0f 80 01 02 03 04 05 06  07 08 09 0a 0b 0c 0d 0e" +
				"0f 90 01 02 03 04 05 06  07 08 09 0a 0b 0c 0d 0e" +
				"0f a0 01 02 03 04 05 06  07 08 09 0a 0b 0c 0d 0e" +
				"0f b0 01 02 03 04 05 06  07 08 09 0a 0b 0c 0d 0e" +
				"0f c0 01 02 03 04 05 06  07 08 09 0a 0b 0c 0d 0e" +
				"0f d0 01 02 03 04 05 06  07 08 09 0a 0b 0c 0d 0e" +
				"0f e0 01 02 03 04 05 06  07 08 09 0a 0b 0c 0d 0e" +
				"0f f0 01 02 03 04 05 06  07 08 09 0a 00 00 00 00",
			"" +
				"" +
				"4e fb 01 20 00 01 02 03  04 05 06 07 08 09 0a 0b" +
				"0c 0d 0e 0f 10 1d 10 00  20 1d 10 00 30 1d 10 00" +
				"40 1d 10 00 50 1d 10 00  60 1d 10 00 70 1d 10 00" +
				"80 1d 10 00 90 1d 10 00  a0 1d 10 00 b0 1d 10 00" +
				"c0 1d 10 00 d0 1d 10 00  e0 1d 10 00 f0 13 10 01"},

		{1, // shortest long
			"" +
				"00 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f" +
				"10 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f" +
				"20 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f" +
				"30 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f" +
				"40 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f" +
				"50 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f" +
				"60 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f" +
				"70 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f" +
				"80 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f" +
				"90 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f" +
				"a0 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f" +
				"b0 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f" +
				"c0 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f" +
				"d0 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f" +
				"e0 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f" +
				"f0 01 02 03 04 05 06 07 08 09 0a 0b",
			"" +
				"" +
				"fc 00 fc 00 01 02 03 04  05 06 07 08 09 0a 0b 0c" +
				"0d 0e 0f 10 01 02 03 04  05 06 07 08 09 0a 0b 0c" +
				"0d 0e 0f 20 01 02 03 04  05 06 07 08 09 0a 0b 0c" +
				"0d 0e 0f 30 01 02 03 04  05 06 07 08 09 0a 0b 0c" +
				"0d 0e 0f 40 01 02 03 04  05 06 07 08 09 0a 0b 0c" +
				"0d 0e 0f 50 01 02 03 04  05 06 07 08 09 0a 0b 0c" +
				"0d 0e 0f 60 01 02 03 04  05 06 07 08 09 0a 0b 0c" +
				"0d 0e 0f 70 01 02 03 04  05 06 07 08 09 0a 0b 0c" +
				"0d 0e 0f 80 01 02 03 04  05 06 07 08 09 0a 0b 0c" +
				"0d 0e 0f 90 01 02 03 04  05 06 07 08 09 0a 0b 0c" +
				"0d 0e 0f a0 01 02 03 04  05 06 07 08 09 0a 0b 0c" +
				"0d 0e 0f b0 01 02 03 04  05 06 07 08 09 0a 0b 0c" +
				"0d 0e 0f c0 01 02 03 04  05 06 07 08 09 0a 0b 0c" +
				"0d 0e 0f d0 01 02 03 04  05 06 07 08 09 0a 0b 0c" +
				"0d 0e 0f e0 01 02 03 04  05 06 07 08 09 0a 0b 0c" +
				"0d 0e 0f f0 01 02 03 04  05 06 07 08 09 0a 0b 00",
			"" +
				"" +
				"4e fc 01 20 00 01 02 03  04 05 06 07 08 09 0a 0b" +
				"0c 0d 0e 0f 10 1d 10 00  20 1d 10 00 30 1d 10 00" +
				"40 1d 10 00 50 1d 10 00  60 1d 10 00 70 1d 10 00" +
				"80 1d 10 00 90 1d 10 00  a0 1d 10 00 b0 1d 10 00" +
				"c0 1d 10 00 d0 1d 10 00  e0 1d 10 00 f0 15 10 01"},
	}

	for kind := 0; kind < fltInvalidKind; kind++ {
		for i, test := range tab {
			f := func(compress bool, e []byte) {
				f := NewMemFiler()
				flt, err := newCannedFLT(NewMemFiler(), kind)
				if err != nil {
					t.Fatal(err)
				}

				a, err := newPAllocator(f, flt)
				if err != nil {
					t.Fatal(err)
				}

				a.Compress = compress
				h, err := a.Alloc(s2b(test.b))
				if err != nil {
					t.Fatal(i, err)
				}

				if g, e := h, test.h; g != e {
					t.Fatal(i, g, e)
				}

				if g := mfBytes(f); !bytes.Equal(g, e) {
					t.Fatalf("\ni: %d compress: %t\ng:\n%se:\n%s", i, compress, hex.Dump(g), hex.Dump(e))
				}
			}
			f(false, s2b(test.f))
			f(true, s2b(test.fc))
		}
	}
}

func TestAllocatorMakeUsedBlock(t *testing.T) {
	f := NewMemFiler()
	flt, err := newCannedFLT(NewMemFiler(), FLTFull)
	if err != nil {
		t.Fatal(err)
	}

	a, err := NewAllocator(f, flt)
	if err != nil {
		t.Fatal(err)
	}

	var c allocatorBlock
	if _, _, err := a.makeUsedBlock(&c, make([]byte, maxRq)); err != nil {
		t.Fatal(err)
	}

	if _, _, err := a.makeUsedBlock(&c, make([]byte, maxRq+1)); err == nil {
		t.Fatal("unexpected success")
	}
}

func stableRef(m map[int64][]byte) (r []struct {
	h int64
	b []byte
}) {
	a := make(sortutil.Int64Slice, 0, len(m))
	for k := range m {
		a = append(a, k)
	}
	sort.Sort(a)
	for _, v := range a {
		r = append(r, struct {
			h int64
			b []byte
		}{v, m[v]})
	}
	return
}

func TestAllocatorRnd(t *testing.T) {
	N := *testN

	for cc := 0; cc < 2; cc++ {
		for kind := 0; kind < fltInvalidKind; kind++ {
			rng := rand.New(rand.NewSource(42))
			f := NewMemFiler()
			flt, err := newCannedFLT(NewMemFiler(), kind)
			if err != nil {
				t.Fatal(err)
			}

			a, err := newPAllocator(f, flt)
			if err != nil {
				t.Fatal(err)
			}

			balance := 0

			bad := func() bool {
				if a.Compress {
					return false
				}

				actual := a.stats.TotalAtoms - a.stats.FreeAtoms - a.stats.Relocations
				if int64(balance) != actual {
					t.Logf("balance: %d, actual %d\n%#v", balance, actual, a.stats)
					return true
				}

				return false
			}

			if cc != 0 {
				a.Compress = true
			}
			ref := map[int64][]byte{}

			for pass := 0; pass < 2; pass++ {

				// A) Alloc N blocks
				for i := 0; i < N; i++ {
					rq := rng.Int31n(int32(*allocRndTestLimit))
					if rq%127 == 0 {
						rq = 3 * maxRq / 4
					}
					if rq%11 == 0 {
						rq %= 23
					}
					if hl := *allocRndTestHardLimit; hl != 0 {
						rq = rq % int32(hl)
					}
					b := make([]byte, rq)
					for j := range b {
						b[j] = byte(rng.Int())
					}
					if rq > 300 {
						for i := 100; i < 200; i++ {
							b[i] = 'A' // give compression a chance
						}
					}

					balance += n2atoms(len(b))
					h, err := a.Alloc(b)
					if err != nil || bad() {
						dump(a, t)
						t.Fatalf(
							"A) N %d, kind %d, pass %d, i:%d, len(b):%d(%#x), err %v",
							N, kind, pass, i, len(b), len(b), err,
						)
					}

					ref[h] = b
				}

				var rb []byte

				// B) Check them back
				for h, wb := range ref {
					if rb, err = a.Get(rb, h); err != nil {
						dump(a, t)
						t.Fatal("B)", err)
					}

					if !bytes.Equal(rb, wb) {
						dump(a, t)
						t.Fatalf("B) h %d", h)
					}
				}

				nf := 0
				// C) Free every third block
				for _, v := range stableRef(ref) {
					h, b := v.h, v.b
					if rng.Int()%3 != 0 {
						continue
					}

					balance -= n2atoms(len(b))
					if err = a.Free(h); err != nil || bad() {
						dump(a, t)
						t.Fatal(err)
					}

					delete(ref, h)
					nf++
				}

				// D) Check them back
				for h, wb := range ref {
					if rb, err = a.Get(rb, h); err != nil {
						dump(a, t)
						t.Fatal("D)", err)
					}

					if !bytes.Equal(rb, wb) {
						dump(a, t)
						t.Fatalf("D) h %d", h)
					}
				}

				// E) Resize every block remaining
				for _, v := range stableRef(ref) {
					h, wb := v.h, v.b
					len0 := len(wb)
					switch rng.Int() & 1 {
					case 0:
						wb = wb[:len(wb)*3/4]
					case 1:
						wb = append(wb, wb...)
					}
					if len(wb) > maxRq {
						wb = wb[:maxRq]
					}

					for j := range wb {
						wb[j] = byte(rng.Int())
					}
					if len(wb) > 300 {
						for i := 100; i < 200; i++ {
							wb[i] = 'D' // give compression a chance
						}
					}
					a0, a1 := n2atoms(len0), n2atoms(len(wb))
					balance = balance - a0 + a1
					if err := a.Realloc(h, wb); err != nil || bad() {
						dump(a, t)
						t.Fatalf(
							"D) h:%#x, len(b):%#4x, len(wb): %#x, err %s",
							h, len0, len(wb), err,
						)
					}

					ref[h] = wb
				}

				// F) Check them back
				for h, wb := range ref {
					if rb, err = a.Get(rb, h); err != nil {
						dump(a, t)
						t.Fatal("E)", err)
					}

					if !bytes.Equal(rb, wb) {
						dump(a, t)
						t.Fatalf("E) h %d", h)
					}
				}
			}

			if cc == 0 {
				sz, err := f.Size()
				if err != nil {
					t.Fatal(err)
				}

				t.Logf(
					"kind %d, AllocAtoms %7d, AllocBytes %7d, FreeAtoms %7d, Relocations %7d, TotalAtoms %7d, f.Size %7d, space eff %.2f%%",
					kind, a.stats.AllocAtoms, a.stats.AllocBytes, a.stats.FreeAtoms, a.stats.Relocations, a.stats.TotalAtoms, sz, 100*float64(a.stats.AllocBytes)/float64(sz),
				)
			}
			// Free everything
			for h, b := range ref {
				balance -= n2atoms(len(b))
				if err = a.Free(h); err != nil || bad() {
					dump(a, t)
					t.Fatal(err)
				}
			}

			sz, err := a.f.Size()
			if err != nil {
				t.Fatal(err)
			}

			if g, e := sz, int64(0); g != e {
				dump(a, t)
				t.Fatal(g, e)
			}

		}
	}
}

func TestRollbackAllocator(t *testing.T) {
	for kind := 0; kind < fltInvalidKind; kind++ {
		f := NewMemFiler()
		var r *RollbackFiler
		r, err := NewRollbackFiler(f,
			func() (err error) {
				sz, err := r.Size()
				if err != nil {
					return
				}

				return f.Truncate(sz)
			},
			f,
		)
		if err != nil {
			t.Fatal(err)
		}

		if err := r.BeginUpdate(); err != nil { // BeginUpdate 0->1
			t.Fatal(err)
		}

		a, err := NewFLTAllocator(r, kind)
		if err != nil {
			t.Fatal(err)
		}

		h, err := a.Alloc(nil)
		if err != nil {
			t.Fatal(err)
		}

		if h != 1 {
			t.Fatal(h)
		}

		// | 1 |

		h, err = a.Alloc(nil)
		if err != nil {
			t.Fatal(err)
		}

		if h != 2 {
			t.Fatal(h)
		}

		// | 1 | 2 |
		h, err = a.Alloc(nil)
		if err != nil {
			t.Fatal(err)
		}

		if h != 3 {
			t.Fatal(h)
		}

		// | 1 | 2 | 3 |
		if err = a.Free(2); err != nil {
			t.Fatal(err)
		}

		// | 1 | free | 3 |
		if err := r.BeginUpdate(); err != nil { // BeginUpdate 1->2
			t.Fatal(err)
		}

		h, err = a.Alloc(nil)
		if err != nil {
			t.Fatal(err)
		}

		if h != 2 {
			t.Fatal(h)
		}

		// | 1 | 2 | 3 |
		if err := r.Rollback(); err != nil { // Rollback 2->1
			t.Fatal(err)
		}

		// | 1 | free | 3 |
		h, err = a.Alloc(nil)
		if err != nil {
			t.Fatal(err)
		}

		if h != 2 {
			t.Fatal(h)
		}

		// | 1 | 2 | 3 |
		if err := a.Verify(NewMemFiler(), nil, nil); err != nil {
			t.Fatal(err)
		}

	}
}
