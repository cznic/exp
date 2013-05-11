// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package lldb

import (
	"bytes"
	"encoding/hex"
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

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
				panic("intrnal error")
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

				if err = f.Truncate(sz); err != nil {
					return err
				}

				return f.Sync()
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

func benchmarkAllocatorAlloc(b *testing.B, f Filer, sz int) {
	b.SetBytes(int64(sz))

	if err := f.BeginUpdate(); err != nil {
		b.Error(err)
		return
	}

	a, err := NewFLTAllocator(f, FLTPowersOf2)
	if err != nil {
		b.Error(err)
		return
	}

	if err = f.EndUpdate(); err != nil {
		b.Error(err)
		return
	}

	v := make([]byte, sz)
	runtime.GC()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err = f.BeginUpdate(); err != nil {
			b.Error(err)
			return
		}

		if h, err := a.Alloc(v); h <= 0 || err != nil {
			f.EndUpdate()
			b.Error(h, err)
			return
		}

		if err = f.EndUpdate(); err != nil {
			b.Error(err)
			return
		}
	}
}

func benchmarkAllocatorAllocMemFiler(b *testing.B, sz int) {
	f := NewMemFiler()
	benchmarkAllocatorAlloc(b, f, sz)
}

func BenchmarkAllocatorAllocMemFiler1e0(b *testing.B) {
	benchmarkAllocatorAllocMemFiler(b, 0)
}

func BenchmarkAllocatorAllocMemFiler1e1(b *testing.B) {
	benchmarkAllocatorAllocMemFiler(b, 1e1)
}

func BenchmarkAllocatorAllocMemFiler1e2(b *testing.B) {
	benchmarkAllocatorAllocMemFiler(b, 1e2)
}

func BenchmarkAllocatorAllocMemFiler1e3(b *testing.B) {
	benchmarkAllocatorAllocMemFiler(b, 1e3)
}

func benchmarkAllocatorAllocSimpleFileFiler(b *testing.B, sz int) {
	os.Remove(testDbName)
	f, err := os.OpenFile(testDbName, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		b.Fatal(err)
	}

	defer func() {
		f.Close()
		os.Remove(testDbName)
	}()

	benchmarkAllocatorAlloc(b, NewSimpleFileFiler(f), sz)
}

func BenchmarkAllocatorAllocSimpleFileFiler0(b *testing.B) {
	benchmarkAllocatorAllocSimpleFileFiler(b, 0)
}

func BenchmarkAllocatorAllocSimpleFileFiler1e1(b *testing.B) {
	benchmarkAllocatorAllocSimpleFileFiler(b, 1e1)
}

func BenchmarkAllocatorAllocSimpleFileFiler1e2(b *testing.B) {
	benchmarkAllocatorAllocSimpleFileFiler(b, 1e2)
}

func BenchmarkAllocatorAllocSimpleFileFiler1e3(b *testing.B) {
	benchmarkAllocatorAllocSimpleFileFiler(b, 1e3)
}

func benchmarkAllocatorAllocRollbackFiler(b *testing.B, sz int) {
	os.Remove(testDbName)
	f, err := os.OpenFile(testDbName, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		b.Fatal(err)
	}

	defer func() {
		f.Close()
		os.Remove(testDbName)
	}()

	g := NewSimpleFileFiler(f)
	var filer *RollbackFiler
	if filer, err = NewRollbackFiler(
		g,
		func() error {
			sz, err := filer.Size()
			if err != nil {
				return err
			}

			if err = g.Truncate(sz); err != nil {
				return err
			}

			return g.Sync()
		},
		g,
	); err != nil {
		b.Error(err)
		return
	}

	benchmarkAllocatorAlloc(b, filer, sz)
}

func BenchmarkAllocatorAllocRollbackFiler0(b *testing.B) {
	benchmarkAllocatorAllocRollbackFiler(b, 0)
}

func BenchmarkAllocatorAllocRollbackFiler1e1(b *testing.B) {
	benchmarkAllocatorAllocRollbackFiler(b, 1e1)
}

func BenchmarkAllocatorAllocRollbackFiler1e2(b *testing.B) {
	benchmarkAllocatorAllocRollbackFiler(b, 1e2)
}

func BenchmarkAllocatorAllocRollbackFiler1e3(b *testing.B) {
	benchmarkAllocatorAllocRollbackFiler(b, 1e3)
}

func benchmarkAllocatorAllocACIDFiler(b *testing.B, sz int) {
	os.Remove(testDbName)
	os.Remove(walName)
	f, err := os.OpenFile(testDbName, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		b.Fatal(err)
	}

	defer func() {
		f.Close()
		os.Remove(testDbName)
	}()

	wal, err := os.OpenFile(walName, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		b.Fatal(err)
	}

	defer func() {
		wal.Close()
		os.Remove(walName)
	}()

	filer, err := NewACIDFiler(NewSimpleFileFiler(f), wal)
	if err != nil {
		b.Error(err)
		return
	}

	benchmarkAllocatorAlloc(b, filer, sz)
}

func BenchmarkAllocatorAllocACIDFiler0(b *testing.B) {
	benchmarkAllocatorAllocACIDFiler(b, 0)
}

func BenchmarkAllocatorAllocACIDFiler1e1(b *testing.B) {
	benchmarkAllocatorAllocACIDFiler(b, 1e1)
}

func BenchmarkAllocatorAllocACIDFiler1e2(b *testing.B) {
	benchmarkAllocatorAllocACIDFiler(b, 1e2)
}

func BenchmarkAllocatorAllocACIDFiler1e3(b *testing.B) {
	benchmarkAllocatorAllocACIDFiler(b, 1e3)
}

func benchmarkAllocatorRndFree(b *testing.B, f Filer, sz int) {
	b.SetBytes(int64(sz))

	if err := f.BeginUpdate(); err != nil {
		b.Error(err)
		return
	}

	a, err := NewFLTAllocator(f, FLTPowersOf2)
	if err != nil {
		b.Error(err)
		return
	}

	if err = f.EndUpdate(); err != nil {
		b.Error(err)
		return
	}

	v := make([]byte, sz)
	ref := map[int64]struct{}{}
	for i := 0; i < b.N; i++ {
		if err = f.BeginUpdate(); err != nil {
			b.Error(err)
			return
		}

		h, err := a.Alloc(v)
		if h <= 0 || err != nil {
			f.EndUpdate()
			b.Error(h, err)
			return
		}

		ref[h] = struct{}{}

		if err = f.EndUpdate(); err != nil {
			b.Error(err)
			return
		}
	}
	runtime.GC()
	b.ResetTimer()
	for h := range ref {
		if err = f.BeginUpdate(); err != nil {
			b.Error(err)
			return
		}

		if err = a.Free(h); err != nil {
			f.EndUpdate()
			b.Error(h, err)
			return
		}

		if err = f.EndUpdate(); err != nil {
			b.Error(err)
			return
		}
	}
}

func benchmarkAllocatorRndFreeMemFiler(b *testing.B, sz int) {
	f := NewMemFiler()
	benchmarkAllocatorRndFree(b, f, sz)
}

func BenchmarkAllocatorRndFreeMemFiler0(b *testing.B) {
	benchmarkAllocatorRndFreeMemFiler(b, 0)
}

func BenchmarkAllocatorRndFreeMemFiler1e1(b *testing.B) {
	benchmarkAllocatorRndFreeMemFiler(b, 1e1)
}

func BenchmarkAllocatorRndFreeMemFiler1e2(b *testing.B) {
	benchmarkAllocatorRndFreeMemFiler(b, 1e2)
}

func BenchmarkAllocatorRndFreeMemFiler1e3(b *testing.B) {
	benchmarkAllocatorRndFreeMemFiler(b, 1e3)
}

func benchmarkAllocatorRndFreeSimpleFileFiler(b *testing.B, sz int) {
	os.Remove(testDbName)
	<-time.After(5 * time.Second)
	f, err := os.OpenFile(testDbName, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		b.Fatal(err)
	}

	defer func() {
		f.Close()
		os.Remove(testDbName)
	}()

	benchmarkAllocatorRndFree(b, NewSimpleFileFiler(f), sz)
}

func BenchmarkAllocatorRndFreeSimpleFileFiler0(b *testing.B) {
	benchmarkAllocatorRndFreeSimpleFileFiler(b, 0)
}

func BenchmarkAllocatorRndFreeSimpleFileFiler1e1(b *testing.B) {
	benchmarkAllocatorRndFreeSimpleFileFiler(b, 1e1)
}

func BenchmarkAllocatorRndFreeSimpleFileFiler1e2(b *testing.B) {
	benchmarkAllocatorRndFreeSimpleFileFiler(b, 1e2)
}

func BenchmarkAllocatorRndFreeSimpleFileFiler1e3(b *testing.B) {
	benchmarkAllocatorRndFreeSimpleFileFiler(b, 1e3)
}

func benchmarkAllocatorRndFreeRollbackFiler(b *testing.B, sz int) {
	os.Remove(testDbName)
	<-time.After(5 * time.Second)
	f, err := os.OpenFile(testDbName, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		b.Fatal(err)
	}

	defer func() {
		f.Close()
		os.Remove(testDbName)
	}()

	g := NewSimpleFileFiler(f)
	var filer *RollbackFiler
	if filer, err = NewRollbackFiler(
		g,
		func() error {
			sz, err := filer.Size()
			if err != nil {
				return err
			}

			if err = g.Truncate(sz); err != nil {
				return err
			}

			return g.Sync()
		},
		g,
	); err != nil {
		b.Error(err)
		return
	}

	benchmarkAllocatorRndFree(b, filer, sz)
}

func BenchmarkAllocatorRndFreeRollbackFiler0(b *testing.B) {
	benchmarkAllocatorRndFreeRollbackFiler(b, 0)
}

func BenchmarkAllocatorRndFreeRollbackFiler1e1(b *testing.B) {
	benchmarkAllocatorRndFreeRollbackFiler(b, 1e1)
}

func BenchmarkAllocatorRndFreeRollbackFiler1e2(b *testing.B) {
	benchmarkAllocatorRndFreeRollbackFiler(b, 1e2)
}

func BenchmarkAllocatorRndFreeRollbackFiler1e3(b *testing.B) {
	benchmarkAllocatorRndFreeRollbackFiler(b, 1e3)
}

func benchmarkAllocatorRndFreeACIDFiler(b *testing.B, sz int) {
	os.Remove(testDbName)
	os.Remove(walName)
	<-time.After(5 * time.Second)
	f, err := os.OpenFile(testDbName, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		b.Fatal(err)
	}

	defer func() {
		f.Close()
		os.Remove(testDbName)
	}()

	wal, err := os.OpenFile(walName, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		b.Fatal(err)
	}

	defer func() {
		wal.Close()
		os.Remove(walName)
	}()

	filer, err := NewACIDFiler(NewSimpleFileFiler(f), wal)
	if err != nil {
		b.Error(err)
		return
	}

	benchmarkAllocatorRndFree(b, filer, sz)
}

func BenchmarkAllocatorRndFreeACIDFiler0(b *testing.B) {
	benchmarkAllocatorRndFreeACIDFiler(b, 0)
}

func BenchmarkAllocatorRndFreeACIDFiler1e1(b *testing.B) {
	benchmarkAllocatorRndFreeACIDFiler(b, 1e1)
}

func BenchmarkAllocatorRndFreeACIDFiler1e2(b *testing.B) {
	benchmarkAllocatorRndFreeACIDFiler(b, 1e2)
}

func BenchmarkAllocatorRndFreeACIDFiler1e3(b *testing.B) {
	benchmarkAllocatorRndFreeACIDFiler(b, 1e3)
}

func benchmarkAllocatorRndGet(b *testing.B, f Filer, sz int) {
	b.SetBytes(int64(sz))

	if err := f.BeginUpdate(); err != nil {
		b.Error(err)
		return
	}

	a, err := NewFLTAllocator(f, FLTPowersOf2)
	if err != nil {
		b.Error(err)
		return
	}

	if err = f.EndUpdate(); err != nil {
		b.Error(err)
		return
	}

	v := make([]byte, sz)
	ref := map[int64]struct{}{}
	for i := 0; i < b.N; i++ {
		if err = f.BeginUpdate(); err != nil {
			b.Error(err)
			return
		}

		h, err := a.Alloc(v)
		if h <= 0 || err != nil {
			f.EndUpdate()
			b.Error(h, err)
			return
		}

		ref[h] = struct{}{}

		if err = f.EndUpdate(); err != nil {
			b.Error(err)
			return
		}
	}
	runtime.GC()
	b.ResetTimer()
	for h := range ref {
		if err = f.BeginUpdate(); err != nil {
			b.Error(err)
			return
		}

		if _, err = a.Get(v, h); err != nil {
			f.EndUpdate()
			b.Error(h, err)
			return
		}

		if err = f.EndUpdate(); err != nil {
			b.Error(err)
			return
		}
	}
}

func benchmarkAllocatorRndGetMemFiler(b *testing.B, sz int) {
	f := NewMemFiler()
	benchmarkAllocatorRndGet(b, f, sz)
}

func BenchmarkAllocatorRndGetMemFiler0(b *testing.B) {
	benchmarkAllocatorRndGetMemFiler(b, 0)
}

func BenchmarkAllocatorRndGetMemFiler1e1(b *testing.B) {
	benchmarkAllocatorRndGetMemFiler(b, 1e1)
}

func BenchmarkAllocatorRndGetMemFiler1e2(b *testing.B) {
	benchmarkAllocatorRndGetMemFiler(b, 1e2)
}

func BenchmarkAllocatorRndGetMemFiler1e3(b *testing.B) {
	benchmarkAllocatorRndGetMemFiler(b, 1e3)
}

func benchmarkAllocatorRndGetSimpleFileFiler(b *testing.B, sz int) {
	os.Remove(testDbName)
	<-time.After(5 * time.Second)
	f, err := os.OpenFile(testDbName, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		b.Fatal(err)
	}

	defer func() {
		f.Close()
		os.Remove(testDbName)
	}()

	benchmarkAllocatorRndGet(b, NewSimpleFileFiler(f), sz)
}

func BenchmarkAllocatorRndGetSimpleFileFiler0(b *testing.B) {
	benchmarkAllocatorRndGetSimpleFileFiler(b, 0)
}

func BenchmarkAllocatorRndGetSimpleFileFiler1e1(b *testing.B) {
	benchmarkAllocatorRndGetSimpleFileFiler(b, 1e1)
}

func BenchmarkAllocatorRndGetSimpleFileFiler1e2(b *testing.B) {
	benchmarkAllocatorRndGetSimpleFileFiler(b, 1e2)
}

func BenchmarkAllocatorRndGetSimpleFileFiler1e3(b *testing.B) {
	benchmarkAllocatorRndGetSimpleFileFiler(b, 1e3)
}

// func benchmarkAllocatorRndGetRollbackFiler(b *testing.B, sz int) {
// 	os.Remove(testDbName)
// 	f, err := os.OpenFile(testDbName, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
// 	if err != nil {
// 		b.Fatal(err)
// 	}
//
// 	defer func() {
// 		f.Close()
// 		os.Remove(testDbName)
// 	}()
//
// 	g := NewSimpleFileFiler(f)
// 	var filer *RollbackFiler
// 	if filer, err = NewRollbackFiler(
// 		g,
// 		func() error {
// 			sz, err := filer.Size()
// 			if err != nil {
// 				return err
// 			}
//
// 			if err = g.Truncate(sz); err != nil {
// 				return err
// 			}
//
// 			return g.Sync()
// 		},
// 		g,
// 	); err != nil {
// 		b.Error(err)
// 		return
// 	}
//
// 	benchmarkAllocatorRndGet(b, filer, sz)
// }
//
// func BenchmarkAllocatorRndGetRollbackFiler0(b *testing.B) {
// 	benchmarkAllocatorRndGetRollbackFiler(b, 0)
// }
//
// func BenchmarkAllocatorRndGetRollbackFiler1e1(b *testing.B) {
// 	benchmarkAllocatorRndGetRollbackFiler(b, 1e1)
// }
//
// func BenchmarkAllocatorRndGetRollbackFiler1e2(b *testing.B) {
// 	benchmarkAllocatorRndGetRollbackFiler(b, 1e2)
// }
//
// func BenchmarkAllocatorRndGetRollbackFiler1e3(b *testing.B) {
// 	benchmarkAllocatorRndGetRollbackFiler(b, 1e3)
// }
//
// func benchmarkAllocatorRndGetACIDFiler(b *testing.B, sz int) {
// 	dbg("%v: %d", now(), b.N)
// 	os.Remove(testDbName)
// 	os.Remove(walName)
// 	f, err := os.OpenFile(testDbName, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
// 	if err != nil {
// 		b.Fatal(err)
// 	}
//
// 	defer func() {
// 		f.Close()
// 		os.Remove(testDbName)
// 	}()
//
// 	wal, err := os.OpenFile(walName, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
// 	if err != nil {
// 		b.Fatal(err)
// 	}
//
// 	defer func() {
// 		wal.Close()
// 		os.Remove(walName)
// 	}()
//
// 	filer, err := NewACIDFiler(NewSimpleFileFiler(f), wal)
// 	if err != nil {
// 		b.Error(err)
// 		return
// 	}
//
// 	benchmarkAllocatorRndGet(b, filer, sz)
// }
//
// func BenchmarkAllocatorRndGetACIDFiler0(b *testing.B) {
// 	benchmarkAllocatorRndGetACIDFiler(b, 0)
// }
//
// func BenchmarkAllocatorRndGetACIDFiler1e1(b *testing.B) {
// 	benchmarkAllocatorRndGetACIDFiler(b, 1e1)
// }
//
// func BenchmarkAllocatorRndGetACIDFiler1e2(b *testing.B) {
// 	benchmarkAllocatorRndGetACIDFiler(b, 1e2)
// }
//
// func BenchmarkAllocatorRndGetACIDFiler1e3(b *testing.B) {
// 	benchmarkAllocatorRndGetACIDFiler(b, 1e3)
// }

func TestBug20130511(t *testing.T) {
	var (
		data       []byte
		maxHandles = 63 // < 63 passes
		dsz        = 65536
		pollN      = 100
		filer      Filer
		a          *Allocator
		pollcnt    int
		handles    = []int64{}
	)

	data, err := ioutil.ReadFile("lab/1/data")
	if err != nil {
		t.Fatal(err)
	}

	bu := func() {
		if err := filer.BeginUpdate(); err != nil {
			t.Fatal(err)
		}
	}

	eu := func() {
		if err := filer.EndUpdate(); err != nil {
			t.Fatal(err)
		}
	}

	poll := func() {
		pollcnt++
		if pollcnt%pollN == 0 {
			eu()
			t.Logf("commited")
			bu()
		}
	}

	alloc := func(b []byte) {
		h, err := a.Alloc(b)
		if err != nil {
			t.Fatalf("alloc(%#x): %v", len(b), err)
		}

		handles = append(handles, h)
		t.Logf("alloc(%#x) -> %#x\n", len(b), h)
		poll()
	}

	wal, err := ioutil.TempFile("", "")
	if err != nil {
		t.Fatal(err)
	}

	filer, err = NewACIDFiler(NewMemFiler(), wal)
	if err != nil {
		t.Fatal(err)
	}

	bu()
	a, err = NewFLTAllocator(filer, FLTFull)
	if err != nil {
		t.Fatal(err)
	}

	a.Compress = true

	runtime.GC()
	rng := rand.New(rand.NewSource(42))

	for len(handles) < maxHandles {
		alloc(data[:rng.Intn(dsz+1)])
	}
	for len(handles) > 31 { //maxHandles/2 {
		if len(handles) < 2 {
			break
		}

		x := rng.Intn(len(handles))
		h := handles[x]
		ln := len(handles)
		handles[x] = handles[ln-1]
		handles = handles[:ln-1]
		err := a.Free(h)
		if err != nil {
			t.Fatal(err)
		}

		t.Logf("free(%#x)", h)
		poll()
	}
	for _, h := range handles[:6] {
		ln := rng.Intn(dsz + 1)
		err := a.Realloc(h, data[:ln])
		if err != nil {
			t.Fatal(err)
		}

		t.Logf("realloc(h:%#x, sz:%#x)", h, ln)
		poll()
	}

	for len(handles) < maxHandles {
		alloc(data[:rng.Intn(dsz+1)])
	}
	eu()

	t.Logf("PeakWALSize  %d", filer.(*ACIDFiler0).PeakWALSize())
	fn := wal.Name()
	wal.Close()
	os.Remove(fn)
}

/*

jnml@fsc-r630:~/src/github.com/cznic/exp/lldb$ go test -run Bug
--- FAIL: TestBug20130511 (0.53 seconds)
	falloc_test.go:1822: alloc(0x34f2) -> 0x1
	falloc_test.go:1822: alloc(0xabd9) -> 0x1af
	falloc_test.go:1822: alloc(0xa532) -> 0x630
	falloc_test.go:1822: alloc(0x7784) -> 0xa7f
	falloc_test.go:1822: alloc(0xd244) -> 0xdd8
	falloc_test.go:1822: alloc(0x4955) -> 0x131d
	falloc_test.go:1822: alloc(0xf39a) -> 0x1561
	falloc_test.go:1822: alloc(0x5453) -> 0x1b31
	falloc_test.go:1822: alloc(0x6a69) -> 0x1dbe
	falloc_test.go:1822: alloc(0x2317) -> 0x20c8
	falloc_test.go:1822: alloc(0xbbd1) -> 0x21ee
	falloc_test.go:1822: alloc(0x668a) -> 0x26cd
	falloc_test.go:1822: alloc(0x709c) -> 0x29c4
	falloc_test.go:1822: alloc(0xab58) -> 0x2cf1
	falloc_test.go:1822: alloc(0xfa8f) -> 0x316e
	falloc_test.go:1822: alloc(0x53c1) -> 0x374e
	falloc_test.go:1822: alloc(0x72d6) -> 0x39d6
	falloc_test.go:1822: alloc(0x73d7) -> 0x3d14
	falloc_test.go:1822: alloc(0x3627) -> 0x4059
	falloc_test.go:1822: alloc(0xdce) -> 0x420d
	falloc_test.go:1822: alloc(0x3464) -> 0x42a8
	falloc_test.go:1822: alloc(0x6b8c) -> 0x4452
	falloc_test.go:1822: alloc(0xcf16) -> 0x4760
	falloc_test.go:1822: alloc(0x3a49) -> 0x4c9f
	falloc_test.go:1822: alloc(0xd48a) -> 0x4e76
	falloc_test.go:1822: alloc(0x82ce) -> 0x53c9
	falloc_test.go:1822: alloc(0x8a3a) -> 0x576a
	falloc_test.go:1822: alloc(0x8645) -> 0x5b3e
	falloc_test.go:1822: alloc(0x3aa9) -> 0x5ef7
	falloc_test.go:1822: alloc(0x2422) -> 0x60d0
	falloc_test.go:1822: alloc(0xfc8a) -> 0x61ff
	falloc_test.go:1822: alloc(0xb42e) -> 0x67eb
	falloc_test.go:1822: alloc(0xd2b3) -> 0x6ca8
	falloc_test.go:1822: alloc(0xbf5b) -> 0x71ef
	falloc_test.go:1822: alloc(0x1eee) -> 0x76dc
	falloc_test.go:1822: alloc(0x8130) -> 0x77ef
	falloc_test.go:1822: alloc(0xa150) -> 0x7b81
	falloc_test.go:1822: alloc(0x313a) -> 0x7fbf
	falloc_test.go:1822: alloc(0xe9b6) -> 0x814d
	falloc_test.go:1822: alloc(0x38d2) -> 0x86f8
	falloc_test.go:1822: alloc(0x162a) -> 0x88c2
	falloc_test.go:1822: alloc(0xe229) -> 0x8997
	falloc_test.go:1822: alloc(0xd0b6) -> 0x8f14
	falloc_test.go:1822: alloc(0xb8a2) -> 0x9457
	falloc_test.go:1822: alloc(0xe655) -> 0x9920
	falloc_test.go:1822: alloc(0x1456) -> 0x9eb8
	falloc_test.go:1822: alloc(0x18ae) -> 0x9f85
	falloc_test.go:1822: alloc(0xdad9) -> 0xa06a
	falloc_test.go:1822: alloc(0x5f07) -> 0xa5d9
	falloc_test.go:1822: alloc(0x4fab) -> 0xa8a1
	falloc_test.go:1822: alloc(0x5d2d) -> 0xab04
	falloc_test.go:1822: alloc(0xad16) -> 0xadc6
	falloc_test.go:1822: alloc(0x9a9d) -> 0xb253
	falloc_test.go:1822: alloc(0xdf33) -> 0xb676
	falloc_test.go:1822: alloc(0x312d) -> 0xbbe8
	falloc_test.go:1822: alloc(0xfedd) -> 0xbd76
	falloc_test.go:1822: alloc(0x6390) -> 0xc374
	falloc_test.go:1822: alloc(0x14ee) -> 0xc657
	falloc_test.go:1822: alloc(0x6909) -> 0xc727
	falloc_test.go:1822: alloc(0xc33b) -> 0xca2a
	falloc_test.go:1822: alloc(0x1c13) -> 0xcf29
	falloc_test.go:1822: alloc(0x60f8) -> 0xd02e
	falloc_test.go:1822: alloc(0x1e40) -> 0xd2fd
	falloc_test.go:1865: free(0x576a)
	falloc_test.go:1865: free(0x67eb)
	falloc_test.go:1865: free(0xbbe8)
	falloc_test.go:1865: free(0xc374)
	falloc_test.go:1865: free(0x8f14)
	falloc_test.go:1865: free(0x316e)
	falloc_test.go:1865: free(0x88c2)
	falloc_test.go:1865: free(0x1561)
	falloc_test.go:1865: free(0x6ca8)
	falloc_test.go:1865: free(0x61ff)
	falloc_test.go:1865: free(0x630)
	falloc_test.go:1865: free(0xa5d9)
	falloc_test.go:1865: free(0xdd8)
	falloc_test.go:1865: free(0x1af)
	falloc_test.go:1865: free(0x9f85)
	falloc_test.go:1865: free(0x374e)
	falloc_test.go:1865: free(0x5ef7)
	falloc_test.go:1865: free(0x4c9f)
	falloc_test.go:1865: free(0x1dbe)
	falloc_test.go:1865: free(0x4452)
	falloc_test.go:1865: free(0x3d14)
	falloc_test.go:1865: free(0x814d)
	falloc_test.go:1865: free(0x77ef)
	falloc_test.go:1865: free(0xa06a)
	falloc_test.go:1865: free(0x42a8)
	falloc_test.go:1865: free(0x131d)
	falloc_test.go:1865: free(0xc727)
	falloc_test.go:1865: free(0x76dc)
	falloc_test.go:1865: free(0xa7f)
	falloc_test.go:1865: free(0x53c9)
	falloc_test.go:1865: free(0x29c4)
	falloc_test.go:1865: free(0x9eb8)
	falloc_test.go:1875: realloc(h:0x1, sz:0xc61f)
	falloc_test.go:1875: realloc(h:0xa8a1, sz:0x3ef3)
	falloc_test.go:1875: realloc(h:0xb253, sz:0x69f3)
	falloc_test.go:1875: realloc(h:0xca2a, sz:0xa222)
	falloc_test.go:1875: realloc(h:0xab04, sz:0x2a7f)
	falloc_test.go:1810: commited
	falloc_test.go:1875: realloc(h:0x7fbf, sz:0x2f4c)
	falloc_test.go:1875: realloc(h:0xbd76, sz:0xe23e)
	falloc_test.go:1875: realloc(h:0x1b31, sz:0x90f4)
	falloc_test.go:1875: realloc(h:0x9920, sz:0xff81)
	falloc_test.go:1875: realloc(h:0x20c8, sz:0x1f5d)
	falloc_test.go:1875: realloc(h:0x21ee, sz:0xa6c3)
	falloc_test.go:1875: realloc(h:0x26cd, sz:0x95e7)
	falloc_test.go:1875: realloc(h:0xcf29, sz:0xf4f6)
	falloc_test.go:1875: realloc(h:0x2cf1, sz:0xfcbd)
	falloc_test.go:1875: realloc(h:0xc657, sz:0xea8e)
	falloc_test.go:1875: realloc(h:0x86f8, sz:0x98a4)
	falloc_test.go:1875: realloc(h:0x39d6, sz:0x3888)
	falloc_test.go:1875: realloc(h:0x7b81, sz:0x897b)
	falloc_test.go:1875: realloc(h:0x4059, sz:0xbb6f)
	falloc_test.go:1875: realloc(h:0x420d, sz:0x227c)
	falloc_test.go:1875: realloc(h:0x8997, sz:0x64c6)
	falloc_test.go:1875: realloc(h:0x9457, sz:0xfdbd)
	falloc_test.go:1875: realloc(h:0x4760, sz:0x3bed)
	falloc_test.go:1875: realloc(h:0xd02e, sz:0x9a47)
	falloc_test.go:1875: realloc(h:0x4e76, sz:0x745d)
	falloc_test.go:1875: realloc(h:0x71ef, sz:0xfd6e)
	falloc_test.go:1875: realloc(h:0xd2fd, sz:0xfc1e)
	falloc_test.go:1875: realloc(h:0x5b3e, sz:0xa619)
	falloc_test.go:1875: realloc(h:0xadc6, sz:0xa538)
	falloc_test.go:1875: realloc(h:0x60d0, sz:0x21d0)
	falloc_test.go:1875: realloc(h:0xb676, sz:0xf3c2)
	falloc_test.go:1822: alloc(0x45c4) -> 0x8c81
	falloc_test.go:1822: alloc(0x9108) -> 0x9458
	falloc_test.go:1822: alloc(0xf88a) -> 0x67f2
	falloc_test.go:1822: alloc(0x7af9) -> 0xa51b
	falloc_test.go:1822: alloc(0x2e6) -> 0x39ac
	falloc_test.go:1822: alloc(0x876d) -> 0xc658
	falloc_test.go:1822: alloc(0x4b7a) -> 0x86f9
	falloc_test.go:1822: alloc(0xf09) -> 0xce6c
	falloc_test.go:1822: alloc(0x9741) -> 0x6dcf
	falloc_test.go:1822: alloc(0x349c) -> 0x405a
	falloc_test.go:1822: alloc(0x3bb2) -> 0x51bd
	falloc_test.go:1822: alloc(0xbd47) -> 0x71f0
	falloc_test.go:1822: alloc(0xc4ec) -> 0x4942
	falloc_test.go:1818: Block at offset 0x814c0: Expected a free block tag, got 0x73
FAIL
exit status 1
FAIL	github.com/cznic/exp/lldb	0.552s
jnml@fsc-r630:~/src/github.com/cznic/exp/lldb$

*/
