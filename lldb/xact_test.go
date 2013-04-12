// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package lldb

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
)

func filerBytes(f Filer) []byte {
	sz, err := f.Size()
	if err != nil {
		panic(err)
	}

	b := make([]byte, int(sz))
	n, err := f.ReadAt(b, 0)
	if n != len(b) {
		panic(fmt.Errorf("sz %d n %d err %v", sz, n, err))
	}

	return b
}

func cmpFilerBytes(t *testing.T, fa, fb Filer) {
	a, b := filerBytes(fa), filerBytes(fb)
	if !bytes.Equal(a, b) {
		t.Fatalf("Filer content doesn't match")
	}
}

func TestRollbackFiler0(t *testing.T) {
	var r *RollbackFiler
	f, g := NewMemFiler(), NewMemFiler()

	checkpoint := func() (err error) {
		sz, err := r.Size()
		if err != nil {
			return
		}

		return f.Truncate(sz)
	}

	r, err := NewRollbackFiler(f, checkpoint, f)
	if err != nil {
		t.Fatal(err)
	}

	if err = r.BeginUpdate(); err != nil {
		t.Fatal(err)
	}

	if err = r.EndUpdate(); err != nil {
		t.Fatal(err)
	}

	cmpFilerBytes(t, f, g)
}

func TestRollbackFiler1(t *testing.T) {
	const (
		N = 1 << 5 //TODO target 1e6, fails for 1<<6
		O = 1      //TODO 1234
	)

	var r *RollbackFiler
	f, g := NewMemFiler(), NewMemFiler()

	checkpoint := func() (err error) {
		sz, err := r.Size()
		if err != nil {
			return
		}

		return f.Truncate(sz)
	}

	r, err := NewRollbackFiler(f, checkpoint, f)
	if err != nil {
		t.Fatal(err)
	}

	if err = r.BeginUpdate(); err != nil {
		t.Fatal(err)
	}

	rng := rand.New(rand.NewSource(42))
	b := make([]byte, N)
	for i := range b {
		b[i] = byte(rng.Int())
	}

	if _, err = g.WriteAt(b, O); err != nil {
		t.Fatal(err)
	}

	if _, err = r.WriteAt(b, O); err != nil {
		t.Fatal(err)
	}

	b = filerBytes(f)
	if n := len(b); n != 0 {
		t.Fatal(n)
	}

	if err = r.EndUpdate(); err != nil {
		t.Fatal(err)
	}

	cmpFilerBytes(t, f, g)
}
