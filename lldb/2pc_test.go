// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Two Phase Commit & Structural ACID

package lldb

import (
	"bytes"
	"math/rand"
	"testing"
)

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
