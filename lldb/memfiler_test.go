// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package lldb

import (
	"bytes"
	"math/rand"
	"testing"
)

// Test automatic page releasing (hole punching) of zero pages
func TestMemFilerWriteAt(t *testing.T) {
	f := NewMemFiler()

	// Add page index 0
	if _, err := f.WriteAt([]byte{1}, 0); err != nil {
		t.Fatal(err)
	}

	if g, e := len(f.m), 1; g != e {
		t.Fatal(g, e)
	}

	// Add page index 1
	if _, err := f.WriteAt([]byte{2}, pgSize); err != nil {
		t.Fatal(err)
	}

	if g, e := len(f.m), 2; g != e {
		t.Fatal(g, e)
	}

	// Add page index 2
	if _, err := f.WriteAt([]byte{3}, 2*pgSize); err != nil {
		t.Fatal(err)
	}

	if g, e := len(f.m), 3; g != e {
		t.Fatal(g, e)
	}

	// Remove page index 1
	if _, err := f.WriteAt(make([]byte, 2*pgSize), pgSize/2); err != nil {
		t.Fatal(err)
	}

	if g, e := len(f.m), 2; g != e {
		t.Logf("%#v", f.m)
		t.Fatal(g, e)
	}

	if err := f.Truncate(1); err != nil {
		t.Fatal(err)
	}

	if g, e := len(f.m), 1; g != e {
		t.Logf("%#v", f.m)
		t.Fatal(g, e)
	}

	if err := f.Truncate(0); err != nil {
		t.Fatal(err)
	}

	if g, e := len(f.m), 0; g != e {
		t.Logf("%#v", f.m)
		t.Fatal(g, e)
	}
}

func TestMemFilerWriteTo(t *testing.T) {
	const max = 1e5
	var b [max]byte
	rng := rand.New(rand.NewSource(42))
	for sz := 0; sz < 1e5; sz += 2053 {
		for i := range b[:sz] {
			b[i] = byte(rng.Int())
		}
		f := NewMemFiler()
		if n, err := f.WriteAt(b[:sz], 0); n != sz || err != nil {
			t.Fatal(n, err)
		}

		var buf bytes.Buffer
		if n, err := f.WriteTo(&buf); n != int64(sz) || err != nil {
			t.Fatal(n, err)
		}

		if !bytes.Equal(b[:sz], buf.Bytes()) {
			t.Fatal("content differs")
		}
	}
}
