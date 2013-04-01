// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// A memory-only implementation of Filer.

/*

pgBits: 8
BenchmarkMemFilerWrSeq	   50000	     67556 ns/op	 473.68 MB/s
BenchmarkMemFilerRdSeq	   50000	     64059 ns/op	 499.54 MB/s

pgBits: 9
BenchmarkMemFilerWrSeq	   50000	     38517 ns/op	 830.79 MB/s
BenchmarkMemFilerRdSeq	   50000	     36147 ns/op	 885.27 MB/s

pgBits: 10
BenchmarkMemFilerWrSeq	  100000	     22977 ns/op	1392.67 MB/s
BenchmarkMemFilerRdSeq	  100000	     20193 ns/op	1584.70 MB/s

pgBits: 11
BenchmarkMemFilerWrSeq	  100000	     22537 ns/op	1419.88 MB/s
BenchmarkMemFilerRdSeq	  100000	     14083 ns/op	2272.13 MB/s

pgBits: 12
BenchmarkMemFilerWrSeq	  100000	     22130 ns/op	1445.99 MB/s
BenchmarkMemFilerRdSeq	  200000	     10777 ns/op	2969.25 MB/s

pgBits: 13
BenchmarkMemFilerWrSeq	  100000	     21999 ns/op	1454.56 MB/s
BenchmarkMemFilerRdSeq	  200000	      9561 ns/op	3346.85 MB/s

pgBits: 14
BenchmarkMemFilerWrSeq	  100000	     21980 ns/op	1455.81 MB/s
BenchmarkMemFilerRdSeq	  200000	      8999 ns/op	3555.80 MB/s

pgBits: 15
BenchmarkMemFilerWrSeq	  100000	     22020 ns/op	1453.17 MB/s
BenchmarkMemFilerRdSeq	  200000	      8486 ns/op	3770.90 MB/s

pgBits: 16
BenchmarkMemFilerWrSeq	  100000	     22366 ns/op	1430.74 MB/s
BenchmarkMemFilerRdSeq	  200000	      8993 ns/op	3558.29 MB/s

pgBits: 17
BenchmarkMemFilerWrSeq	  100000	     22426 ns/op	1426.90 MB/s
BenchmarkMemFilerRdSeq	  200000	      8970 ns/op	3567.36 MB/s

pgBits: 18
BenchmarkMemFilerWrSeq	  100000	     22540 ns/op	1419.68 MB/s
BenchmarkMemFilerRdSeq	  200000	      8985 ns/op	3561.27 MB/s

----
$ gp
? \\ BenchmarkMemFilerWrSeq
? plot(x=8,19,[474,831,1393,1420,1446,1455,1456,1453,1431,1427,1420][truncate(x-7)]);

     1456 |'''''''''''''''''_____"""""""""""""""""""""""xxxxxxxxxxxx______
          |           """"""                                             |
          |           :                                                  |
          |           :                                                  |
          |           :                                                  |
          |          :                                                   |
          |          :                                                   |
          |          :                                                   |
          |          :                                                   |
          |     xxxxxx                                                   |
          |     :                                                        |
          |     :                                                        |
          |    :                                                         |
          |    :                                                         |
          ______                                                         |
          |                                                              |
          |                                                              |
          |                                                              |
          |                                                              |
          |                                                              |
          |                                                              |
        0 ,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,
          8                                                             19
? \\ BenchmarkMemFilerRdSeq
? plot(x=8,19,[500,885,1585,2272,2969,3347,3556,3771,3558,3567,3561][truncate(x-7)]);

     3771 |''''''''''''''''''''''''''''''''''''''''"""""'''''''''''''''''|
          |                                  xxxxxx     xxxxxxxxxxxxxxxxxx
          |                            xxxxxx                            |
          |                                                              |
          |                      ______                                  |
          |                      :                                       |
          |                     :                                        |
          |                     :                                        |
          |                 _____                                        |
          |                 :                                            |
          |                :                                             |
          |                :                                             |
          |           ______                                             |
          |           :                                                  |
          |          :                                                   |
          |          :                                                   |
          |     ______                                                   |
          |                                                              |
          ______                                                         |
          |                                                              |
          |                                                              |
        0 ,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,
          8                                                             19
?

*/

package lldb

import (
	"bytes"
	"fmt"
	"github.com/cznic/mathutil"
	"io"
)

const (
	pgBits = 12
	pgSize = 1 << pgBits
	pgMask = pgSize - 1
)

var _ Filer = &MemFiler{} // Ensure MemFiler is a Filer.

type memFilerMap map[int64]*[pgSize]byte

// MemFiler is a memory backed Filer. It implements BeginUpdate, EndUpdate and
// Rollback as no-ops. MemFiler is not automatically persistent, but it has
// ReadFrom and WriteTo methods.
type MemFiler struct {
	m    memFilerMap
	nest int
	size int64
}

// NewMemFiler returns a new MemFiler.
func NewMemFiler() *MemFiler {
	return &MemFiler{m: memFilerMap{}}
}

// BeginUpdate implements Filer.
func (f *MemFiler) BeginUpdate() {
	f.nest++
}

// Close implements Filer.
func (f *MemFiler) Close() (err error) {
	if f.nest != 0 {
		return &ErrPERM{(f.Name() + ":Close")}
	}

	return
}

// EndUpdate implements Filer.
func (f *MemFiler) EndUpdate() (err error) {
	if f.nest == 0 {
		return &ErrPERM{(f.Name() + ":EndUpdate")}
	}

	f.nest--
	return
}

// Name implements Filer.
func (f *MemFiler) Name() string {
	return fmt.Sprintf("%p.memfiler", f)
}

// PunchHole implements Filer.
func (f *MemFiler) PunchHole(off, size int64) (err error) {
	if off < 0 {
		return &ErrINVAL{f.Name() + ":PunchHole off", off}
	}

	if size < 0 || off+size > f.size {
		return &ErrINVAL{f.Name() + ":PunchHole size", size}
	}

	first := off >> pgBits
	if off&pgMask != 0 {
		first++
	}
	off += size - 1
	last := off >> pgBits
	if off&pgMask != 0 {
		last--
	}
	if limit := f.size >> pgBits; last > limit {
		last = limit
	}
	for pg := first; pg <= last; pg++ {
		delete(f.m, pg)
	}
	return
}

var zeroPage [pgSize]byte

// ReadAt implements Filer.
func (f *MemFiler) ReadAt(b []byte, off int64) (n int, err error) {
	avail := f.size - off
	pgI := off >> pgBits
	pgO := int(off & pgMask)
	rem := len(b)
	if int64(rem) >= avail {
		rem = int(avail)
		err = io.EOF
	}
	for rem != 0 && avail > 0 {
		pg := f.m[pgI]
		if pg == nil {
			pg = &zeroPage
		}
		nc := copy(b[:mathutil.Min(rem, pgSize)], pg[pgO:])
		pgI++
		pgO = 0
		rem -= nc
		n += nc
		b = b[nc:]
	}
	return
}

// ReadFrom is a helper to populate MemFiler's content from r.  'n' reports the
// number of bytes read from 'r'.
func (f *MemFiler) ReadFrom(r io.Reader) (n int64, err error) {
	if err = f.Truncate(0); err != nil {
		return
	}

	var (
		b   [pgSize]byte
		rn  int
		off int64
	)

	var rerr error
	for rerr == nil {
		if rn, rerr = r.Read(b[:]); rn != 0 {
			f.WriteAt(b[:rn], off)
			off += int64(rn)
			n += int64(rn)
		}
	}
	if rerr != io.EOF {
		err = rerr
	}
	return
}

// Rollback implements Filer.
func (f *MemFiler) Rollback() (err error) { return }

// Size implements Filer.
func (f *MemFiler) Size() int64 {
	return f.size
}

// Truncate implements Filer.
func (f *MemFiler) Truncate(size int64) (err error) {
	switch {
	case size < 0:
		return &ErrINVAL{"Truncate size", size}
	case size == 0:
		f.m = memFilerMap{}
	}

	first := size >> pgBits
	if size&pgMask != 0 {
		first++
	}
	last := f.size >> pgBits
	if f.size&pgMask != 0 {
		last++
	}
	for ; first < last; first++ {
		delete(f.m, first)
	}

	f.size = size
	return
}

// WriteAt implements Filer.
func (f *MemFiler) WriteAt(b []byte, off int64) (n int, err error) {
	pgI := off >> pgBits
	pgO := int(off & pgMask)
	n = len(b)
	rem := n
	var nc int
	for rem != 0 {
		if pgO == 0 && rem >= pgSize && bytes.Equal(b[:pgSize], zeroPage[:]) {
			delete(f.m, pgI)
			nc = pgSize
		} else {
			pg := f.m[pgI]
			if pg == nil {
				pg = new([pgSize]byte)
				f.m[pgI] = pg
			}
			nc = copy((*pg)[pgO:], b)
		}
		pgI++
		pgO = 0
		rem -= nc
		b = b[nc:]
	}
	f.size = mathutil.MaxInt64(f.size, off+int64(n))
	return
}

// WriteTo is a helper to copy/persist MemFiler's content to w.  If w is also
// an io.WriterAt then WriteTo may attempt to _not_ write any big, for some
// value of big, runs of zeros, i.e. it will attempt to punch holes, where
// possible, in `w` if that happens to be a freshly created or to zero length
// truncated OS file.  'n' reports the number of bytes written to 'w'.
func (f *MemFiler) WriteTo(w io.Writer) (n int64, err error) {
	var (
		b      [pgSize]byte
		wn, rn int
		off    int64
		rerr   error
	)

	if wa, ok := w.(io.WriterAt); ok {
		lastPgI := f.size >> pgBits
		for pgI := int64(0); pgI <= lastPgI; pgI++ {
			sz := pgSize
			if pgI == lastPgI {
				sz = int(f.size & pgMask)
			}
			pg := f.m[pgI]
			if pg != nil {
				wn, err = wa.WriteAt(pg[:sz], off)
				if err != nil {
					return
				}

				n += int64(wn)
				off += int64(sz)
				if wn != sz {
					return n, io.ErrShortWrite
				}
			}
		}
		return
	}

	var werr error
	for rerr == nil {
		if rn, rerr = f.ReadAt(b[:], off); rn != 0 {
			off += int64(rn)
			if wn, werr = w.Write(b[:rn]); werr != nil {
				return n, werr
			}

			n += int64(wn)
		}
	}
	if rerr != io.EOF {
		err = rerr
	}
	return
}
