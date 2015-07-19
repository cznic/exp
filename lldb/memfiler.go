// Copyright 2014 The lldb Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// A memory-only implementation of Filer.

package lldb

import (
	"bytes"
	"fmt"
	"io"

	"github.com/cznic/fileutil"
	"github.com/cznic/mathutil"
)

const (
	maxCacheBytes = 64 * 1024 * 1024 //TODO Add Options item for this.
	maxCachePages = (maxCacheBytes + pgSize - 1) / pgSize
	pgBits        = 16
	pgMask        = pgSize - 1
	pgSize        = 1 << pgBits
)

var (
	_ Filer = (*MemFiler)(nil)
	_ Filer = (*cache)(nil)
)

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
func (f *MemFiler) BeginUpdate() error {
	f.nest++
	return nil
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
		return &ErrPERM{(f.Name() + ": EndUpdate")}
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
		return &ErrINVAL{f.Name() + ": PunchHole off", off}
	}

	if size < 0 || off+size > f.size {
		return &ErrINVAL{f.Name() + ": PunchHole size", size}
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
	if !fileutil.IsEOF(rerr) {
		err = rerr
	}
	return
}

// Rollback implements Filer.
func (f *MemFiler) Rollback() (err error) { return }

// Size implements Filer.
func (f *MemFiler) Size() (int64, error) {
	return f.size, nil
}

// Sync implements Filer.
func (f *MemFiler) Sync() error {
	return nil
}

// Truncate implements Filer.
func (f *MemFiler) Truncate(size int64) (err error) {
	switch {
	case size < 0:
		return &ErrINVAL{"Truncate size", size}
	case size == 0:
		f.m = memFilerMap{}
		f.size = 0
		return
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
	if !fileutil.IsEOF(rerr) {
		err = rerr
	}
	return
}

type cache struct {
	Filer
	m *MemFiler
}

func newCache(f Filer) Filer {
	c := &cache{
		Filer: f,
		m:     NewMemFiler(),
	}
	return c
}

func (c *cache) ReadAt(b []byte, off int64) (n int, err error) {
	avail, err := c.Size()
	if err != nil {
		return 0, err
	}

	avail -= off
	pgI := off >> pgBits
	pgO := int(off & pgMask)
	rem := len(b)
	truncated := false
	if int64(rem) >= avail {
		rem = int(avail)
		truncated = true
	}
	for rem != 0 && avail > 0 {
		pg, err := c.load(pgI, -1)
		if err != nil {
			return n, err
		}

		nc := copy(b[:mathutil.Min(rem, pgSize)], pg[pgO:])
		pgI++
		pgO = 0
		rem -= nc
		n += nc
		b = b[nc:]
	}
	if truncated {
		return n, io.EOF
	}

	return n, nil
}

func (c *cache) load(pgI int64, wr int) (*[pgSize]byte, error) {
	if pg := c.m.m[pgI]; pg != nil {
		return pg, nil
	}

	if len(c.m.m) >= maxCachePages {
		for k := range c.m.m {
			delete(c.m.m, k)
			break
		}
	}
	if wr >= pgSize {
		pg := &[pgSize]byte{}
		c.m.m[pgI] = pg
		return pg, nil
	}

	sz, err := c.Filer.Size()
	if err != nil {
		return nil, err
	}

	off := pgI << pgBits
	avail := sz - off
	if avail <= 0 {
		return nil, io.EOF
	}

	rq := int(mathutil.MinInt64(avail, pgSize))
	pg := &[pgSize]byte{}
	if _, err = c.Filer.ReadAt(pg[:rq], off); err != nil {
		return nil, err
	}
	c.m.m[pgI] = pg
	return pg, nil
}

func (c *cache) Truncate(size int64) error {
	c.m.Truncate(size)
	return c.Filer.Truncate(size)
}

func (c *cache) WriteAt(b []byte, off int64) (n int, err error) {
	if n, err := c.Filer.WriteAt(b, off); err != nil {
		return n, err
	}

	pgI := off >> pgBits
	pgO := int(off & pgMask)
	n = len(b)
	rem := n
	var nc int
	for rem != 0 {
		pg, err := c.load(pgI, rem)
		if err != nil {
			return n, err
		}
		nc = copy((*pg)[pgO:], b)
		pgI++
		pgO = 0
		rem -= nc
		b = b[nc:]
	}
	c.m.size = mathutil.MaxInt64(c.m.size, off+int64(n))
	return n, nil
}
