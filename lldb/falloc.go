// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// The storage space management.

package lldb

import (
	"bytes"
	"fmt"
	"io"

	"code.google.com/p/snappy-go/snappy"
	"github.com/cznic/mathutil"
)

// A AllocStats records statistics about a Filer. It can be optionally filled by
// Allocator.Verify, if successful.
type AllocStats struct {
	TotalAtoms  int64 // total number of atoms == AllocAtoms + FreeAtoms
	AllocBytes  int64 // bytes allocated (after decompression, if/where used)
	AllocAtoms  int64 // atoms allocated/used, including relocation atoms
	Relocations int64 // number of relocated used blocks
	FreeAtoms   int64 // atoms unused
}

/*

Allocator implements "raw" storage space management (allocation and
deallocation) for a low level of a DB engine.  The storage is an abstraction
provided by a Filer.

The terms MUST or MUST NOT, if/where used in the documentation of Allocator,
written in all caps as seen here, are a requirement for any possible
alternative implementations aiming for compatibility with this one.

Filer file

A Filer file, or simply 'file', is a linear, contiguous sequence of blocks.
Blocks may be either free (currently unused) or allocated (currently used).
Some blocks may eventually become virtual in a sense as they may not be
realized in the storage (sparse files).

File blocks

A block is a linear, contiguous sequence of atoms. The first and last atoms of
a block provide information about, for example, whether the block is free or
used, what is the size of the block, etc.  Details are discussed elsewhere.

Block atoms

An atom is a fixed size piece of a block (and thus of a file too); it is 16
bytes long. A consequence is that for a valid file:

 filesize == 0 (mod 16)

Block handles

A handle is an integer referring to a block. The reference is the number of the
atom the block starts with plus one. Put in other way, considering 16 byte
atoms:

 handle == offset / 16 + 1
 offset == 16 * (handle - 1)

`offset` is the offset of the first byte of the block, measured in bytes
- as in fseek(3). Handle has type `int64`, but only the lower 7 bytes may be
nonzero while referring to a block, both in code as well as when persisted in
the the file's internal bookkeeping structures - see 'Block types' bellow. So a
handle is effectively only `uint56`.  This also means that the maximum usable
size of a file is 2^56 atoms.  That is 2^60 bytes == 1 exabyte (10^18 bytes).

Nil handles

A handle with numeric value of '0' refers to no block.

Zero padding

A padding is used to round-up a block size to be a whole number of atoms. Any
padding, if present, MUST be all zero bytes. Note that the size of padding is
in [0, 15].

Content wiping

When a block is deallocated, it's data content is not wiped as the added
overhead may be substantial while not necessarily needed. Client code should
however overwrite the content of any block having sensitive data with zeros (or
random garbage or whatever safe) - before deallocating the block.

Block tags

Every block is tagged in its first byte (a head tag) and last byte (tail tag).
Block types are:

 1. Short content used block (head tags 0x00-0xFB)
 2. Long content used block (head tag 0xFC)
 3. Relocated used block (head tag 0xFD)
 4. Short, single atom, free block (head tag 0xFE)
 5. Long free block (head tag 0xFF)

Note: Relocated used block, 3. above (head tag 0xFD) MUST NOT refer to blocks
other then 1. or 2. above (head tags 0x00-0xFC).

Content blocks

Used blocks (head tags 0x00-0xFC) tail tag distinguish used/unused block and if
content is compressed or not.

Content compression

The tail flag of an used block is one of

	CC == 0 // Content is not compressed.
	CC == 1 // Content is in Snappy[1] compression format.

	[1]: http://code.google.com/p/snappy

If compression of written content is enabled, there are two cases: If
compressed size < original size then the compressed content should be written
if it will save at least one atom of the block. If compressed size >= original
size then the compressed content should not be used.

Short content block

Short content block carries content of length between N == 0(0x00) and N ==
251(0xFB) bytes.

	|<-first atom start  ...  last atom end->|
	+---++--   ...   --+--   ...   --++------+
	| 0 ||    1...     |  0x*...0x*E || 0x*F |
	+---++--   ...   --+--   ...   --++------+
	| N ||   content   |   padding   ||  CC  |
	+---++--   ...   --+--   ...   --++------+

	A == (N+1)/16 + 1        // The number of atoms in the block [1, 16]
	padding == 15 - (N+1)%16 // Length of the zero padding

Long content block

Long content block carries content of length between N == 252(0xFC) and N ==
65787(0x100FB) bytes.

	|<-first atom start    ...     last atom end->|
	+------++------+-- ... --+--  ...   --++------+
	|  0   || 1..2 |   3...  | 0x*...0x*E || 0x*F |
	+------++------+-- ... --+--  ...   --++------+
	| 0xFC ||  M   | content |  padding   ||  CC  |
	+------++------+-- ... --+--  ...   --++------+

	A == (N+3)/16 + 1        // The number of atoms in the block [16, 4112]
	M == N % 0x10000         // Stored as 2 bytes in network byte order
	padding == 15 - (N+3)%16 // Length of the zero padding

Relocated used block

Relocated block allows to permanently assign a handle to some content and
resize the content anytime afterwards without having to update all the possible
existing references; the handle can be constant while the content size may be
dynamic. When relocating a block, any space left by the original block content,
above this single atom block, MUST be reclaimed.

Relocations MUST point to only to a used short or long block == blocks with
tags 0x00...0xFC.

	+------++------+---------++----+
	|  0   || 1..7 | 8...14  || 15 |
	+------++------+---------++----+
	| 0xFD ||  H   | padding || 0  |
	+------++------+---------++----+

H is the handle of the relocated block in network byte order.

Free blocks

Free blocks are the result of space deallocation. Free blocks are organized in
one ore more doubly linked lists, abstracted by the FLT interface. Free blocks
MUST be "registered" by putting them in such list. Allocator MUST reuse a big
enough free block, if such exists, before growing the file size. When a free
block is created by deallocation or reallocation it MUST be joined with any
adjacently existing free blocks before "registering". If the resulting free
block is now a last block of a file, the free block MUST be discarded and the
file size MUST be truncated accordingly instead. Put differently, there MUST
NOT ever be a free block at the file end.


A single free atom

Is an unused block of size 1 atom.

	+------++------+--------++------+
	|  0   || 1..7 | 8...14 ||  15  |
	+------++------+--------++------+
	| 0xFE ||  P   |   N    || 0xFE |
	+------++------+--------++------+

P and N, stored in network byte order, are the previous and next free block
handles in the doubly linked list to which this free block belongs.

A long unused block

Is an unused block of size > 1 atom.

	+------++------+-------+---------+- ... -+----------++------+
	|  0   || 1..7 | 8..14 | 15...21 |       | Z-7..Z-1 ||  Z   |
	+------++------+-------+---------+- ... -+----------++------+
	| 0xFF ||  S   |   P   |    N    | Leak  |    S     || 0xFF |
	+------++------+-------+---------+- ... -+----------++------+

	Z == 16 * S - 1

S is the size of this unused block in atoms. P and N are the previous and next
free block handles in the doubly linked list to which this free block belongs.
Leak contains any data the block had before deallocating this block.  See also
the subtitle 'Content wiping' above. S, P and N are stored in network byte
order. Large free blocks may trigger a consideration of file hole punching of
the Leak field - for some value of 'large'.

Note: No Allocator method returns io.EOF.

*/
type Allocator struct {
	f        Filer
	flt      *flt
	Compress bool   // enables content compression
	zbuf     []byte // [de]compression buffer
}

// NewAllocator returns a new Allocator. To open an existing file, pass its
// Filer. To create a "new" file, pass a Filer which file is of zero size.
func NewAllocator(f Filer, flt FLT) (*Allocator, error) {
	fltab, err := newFlt(flt)
	if err != nil {
		return nil, err
	}

	return &Allocator{
		f:   f,
		flt: fltab,
	}, nil
}

// Alloc allocates storage space for b and returns the handle of the new block
// with content set to b or an error, if any. The returned handle is valid only
// while the block is used - until the block is deallocated. No two valid
// handles share the same value within the same Filer, but the any value of a
// handle not referring to any used block may become valid any time as a result
// of Alloc.
//
// Passing handles not obtained initially from Alloc or not anymore valid to
// any other Allocator methods can result in an irreparably corrupted database.
func (a *Allocator) Alloc(b []byte) (handle int64, err error) {
	var c allocatorBlock
	if b, _, err = a.makeUsedBlock(&c, b); err != nil {
		return
	}

	return a.alloc(b, &c)
}

func (a *Allocator) alloc(b []byte, c *allocatorBlock) (h int64, err error) {
	rqAtoms := n2atoms(len(b))
	if h, err = a.flt.find(int64(rqAtoms)); err != nil {
		return
	}

	if h == 0 { // must grow
		h = off2h(a.f.Size())
		err = a.writeUsedBlock(h, c, b)
		return
	}

	// Handle is the first item of a free blocks list.
	tag, s, prev, next, err := a.nfo(h)
	if err != nil {
		return
	}

	if tag != tagFreeShort && tag != tagFreeLong {
		err = &ErrILSEQ{Type: ErrExpFreeTag, Off: h2off(h), Arg: int64(tag)}
		return
	}

	if prev != 0 {
		err = &ErrILSEQ{Type: ErrHead, Off: h2off(h), Arg: prev}
		return
	}

	if s < int64(rqAtoms) {
		err = &ErrILSEQ{Type: ErrSmall, Arg: int64(rqAtoms), Arg2: s, Off: h2off(h)}
		return
	}

	if err = a.unlink(h, s, prev, next); err != nil {
		return
	}

	if s > int64(rqAtoms) {
		freeH := h + int64(rqAtoms)
		freeAtoms := s - int64(rqAtoms)
		if err = a.link(freeH, freeAtoms); err != nil {
			return
		}
	}
	return h, a.writeUsedBlock(h, c, b)
}

// Free deallocates the block referred to by handle or returns an error, if
// any.
//
// After Free succeeds, handle is invalid and must not be used.
//
// Handle must have been obtained initially from Alloc and must be still valid,
// otherwise a database may get irreparably corrupted.
func (a *Allocator) Free(handle int64) (err error) {
	if handle <= 0 || handle > maxHandle {
		return &ErrINVAL{"Allocator.Free: handle out of limits", handle}
	}

	return a.free(handle, 0, true)
}

func (a *Allocator) free(h, from int64, acceptRelocs bool) (err error) {
	//fmt.Printf("free(h %#x from %#x acceptRelocs %t)\n", h, from, acceptRelocs)
	tag, atoms, _, n, err := a.nfo(h)
	if err != nil {
		return
	}

	switch tag {
	default:
		// nop
	case tagUsedLong:
		// nop
	case tagUsedRelocated:
		if !acceptRelocs {
			return &ErrILSEQ{Type: ErrUnexpReloc, Off: h2off(h), Arg: h2off(from)}
		}

		if err = a.free(n, h, false); err != nil {
			return
		}
	case tagFreeShort, tagFreeLong:
		return &ErrINVAL{"Allocator.Free: attempt to free a free block at off", h2off(h)}
	}

	return a.free2(h, atoms)
}

func (a *Allocator) free2(h, atoms int64) (err error) {
	//fmt.Printf("free2(h %#x off %#x atoms %#x)\n", h, h2off(h), atoms)
	sz := a.f.Size()
	ltag, latoms, lp, ln, err := a.leftNfo(h)
	if err != nil {
		return
	}

	if ltag != tagFreeShort && ltag != tagFreeLong {
		latoms = 0
	}

	var rtag byte
	var ratoms, rp, rn int64

	isTail := h2off(h)+atoms*16 == sz
	if !isTail {
		if rtag, ratoms, rp, rn, err = a.nfo(h + atoms); err != nil {
			return
		}
	}

	if rtag != tagFreeShort && rtag != tagFreeLong {
		ratoms = 0
	}

	switch {
	case latoms == 0 && ratoms == 0:
		// -> isolated <-
		if isTail { // cut tail
			return a.f.Truncate(h2off(h))
		}

		return a.link(h, atoms)
	case latoms == 0 && ratoms != 0:
		// right join ->
		if err = a.unlink(h+atoms, ratoms, rp, rn); err != nil {
			return
		}

		return a.link(h, atoms+ratoms)
	case latoms != 0 && ratoms == 0:
		// <- left join
		if err = a.unlink(h-latoms, latoms, lp, ln); err != nil {
			return
		}

		if isTail {
			return a.f.Truncate(h2off(h - latoms))
		}

		return a.link(h-latoms, latoms+atoms)
	case latoms != 0 && ratoms != 0:
		// <- middle join ->
		lh, rh := h-latoms, h+atoms
		if err = a.unlink(lh, latoms, lp, ln); err != nil {
			return
		}

		// Prev unlink may have invalidated rp or rn
		if _, _, rp, rn, err = a.nfo(rh); err != nil {
			return
		}

		if err = a.unlink(rh, ratoms, rp, rn); err != nil {
			return
		}

		return a.link(h-latoms, latoms+atoms+ratoms)
	}
	panic("unreachable")
}

// Add a free block h to the appropriate free list
func (a *Allocator) link(h, atoms int64) (err error) {
	next := a.flt.head(atoms)

	if err = a.makeFree(h, atoms, 0, next); err != nil {
		return
	}

	return a.flt.setHead(h, atoms)
}

// Remove free block h from the free list
func (a *Allocator) unlink(h, atoms, p, n int64) (err error) {
	switch {
	case p == 0 && n == 0:
		// single item list, must be head
		return a.flt.setHead(0, atoms)
	case p == 0 && n != 0:
		// head of list (has next item[s])
		if err = a.prev(n, 0); err != nil {
			return
		}

		// new head
		return a.flt.setHead(n, atoms)
	case p != 0 && n == 0:
		// last item in list
		return a.next(p, 0)
	case p != 0 && n != 0:
		// intermediate item in a list
		if err = a.next(p, n); err != nil {
			return
		}

		return a.prev(n, p)
	}
	panic("unreachable")
}

// Return len(slice) == n, reuse src if possible.
func need(n int, src []byte) []byte {
	if cap(src) < n {
		return make([]byte, n)
	}

	return src[:n]
}

// Get returns the data content of a block referred to by handle or an error if
// any.  The returned slice may be a sub-slice of dst if dst was large enough
// to hold the entire content.  Otherwise, a newly allocated slice will be
// returned.  It is valid to pass a nil dst.
//
// If the content was stored using compression then it is transparently
// returned decompressed.
//
// Handle must have been obtained initially from Alloc and must be still valid,
// otherwise invalid data may be returned without detecting the error.
func (a *Allocator) Get(dst []byte, handle int64) (b []byte, err error) {
	var first [16]byte
	relocated := false
	relocSrc := handle
reloc:
	if handle <= 0 || handle > maxHandle {
		return nil, &ErrINVAL{"Allocator.Get: handle out of limits", handle}
	}

	off := h2off(handle)
	if err = a.read(first[:], off); err != nil {
		return
	}

	switch tag := first[0]; tag {
	default:
		dlen := int(tag)
		atoms := n2atoms(dlen)
		switch atoms {
		case 1:
			switch tag := first[15]; tag {
			default:
				return nil, &ErrILSEQ{Type: ErrTailTag, Off: off, Arg: int64(tag)}
			case tagNotCompressed:
				b = need(dlen, dst)
				copy(b, first[1:])
				return
			case tagCompressed:
				return snappy.Decode(dst, first[1:dlen+1])
			}
		default:
			var cc [1]byte
			dlen := int(tag)
			atoms := n2atoms(dlen)
			tailOff := off + 16*int64(atoms) - 1
			if err = a.read(cc[:], tailOff); err != nil {
				return
			}

			switch tag := cc[0]; tag {
			default:
				return nil, &ErrILSEQ{Type: ErrTailTag, Off: off, Arg: int64(tag)}
			case tagNotCompressed:
				b = need(dlen, dst)
				off += 1
				if err = a.read(b, off); err != nil {
					b = dst[:0]
				}
				return
			case tagCompressed:
				a.zbuf = need(dlen, a.zbuf)
				off += 1
				if err = a.read(a.zbuf[:dlen], off); err != nil {
					return dst[:0], err
				}

				return snappy.Decode(dst, a.zbuf)
			}
		}
	case 0:
		return dst[:0], nil
	case tagUsedLong:
		var cc [1]byte
		dlen := m2n(int(first[1])<<8 | int(first[2]))
		atoms := n2atoms(dlen)
		tailOff := off + 16*int64(atoms) - 1
		if err = a.read(cc[:], tailOff); err != nil {
			return
		}

		switch tag := cc[0]; tag {
		default:
			return nil, &ErrILSEQ{Type: ErrTailTag, Off: off, Arg: int64(tag)}
		case tagNotCompressed:
			b = need(dlen, dst)
			off += 3
			if err = a.read(b, off); err != nil {
				b = dst[:0]
			}
			return
		case tagCompressed:
			a.zbuf = need(dlen, a.zbuf)
			off += 3
			if err = a.read(a.zbuf[:dlen], off); err != nil {
				return dst[:0], err
			}

			return snappy.Decode(dst, a.zbuf)
		}
	case tagFreeShort, tagFreeLong:
		return nil, &ErrILSEQ{Type: ErrExpUsedTag, Off: off, Arg: int64(tag)}
	case tagUsedRelocated:
		if relocated {
			return nil, &ErrILSEQ{Type: ErrUnexpReloc, Off: off, Arg: relocSrc}
		}

		handle = b2h(first[1:])
		relocated = true
		goto reloc
	}
	panic("unreachable")
}

// Realloc sets the content of a block referred to by handle ot or returns an
// error, if any.
//
// Handle must have been obtained initially from Alloc and must be still valid,
// otherwise a database may get irreparably corrupted.
func (a *Allocator) Realloc(handle int64, b []byte) (err error) {
	if handle <= 0 || handle > maxHandle {
		return &ErrINVAL{"Allocator.Free: handle out of limits", handle}
	}

	return a.realloc(handle, b)
}

func (a *Allocator) realloc(handle int64, b []byte) (err error) {
	var (
		b8 [8]byte
		c  allocatorBlock

		dlen, needAtoms0 int
	)

	if b, needAtoms0, err = a.makeUsedBlock(&c, b); err != nil {
		return
	}

	needAtoms := int64(needAtoms0)

	off := h2off(handle)
	if err = a.read(b8[:], off); err != nil {
		return
	}

	switch tag := b8[0]; tag {
	default:
		dlen = int(b8[0])
	case tagUsedLong:
		dlen = m2n(int(b8[1])<<8 | int(b8[2]))
	case tagUsedRelocated:
		if err = a.free(b2h(b8[1:]), handle, false); err != nil {
			return err
		}

		dlen = 0
	case tagFreeShort, tagFreeLong:
		return &ErrINVAL{"Allocator.Realloc: invalid handle", handle}
	}

	atoms := int64(n2atoms(dlen))
retry:
	switch {
	case needAtoms < atoms:
		// in place shrink
		if err = a.writeUsedBlock(handle, &c, b); err != nil {
			return
		}

		fh, fa, sz := handle+needAtoms, atoms-needAtoms, a.f.Size()
		if h2off(fh)+16*fa == sz {
			return a.f.Truncate(h2off(fh))
		}

		return a.free2(fh, fa)
	case needAtoms == atoms:
		// in place replace
		return a.writeUsedBlock(handle, &c, b)
	case needAtoms > atoms:
		// in place extend or relocate
		sz := a.f.Size()
		off := h2off(handle)
		switch {
		case off+atoms*16 == sz:
			// relocating tail block - shortcut
			return a.writeUsedBlock(handle, &c, b)
		default:
			if off+atoms*16 < a.f.Size() {
				// handle is not a tail block, check right neighbour
				rh := handle + atoms
				rtag, ratoms, p, n, e := a.nfo(rh)
				if e != nil {
					return e
				}

				if rtag == tagFreeShort || rtag == tagFreeLong {
					// Right neighbour is a free block
					if needAtoms <= atoms+ratoms {
						// can expand in place
						if err = a.unlink(rh, ratoms, p, n); err != nil {
							return
						}

						atoms += ratoms
						goto retry

					}
				}
			}
		}

		if atoms > 1 {
			if err = a.realloc(handle, nil); err != nil {
				return
			}
		}

		var newH int64
		if newH, err = a.alloc(b, &c); err != nil {
			return err
		}

		rb := [16]byte{0: tagUsedRelocated}
		h2b(rb[1:], newH)
		if err = a.writeAt(rb[:], h2off(handle)); err != nil {
			return
		}

		return a.writeUsedBlock(newH, &c, b)
	}
	panic("unreachable")
}

func (a *Allocator) writeAt(b []byte, off int64) (err error) {
	var n int
	if n, err = a.f.WriteAt(b, off); err != nil {
		return
	}

	if n != len(b) {
		err = io.ErrShortWrite
	}
	return
}

func (a *Allocator) write(off int64, b ...[]byte) (err error) {
	for _, part := range b {
		if err = a.writeAt(part, off); err != nil {
			return
		}

		off += int64(len(part))
	}
	return
}

func (a *Allocator) read(b []byte, off int64) (err error) {
	var rn int
	if rn, err = a.f.ReadAt(b, off); rn != len(b) {
		//TODO- return fmt.Errorf("ReadAt(size %d, off %#x) == (%d, %q)", len(b), off, rn, err)
		return &ErrILSEQ{Type: ErrOther, Off: off, More: err}
	}

	return nil
}

// nfo returns h's tag. If it's a free block then return also (s)ize (in
// atoms), (p)rev and (n)ext. If it's a used block then only (s)ize is returned
// (again in atoms). If it's a used relocate block then (n)ext is set to the
// relocation target handle.
func (a *Allocator) nfo(h int64) (tag byte, s, p, n int64, err error) {
	off := h2off(h)
	rq := int64(22)
	if fsize := a.f.Size(); off+rq >= fsize {
		if rq = fsize - off; rq < 15 {
			err = io.ErrUnexpectedEOF
			return
		}
	}

	var buf [22]byte
	if err = a.read(buf[:rq], off); err != nil {
		return
	}

	switch tag = buf[0]; tag {
	default:
		s = int64(n2atoms(int(tag)))
	case tagUsedLong:
		s = int64(n2atoms(m2n(int(buf[1])<<8 | int(buf[2]))))
	case tagFreeLong:
		if rq < 22 {
			err = io.ErrUnexpectedEOF
			return
		}

		s, p, n = b2h(buf[1:]), b2h(buf[8:]), b2h(buf[15:])
	case tagUsedRelocated:
		s, n = 1, b2h(buf[1:])
	case tagFreeShort:
		s, p, n = 1, b2h(buf[1:]), b2h(buf[8:])
	}
	return
}

// leftNfo returns nfo for h's left neighbour if h > 1 and the left neighbour
// is a free block. Otherwise all zero values are returned instead.
func (a *Allocator) leftNfo(h int64) (tag byte, s, p, n int64, err error) {
	if !(h > 1) {
		return
	}

	var buf [8]byte
	off := h2off(h)
	if err = a.read(buf[:], off-8); err != nil {
		return
	}

	switch tag := buf[7]; tag {
	case tagFreeShort:
		return a.nfo(h - 1)
	case tagFreeLong:
		return a.nfo(h - b2h(buf[:]))
	}
	return
}

// Set h.prev = p
func (a *Allocator) prev(h, p int64) (err error) {
	var b [7]byte
	off := h2off(h)
	if err = a.read(b[:1], off); err != nil {
		return
	}

	switch tag := b[0]; tag {
	default:
		return &ErrILSEQ{Type: ErrExpFreeTag, Off: off, Arg: int64(tag)}
	case tagFreeShort:
		off += 1
	case tagFreeLong:
		off += 8
	}
	return a.writeAt(h2b(b[:7], p), off)
}

// Set h.next = n
func (a *Allocator) next(h, n int64) (err error) {
	var b [7]byte
	off := h2off(h)
	if err = a.read(b[:1], off); err != nil {
		return
	}

	switch tag := b[0]; tag {
	default:
		return &ErrILSEQ{Type: ErrExpFreeTag, Off: off, Arg: int64(tag)}
	case tagFreeShort:
		off += 8
	case tagFreeLong:
		off += 15
	}
	return a.writeAt(h2b(b[:7], n), off)
}

// Make the filer image @h a free block.
func (a *Allocator) makeFree(h, atoms, prev, next int64) (err error) {
	var buf [22]byte
	switch {
	case atoms == 1:
		buf[0], buf[15] = tagFreeShort, tagFreeShort
		h2b(buf[1:], prev)
		h2b(buf[8:], next)
		if err = a.write(h2off(h), buf[:16]); err != nil {
			return
		}
	default:

		buf[0] = tagFreeLong
		h2b(buf[1:], atoms)
		h2b(buf[8:], prev)
		h2b(buf[15:], next)
		if err = a.write(h2off(h), buf[:22]); err != nil {
			return
		}

		h2b(buf[:], atoms)
		buf[7] = tagFreeLong
		if err = a.write(h2off(h+atoms)-8, buf[:8]); err != nil {
			return
		}
	}
	if prev != 0 {
		if err = a.next(prev, h); err != nil {
			return
		}
	}

	if next != 0 {
		err = a.prev(next, h)
	}
	return
}

type allocatorBlock struct {
	headSize int
	head     [3]byte
	padding  [15]byte
	tail     [8]byte
	tailSize int
}

func (a *Allocator) makeUsedBlock(c *allocatorBlock, b []byte) (w []byte, rqAtoms int, err error) {
	c.headSize = 1
	c.tail[0] = tagNotCompressed
	c.tailSize = 1
	w = b

	var n int
	if n = len(b); n > maxRq {
		return nil, 0, &ErrINVAL{"Allocator.makeUsedBlock: content size out of limits", n}
	}

	rqAtoms = n2atoms(n)
	if a.Compress && n > 14 { // attempt compression
		if a.zbuf, err = snappy.Encode(a.zbuf, b); err != nil {
			return
		}

		n2 := len(a.zbuf)
		if rqAtoms2 := n2atoms(n2); rqAtoms2 < rqAtoms { // compression saved at least a single atom
			w, n, rqAtoms, c.tail[0] = a.zbuf, n2, rqAtoms2, tagCompressed
		}
	}
	switch n <= maxShort {
	case true:
		c.head[0] = byte(n)
	case false:
		m := n2m(n)
		c.head[0], c.head[1], c.head[2] = tagUsedLong, byte(m>>8), byte(m)
		c.headSize = 3
	}
	return
}

func (a *Allocator) writeUsedBlock(h int64, c *allocatorBlock, b []byte) (err error) {
	return a.write(h2off(h), c.head[:c.headSize], b, c.padding[:n2padding(len(b))], c.tail[:c.tailSize])
}

func (a *Allocator) verifyUnused(h, totalAtoms int64, tag byte, log func(error) bool, fast bool) (atoms, prev, next int64, err error) {
	switch tag {
	default:
		panic("internal error")
	case tagFreeShort:
		var b [16]byte
		off := h2off(h)
		if err = a.read(b[:], off); err != nil {
			return
		}

		if b[15] != tagFreeShort {
			err = &ErrILSEQ{Type: ErrShortFreeTailTag, Off: off, Arg: int64(b[15])}
			log(err)
			return
		}

		atoms, prev, next = 1, b2h(b[1:]), b2h(b[8:])
	case tagFreeLong:
		var b [22]byte
		off := h2off(h)
		if err = a.read(b[:], off); err != nil {
			return
		}

		atoms, prev, next = b2h(b[1:]), b2h(b[8:]), b2h(b[15:])
		if fast {
			return
		}

		if atoms < 2 {
			err = &ErrILSEQ{Type: ErrLongFreeBlkTooShort, Off: off, Arg: int64(atoms)}
			break
		}

		if h+atoms-1 > totalAtoms {
			err = &ErrILSEQ{Type: ErrLongFreeBlkTooLong, Off: off, Arg: atoms}
			break
		}

		if prev > totalAtoms {
			err = &ErrILSEQ{Type: ErrLongFreePrevBeyondEOF, Off: off, Arg: next}
			break
		}

		if next > totalAtoms {
			err = &ErrILSEQ{Type: ErrLongFreeNextBeyondEOF, Off: off, Arg: next}
			break
		}

		toff := h2off(h+atoms) - 8
		if err = a.read(b[:8], toff); err != nil {
			return
		}

		if b[7] != tag {
			err = &ErrILSEQ{Type: ErrLongFreeTailTag, Off: off, Arg: int64(b[7])}
			break
		}

		if s2 := b2h(b[:]); s2 != atoms {
			err = &ErrILSEQ{Type: ErrVerifyTailSize, Off: off, Arg: atoms, Arg2: s2}
			break
		}

	}
	if err != nil {
		log(err)
	}
	return
}

func (a *Allocator) verifyUsed(h, totalAtoms int64, tag byte, buf, ubuf []byte, log func(error) bool, fast bool) (dlen int, atoms, link int64, err error) {
	var (
		padding  int
		doff     int64
		padZeros [15]byte
		tailBuf  [16]byte
	)

	switch tag {
	default: // Short used
		dlen = int(tag)
		atoms = int64((dlen+1)/16) + 1
		padding = 15 - (dlen+1)%16
		doff = h2off(h) + 1
	case tagUsedLong:
		off := h2off(h) + 1
		var b2 [2]byte
		if err = a.read(b2[:], off); err != nil {
			return
		}

		dlen = m2n(int(b2[0])<<8 | int(b2[1]))
		atoms = int64((dlen+3)/16) + 1
		padding = 15 - (dlen+3)%16
		doff = h2off(h) + 3
	case tagUsedRelocated:
		dlen = 7
		atoms = 1
		padding = 7
		doff = h2off(h) + 1
	case tagFreeShort, tagFreeLong:
		panic("internal error")
	}

	if fast {
		if tag == tagUsedRelocated {
			dlen = 0
			if err = a.read(buf[:7], doff); err != nil {
				return
			}

			link = b2h(buf)
		}

		return dlen, atoms, link, nil
	}

	if ok := h+atoms-1 <= totalAtoms; !ok { // invalid last block
		err = &ErrILSEQ{Type: ErrVerifyUsedSpan, Off: h2off(h), Arg: atoms}
		log(err)
		return
	}

	tailsz := 1 + padding
	off := h2off(h) + 16*atoms - int64(tailsz)
	if err = a.read(tailBuf[:tailsz], off); err != nil {
		return 0, 0, 0, err
	}

	if ok := bytes.Equal(padZeros[:padding], tailBuf[:padding]); !ok {
		err = &ErrILSEQ{Type: ErrVerifyPadding, Off: h2off(h)}
		log(err)
		return
	}

	var cc byte
	switch cc = tailBuf[padding]; cc {
	default:
		err = &ErrILSEQ{Type: ErrTailTag, Off: h2off(h)}
		log(err)
		return
	case tagCompressed:
		if tag == tagUsedRelocated {
			err = &ErrILSEQ{Type: ErrTailTag, Off: h2off(h)}
			log(err)
			return
		}

		fallthrough
	case tagNotCompressed:
		if err = a.read(buf[:dlen], doff); err != nil {
			return 0, 0, 0, err
		}
	}

	if cc == tagCompressed {
		if ubuf, err = snappy.Decode(ubuf, buf[:dlen]); err != nil || len(ubuf) > maxRq {
			err = &ErrILSEQ{Type: ErrDecompress, Off: h2off(h)}
			log(err)
			return
		}

		dlen = len(ubuf)
	}

	if tag == tagUsedRelocated {
		link = b2h(buf)
		if link == 0 {
			err = &ErrILSEQ{Type: ErrNullReloc, Off: h2off(h)}
			log(err)
			return
		}

		if link > totalAtoms { // invalid last block
			err = &ErrILSEQ{Type: ErrRelocBeyondEOF, Off: h2off(h), Arg: link}
			log(err)
			return
		}
	}

	return
}

var nolog = func(error) bool { return false }

// Verify attempts to find any structural errors in a Filer wrt the
// organization of it as defined by Allocator. 'bitmap' is a scratch pad for
// necessary bookkeeping and will grow to at most to Allocator's
// Filer.Size()/128 (0,78%).  Any problems found are reported to 'log' except
// non verify related errors like disk read fails etc.  If 'log' returns false
// or the error doesn't allow to (reliably) continue, the verification process
// is stopped and an error is returned from the Verify function. Passing a nil
// log works like providing a log function always returning false. Any
// non-structural errors, like for instance Filer read errors, are NOT reported
// to 'log', but returned as the Verify's return value, because Verify cannot
// proceed in such cases.  Verify returns nil only if it fully completed
// verifying Allocator's Filer without detecting any error.
//
// It is recommended to limit the number reported problems by returning false
// from 'log' after reaching some limit. Huge and corrupted DB can produce an
// overwhelming error report dataset.
//
// The verifying process will scan the whole DB at least 3 times (a trade
// between processing space and time consumed). It doesn't read the content of
// free blocks above the head/tail info bytes. If the 3rd phase detects lost
// free space, then a 4th scan (a faster one) is performed to precisely report
// all of them.
//
// If the DB/Filer to be verified is reasonably small, respective if its
// size/128 can comfortably fit within process's free memory, then it is
// recommended to consider using a MemFiler for the bit map.
//
// Statistics are returned via 'stats' if non nil. The statistics are valid
// only if Verify succeeded, ie. it didn't reported anything to log and it
// returned a nil error.
func (a *Allocator) Verify(bitmap Filer, log func(error) bool, stats *AllocStats) (err error) { //TODO-
	if log == nil {
		log = nolog
	}

	if n := bitmap.Size(); n != 0 {
		return &ErrINVAL{"Allocator.Verify: bit map initial size non zero (%d)", n}
	}

	var bits int64
	bitMask := [8]byte{1, 2, 4, 8, 16, 32, 64, 128}
	byteBuf := []byte{0}

	//TODO later
	// +performance, this implementation is hopefully correct but _very_
	// naive, probably good as a prototype only. Use maybe a MemFiler
	// "cache" etc.
	bit := func(on bool, h int64) (wasOn bool, err error) {
		m := bitMask[h&7]
		off := h >> 3
		var v byte
		if off < bitmap.Size() {
			if n, err := bitmap.ReadAt(byteBuf, off); n != 1 {
				//TODO- return false, fmt.Errorf("Allocator.Verify - reading bitmap: %s", err)
				return false, &ErrILSEQ{Type: ErrOther, Off: off, More: fmt.Errorf("Allocator.Verify - reading bitmap: %s", err)}
			}

			v = byteBuf[0]
		}
		switch wasOn = v&m != 0; on {
		case true:
			if !wasOn {
				v |= m
				bits++
			}
		case false:
			if wasOn {
				v ^= m
				bits--
			}
		}
		byteBuf[0] = v
		if n, err := bitmap.WriteAt(byteBuf, off); n != 1 || err != nil {
			//TODO- return false, fmt.Errorf("Allocator.Verify - writing bitmap: %s", err)
			return false, &ErrILSEQ{Type: ErrOther, Off: off, More: fmt.Errorf("Allocator.Verify - writing bitmap: %s", err)}
		}

		return
	}

	// Phase 1 - sequentially scan a.f to reliably determine block
	// boundaries. Set a bit for every block start.
	var (
		buf, ubuf       [maxRq]byte
		prevH, h, atoms int64
		wasOn           bool
		tag             byte
		st              AllocStats
		dlen            int
	)

	fsz := a.f.Size()
	ok := fsz%16 == 0
	totalAtoms := fsz / atomLen
	if !ok {
		err = &ErrILSEQ{Type: ErrFileSize, Arg: fsz}
		log(err)
		return
	}

	st.TotalAtoms = totalAtoms
	prevTag := -1
	lastH := int64(-1)

	for h = 1; h <= totalAtoms; h += atoms {
		prevH = h // For checking last block == used

		off := h2off(h)
		if err = a.read(buf[:1], off); err != nil {
			return
		}

		switch tag = buf[0]; tag {
		default: // Short used
			fallthrough
		case tagUsedLong, tagUsedRelocated:
			if dlen, atoms, _, err = a.verifyUsed(h, totalAtoms, tag, buf[:], ubuf[:], log, false); err != nil {
				return
			}

			st.AllocAtoms += atoms
			st.AllocBytes += int64(dlen)
			if tag == tagUsedRelocated {
				st.Relocations++
			}
		case tagFreeShort, tagFreeLong:
			if prevTag == tagFreeShort || prevTag == tagFreeLong {
				err = &ErrILSEQ{Type: ErrAdjacentFree, Off: h2off(lastH), Arg: off}
				log(err)
				return
			}

			if atoms, _, _, err = a.verifyUnused(h, totalAtoms, tag, log, false); err != nil {
				return
			}

			st.FreeAtoms += atoms
		}

		if wasOn, err = bit(true, h); err != nil {
			return
		}

		if wasOn {
			panic("internal error")
		}

		prevTag = int(tag)
		lastH = h
	}

	if totalAtoms != 0 && (tag == tagFreeShort || tag == tagFreeLong) {
		err = &ErrILSEQ{Type: ErrFreeTailBlock, Off: h2off(prevH)}
		log(err)
		return
	}

	// Phase 2 - check used blocks, turn off the map bit for every used
	// block.
	for h = 1; h <= totalAtoms; h += atoms {
		off := h2off(h)
		if err = a.read(buf[:1], off); err != nil {
			return
		}

		var link int64
		switch tag = buf[0]; tag {
		default: // Short used
			fallthrough
		case tagUsedLong, tagUsedRelocated:
			if _, atoms, link, err = a.verifyUsed(h, totalAtoms, tag, buf[:], ubuf[:], log, true); err != nil {
				return
			}
		case tagFreeShort, tagFreeLong:
			if atoms, _, _, err = a.verifyUnused(h, totalAtoms, tag, log, true); err != nil {
				return
			}
		}

		turnoff := true
		switch tag {
		case tagUsedRelocated:
			if err = a.read(buf[:1], h2off(link)); err != nil {
				return
			}

			switch linkedTag := buf[0]; linkedTag {
			case tagFreeShort, tagFreeLong, tagUsedRelocated:
				err = &ErrILSEQ{Type: ErrInvalidRelocTarget, Off: off, Arg: link}
				log(err)
				return
			}

		case tagFreeShort, tagFreeLong:
			turnoff = false
		}

		if !turnoff {
			continue
		}

		if wasOn, err = bit(false, h); err != nil {
			return
		}

		if !wasOn {
			panic("internal error")
		}

	}

	// Phase 3 - using the flt.Report() check heads link to proper free
	// blocks.  For every free block, walk the list, verify the {next,
	// prev} links and turn the respective map bit off. After processing
	// all free lists, the map bits count should be zero. Otherwise there
	// are "lost" free blocks.

	var prev, next, fprev, fnext int64
	rep, err := a.flt.Report()
	if err != nil {
		return
	}

	for _, list := range rep {
		for prev, next = 0, list.Head(); next != 0; prev, next = next, fnext {
			if wasOn, err = bit(false, next); err != nil {
				return
			}

			if !wasOn {
				err = &ErrILSEQ{Type: ErrFLT, Off: h2off(next), Arg: h}
				log(err)
				return
			}

			off := h2off(next)
			if err = a.read(buf[:1], off); err != nil {
				return
			}

			switch tag = buf[0]; tag {
			default:
				panic("internal error")
			case tagFreeShort, tagFreeLong:
				if atoms, fprev, fnext, err = a.verifyUnused(next, totalAtoms, tag, log, true); err != nil {
					return
				}

				if min := list.MinSize(); atoms < min {
					err = &ErrILSEQ{Type: ErrFLTSize, Off: h2off(next), Arg: atoms, Arg2: min}
					log(err)
					return
				}

				if fprev != prev {
					err = &ErrILSEQ{Type: ErrFreeChaining, Off: h2off(next)}
					log(err)
					return
				}
			}
		}

	}

	if bits == 0 { // Verify succeeded
		if stats != nil {
			*stats = st
		}
		return
	}

	// Phase 4 - if after phase 3 there are lost free blocks, report all of
	// them to 'log'
	for i := range ubuf { // setup zeros for compares
		ubuf[i] = 0
	}

	var off, lh int64
	rem := bitmap.Size()
	for rem != 0 {
		rq := int(mathutil.MinInt64(64*1024, rem))
		var n int
		if n, err = bitmap.ReadAt(buf[:rq], off); n != rq {
			//TODO- return fmt.Errorf("bitmap ReadAt(size %d, off %#x): %s", rq, off, err)
			return &ErrILSEQ{Type: ErrOther, Off: off, More: fmt.Errorf("bitmap ReadAt(size %d, off %#x): %s", rq, off, err)}
		}

		if !bytes.Equal(buf[:rq], ubuf[:rq]) {
			for d, v := range buf[:rq] {
				if v != 0 {
					for i, m := range bitMask {
						if v&m != 0 {
							lh = 8*(off+int64(d)) + int64(i)
							err = &ErrILSEQ{Type: ErrLostFreeBlock, Off: h2off(lh)}
							log(err)
							return
						}
					}
				}
			}
		}

		off += int64(rq)
		rem -= int64(rq)
	}

	return
}
