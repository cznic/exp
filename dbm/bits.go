// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package dbm

import (
	"github.com/cznic/mathutil"
)

const (
	opOn = iota
	opOff
	opCpl
)

var (
	byteMask = [8][8]byte{ // [from][to]
		[8]uint8{0x01, 0x03, 0x07, 0x0f, 0x1f, 0x3f, 0x7f, 0xff},
		[8]uint8{0x00, 0x02, 0x06, 0x0e, 0x1e, 0x3e, 0x7e, 0xfe},
		[8]uint8{0x00, 0x00, 0x04, 0x0c, 0x1c, 0x3c, 0x7c, 0xfc},
		[8]uint8{0x00, 0x00, 0x00, 0x08, 0x18, 0x38, 0x78, 0xf8},
		[8]uint8{0x00, 0x00, 0x00, 0x00, 0x10, 0x30, 0x70, 0xf0},
		[8]uint8{0x00, 0x00, 0x00, 0x00, 0x00, 0x20, 0x60, 0xe0},
		[8]uint8{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0xc0},
		[8]uint8{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80},
	}

	bitMask = [8]byte{0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80}

	onePage [pgSize]byte
)

func init() {
	for i := range onePage {
		onePage[i] = 0xff
	}
}

// uBits are a File with a bit-manipulation set of methods. It can be useful as
// e.g. a bitmap index[1].
//
//   [1]: http://en.wikipedia.org/wiki/Bitmap_index
type uBits File

func (b *uBits) pageBytes(pgI int64, pgFrom, pgTo, op int) (err error) {
	f := (*File)(b)
	a := (*Array)(f)
	switch op {
	case opOn:
		if pgFrom == 0 && pgTo == pgSize*8-1 {
			return a.Set(onePage[:], pgI)
		}

		_, err = f.writeAt(onePage[pgFrom:pgTo+1], pgI*pgSize+int64(pgFrom), true)
		return
	case opOff:
		if pgFrom == 0 && pgTo == pgSize*8-1 {
			return a.Delete(pgI)
		}

		_, err = f.writeAt(zeroPage[pgFrom:pgTo+1], pgI*pgSize+int64(pgFrom), true)
		return
	case opCpl:
		var buf [pgSize]byte
		var n int
		if n, err = f.readAt(buf[:], pgSize, true); n != pgSize {
			return
		}

		for i, v := range buf[pgFrom : pgTo+1] {
			buf[i] = ^v
		}
		if buf == zeroPage {
			return a.Delete(pgI)
		}

		_, err = f.writeAt(buf[:], pgI*pgSize+int64(pgFrom), true)
		return
	}
	panic("unreachable")
}

func (b *uBits) pageByte(off int64, fromBit, toBit, op int) (err error) {
	f := (*File)(b)
	var buf [1]byte
	if _, err = f.readAt(buf[:], off, true); err != nil {
		return
	}

	switch op {
	case opOn:
		buf[0] |= byteMask[fromBit][toBit]
	case opOff:
		buf[0] &^= byteMask[fromBit][toBit]
	case opCpl:
		buf[0] ^= byteMask[fromBit][toBit]
	}
	_, err = f.writeAt(buf[:], off, true)
	return
}

func (b *uBits) pageBits(pgI int64, fromBit, toBit, op int) (err error) {
	pgFrom, pgTo := fromBit>>3, toBit>>3
	switch from, to := fromBit&7, toBit&7; {
	case from == 0 && to == 7:
		return b.pageBytes(pgI, pgFrom, pgTo, op)
	case from == 0 && to != 7:
		switch pgTo - pgFrom {
		case 0:
			return b.pageByte(pgI*pgSize+int64(pgFrom), from, to, op)
		case 1:
			if err = b.pageByte(pgI*pgSize+int64(pgFrom), from, 7, op); err != nil {
				return
			}

			return b.pageByte(pgI*pgSize+int64(pgTo), 0, to, op)
		default:
			if err = b.pageByte(pgI*pgSize+int64(pgFrom), from, 7, op); err != nil {
				return
			}

			if err = b.pageBytes(pgI, pgFrom+1, pgTo-1, op); err != nil {
				return
			}

			return b.pageByte(pgI*pgSize+int64(pgTo), 0, to, op)
		}
	case from != 0 && to == 7:
		switch pgTo - pgFrom {
		case 0:
			return b.pageByte(pgI*pgSize+int64(pgFrom), from, 7, op)
		case 1:
			if err = b.pageByte(pgI*pgSize+int64(pgFrom), from, 7, op); err != nil {
				return
			}

			return b.pageByte(pgI*pgSize+int64(pgTo), 0, 7, op)
		default:
			if err = b.pageByte(pgI*pgSize+int64(pgFrom), from, 7, op); err != nil {
				return
			}

			if err = b.pageBytes(pgI, pgFrom+1, pgTo-1, op); err != nil {
				return
			}

			return b.pageByte(pgI*pgSize+int64(pgTo), 0, 7, op)
		}
	case from != 0 && to != 7:
		switch pgTo - pgFrom {
		case 0:
			return b.pageByte(pgI*pgSize+int64(pgFrom), from, to, op)
		case 1:
			if err = b.pageByte(pgI*pgSize+int64(pgFrom), from, 7, op); err != nil {
				return
			}

			return b.pageByte(pgI*pgSize+int64(pgTo), 0, to, op)
		default:
			if err = b.pageByte(pgI*pgSize+int64(pgFrom), from, 7, op); err != nil {
				return
			}

			if err = b.pageBytes(pgI, pgFrom+1, pgTo-1, op); err != nil {
				return
			}

			return b.pageByte(pgI*pgSize+int64(pgTo), 0, to, op)
		}
	}
	panic("unreachable")
}

func (b *uBits) ops(fromBit, toBit uint64, op int) (err error) {
	const (
		bitsPerPage     = pgSize * 8
		bitsPerPageMask = bitsPerPage - 1
	)

	rem := toBit - fromBit + 1
	pgI := int64(fromBit >> (pgBits + 3))
	for rem != 0 {
		pgFrom := fromBit & bitsPerPageMask
		pgTo := mathutil.MinUint64(bitsPerPage-1, pgFrom+rem-1)
		n := pgTo - pgFrom + 1
		if err = b.pageBits(pgI, int(pgFrom), int(pgTo), op); err != nil {
			return
		}

		pgI++
		rem -= n
		fromBit += n
	}
	return
}

// On sets run bits starting from bit.
func (b *uBits) uOn(bit, run uint64) (err error) {
	if run == 0 {
		return
	}

	return b.ops(bit, bit+run-1, opOn)
}

// Off resets run bits starting from bit.
func (b *uBits) uOff(bit, run uint64) (err error) {
	if run == 0 {
		return
	}

	return b.ops(bit, bit+run-1, opOff)
}

// Cpl complements run bits starting from bit.
func (b *uBits) uCpl(bit, run uint64) (err error) {
	if run == 0 {
		return
	}

	return b.ops(bit, bit+run-1, opCpl)
}

// Get returns the value at bit and how long is the run.
func (b *uBits) uGet(bit, maxrun uint64) (val bool, run uint64, err error) {
	f := (*File)(b)
	if maxrun == 1 {
		var buf [1]byte
		if _, err = f.readAt(buf[:], int64(bit>>3), true); err != nil {
			return
		}

		return buf[0]&bitMask[bit&7] != 0, 1, nil
	}

	panic("TODO") //TODO
}
