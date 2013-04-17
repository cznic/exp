// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Two Phase Commit & Structural ACID

package lldb

/*
import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
)

var _ Filer = &ACIDFiler0{}

type acidWriter0 ACIDFiler0

func (a *acidWriter0) WriteAt(b []byte, off int64) (n int, err error) {
	f := (*ACIDFiler0)(a)
	if f.bwal == nil { // new epoch
		if err = f.data.Clear(); err != nil { // internal error
			return
		}

		f.bwal = bufio.NewWriter(f.wal)
		if err = a.writePacket([]interface{}{wpt00Header, walTypeACIDFiler0, ""}); err != nil {
			return
		}
	}

	if err = a.writePacket([]interface{}{wpt00WriteData, b, off}); err != nil {
		return
	}

	var key [8]byte
	binary.BigEndian.PutUint64(key[:], uint64(off))
	return len(b), f.data.Set(key[:], b) //DONE verify memory BTree can handle more than maxRq
}

func (a *acidWriter0) writePacket(items []interface{}) (err error) {
	f := (*ACIDFiler0)(a)
	b, err := EncodeScalars(items...)
	if err != nil {
		return
	}

	var b4 [4]byte
	binary.BigEndian.PutUint32(b4[:], uint32(len(b)))
	if _, err = f.bwal.Write(b4[:]); err != nil {
		return
	}

	if _, err = f.bwal.Write(b); err != nil {
		return
	}

	if m := (4 + len(b)) % 16; m != 0 {
		var pad [15]byte
		_, err = f.bwal.Write(pad[:16-m])
	}
	return
}

// WAL Packet Tags
const (
	wpt00Header = iota
	wpt00WriteData
	wpt00Checkpoint
)

const (
	walTypeACIDFiler0 = iota
)

// ACIDFiler0 is a very simple, synchronous implementation of 2PC. It uses a
// single write ahead log file to provide (some of the) the structural ACID
// properties.
type ACIDFiler0 struct {
	*RollbackFiler
	db   Filer
	wal  *os.File
	bwal *bufio.Writer
	data *BTree
}

// NewACIDFiler0 returns a  newly created ACIDFiler0 with WAL in wal.
//
// If the WAL if zero sized then a previous clean shutdown of db is taken for
// granted and no recovery procedure is taken.
//
// If the WAL is of non zero size then it is checked for having any
// commited/fully finished transactions not yet been reflected in db. If such
// transactions exists they're committed to db. If the recovery process
// finishes successfully, the WAL is truncated to zero size and fsync'ed prior
// to return from NewACIDFiler0.
func NewACIDFiler(db Filer, wal *os.File) (f *ACIDFiler0, err error) {
	fi, err := wal.Stat()
	if err != nil {
		return
	}

	r := &ACIDFiler0{
		wal:  wal,
		data: NewBTree(nil),
	}

	if fi.Size() != 0 {
		if err = r.recoverDb(db); err != nil {
			return
		}
	}

	acidWriter := (*acidWriter0)(r)

	if r.RollbackFiler, err = NewRollbackFiler(
		db,
		func() (err error) {
			// Checkpoint
			sz, err := r.Size()
			if err != nil {
				return
			}

			if err = acidWriter.writePacket([]interface{}{wpt00Checkpoint, sz}); err != nil {
				return
			}

			if err = r.bwal.Flush(); err != nil {
				return
			}

			r.bwal = nil

			if err = r.wal.Sync(); err != nil {
				return
			}

			// Phase 1 commit complete

			enum, err := r.data.SeekFirst()
			if err != nil {
				return
			}

			for {
				k, v, err := enum.Current()
				if err != nil {
					if err == io.EOF {
						break
					}

					return err
				}

				off := b2h(k)
				if _, err := db.WriteAt(v, off); err != nil {
					return err
				}

				if err = enum.Next(); err != nil {
					if err == io.EOF {
						break
					}

					return err
				}
			}

			if err = db.Truncate(sz); err != nil {
				return
			}

			if err = db.Sync(); err != nil {
				return
			}

			// Phase 2 commit complete

			if err = r.wal.Truncate(0); err != nil {
				return
			}

			return r.wal.Sync()

		},
		acidWriter,
	); err != nil {
		return
	}

	return r, nil
}

func (a *ACIDFiler0) readPacket(f *bufio.Reader) (items []interface{}, err error) {
	var b4 [4]byte
	n, err := f.Read(b4[:])
	if n != 4 {
		return
	}

	ln := int(binary.BigEndian.Uint32(b4[:]))
	m := (4 + ln) % 16
	padd := (16 - m) % 16
	b := make([]byte, ln+padd)
	if n, err = f.Read(b); n != ln+padd {
		return
	}

	return DecodeScalars(b[:ln])
}

func (a *ACIDFiler0) recoverDb(db Filer) (err error) {
	fi, err := a.wal.Stat()
	if err != nil {
		return &ErrILSEQ{More: a.wal.Name()}
	}

	if fi.Size()%16 != 0 {
		return err
	}

	f := bufio.NewReader(a.wal)
	items, err := a.readPacket(f)
	if err != nil {
		return
	}

	if len(items) != 3 || items[0] != wpt00Header || items[1] != walTypeACIDFiler0 {
		return &ErrILSEQ{More: a.wal.Name()}
	}

	tr := NewBTree(nil)

	for {
		items, err = a.readPacket(f)
		if err != nil {
			return
		}

		if len(items) < 2 {
			return &ErrILSEQ{More: a.wal.Name()}
		}

		switch items[0] {
		case wpt00WriteData:
			if len(items) != 3 {
				return &ErrILSEQ{More: a.wal.Name()}
			}

			b, off := items[1].([]byte), items[2].(int64)
			var key [8]byte
			binary.BigEndian.PutUint64(key[:], uint64(off))
			if err = tr.Set(key[:], b); err != nil {
				return
			}
		case wpt00Checkpoint:
			var b1 [1]byte
			if n, err := f.Read(b1[:]); n != 0 || err == nil {
				return &ErrILSEQ{More: a.wal.Name()}
			}

			if len(items) != 2 {
				return &ErrILSEQ{More: a.wal.Name()}
			}

			sz := items[1].(int64)
			enum, err := tr.SeekFirst()
			if err != nil {
				return err
			}

			for {
				k, v, err := enum.Current()
				if err != nil {
					if err == io.EOF {
						break
					}

					return err
				}

				off := b2h(k)
				if _, err = db.WriteAt(v, off); err != nil {
					return err
				}

				if err = enum.Next(); err != nil {
					if err == io.EOF {
						break
					}

					return err
				}
			}

			if err = db.Truncate(sz); err != nil {
				return err
			}

			if err = db.Sync(); err != nil {
				return err
			}

			// Recovery complete

			if err = a.wal.Truncate(0); err != nil {
				return err
			}

			return a.wal.Sync()
		default:
			return &ErrILSEQ{More: a.wal.Name()}
		}
	}
}
*/
