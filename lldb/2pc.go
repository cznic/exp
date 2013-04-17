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

//TODO+ var _ Filer = &ACIDFiler0{}

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

	var key [7]byte
	return len(b), f.data.Set(h2b(key[:], off), b) //TODO verify memory BTree can handle more than maxRq
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

// ACIDFiler0 is a simple (and slow) implementation of 2PC. It uses a single
// write ahead log file to provide the structural ACID properties.
type ACIDFiler0 struct {
	db   Filer
	wal  *os.File
	bwal *bufio.Writer
	data *BTree
}

// NewACIDFiler0 returns a  newly created ACIDFiler0 two write ahead log files,
// wal0 and wal1.
//
// If the WAL file if zero sized then a previous clean shutdown of db is taken
// for granted and no recovery procedure is taken.
//
// If the WAL file if of non zero size then it is checked for having any
// commited/fully finished transactions not yet been reflected in db. If such
// transactions exists they're committed to db. If the recovery process
// finishes successfully, the WALs are truncated to zero size and fsync'ed
// prior to return from NewACIDFiler0.
func NewACIDFiler(db Filer, wal *os.File) (acidDb Filer, err error) {
	fi, err := wal.Stat()
	if err != nil {
		return
	}

	if fi.Size() != 0 {
		panic("TODO") // recovery
	}

	r := &ACIDFiler0{
		wal:  wal,
		data: NewBTree(nil),
	}
	acidWriter := (*acidWriter0)(r)

	var f *RollbackFiler
	if f, err = NewRollbackFiler(
		db,
		func() (err error) {
			// Checkpoint
			sz, err := f.Size()
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

			panic("TODO") //TODO return db.Sync()

		},
		acidWriter,
	); err != nil {
		return
	}

	panic("TODO")
}
*/
