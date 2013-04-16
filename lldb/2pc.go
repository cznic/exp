// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Two Phase Commit & Structural ACID

package lldb

/*
import (
	"os"
)

//TODO+ var _ Filer = &ACIDFiler{}

type acidWriter ACIDFiler

func (f *acidWriter) WriteAt(b []byte, off int64) (n int, err error) {
	panic("TODO")
}

// ACIDFiler used write ahead log and a two phase commit to provide structural
// ACID properties.
type ACIDFiler struct {
	db        Filer
	wal, wal2 *os.File
}

// NewACIDFiler returns a Filer whose structural ACID properties are backed by
// two write ahead log files, wal0 and wal1.
//
// If both of the WAL files are zero sized then a previous clean shutdown of db
// is taken for granted and no recovery procedure is taken.
//
// If any of the WAL files are of non zero size then they're checked for having
// any commited/fully finished transactions not yet been reflected in db. If
// such transactions exists they're committed to db. If the recovery process
// finishes successfully, the WALs are truncated to zero size and fsync'ed
// prior to return from NewACIDFiler.
func NewACIDFiler(db Filer, wal0, wal1 *os.File) (acidDb Filer, err error) {
	fi0, err := wal0.Stat()
	if err != nil {
		return
	}

	fi1, err := wal1.Stat()
	if err != nil {
		return
	}

	if fi0.Size()+fi1.Size() != 0 {
		panic("TODO") // recovery
	}

	r := &ACIDFiler{}

	var f *RollbackFiler
	if f, err = NewRollbackFiler(
		db,
		func() (err error) {
			panic("TODO") //TODO write curr sz to WAL, switch, fsync, etc
		},
		(*acidWriter)(r),
	); err != nil {
		return
	}

	panic("TODO")
}
*/
