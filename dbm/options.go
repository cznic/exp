// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package dbm

import (
	"crypto/sha1"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/cznic/exp/lldb"
)

const (
	// BeginUpdate/EndUpdate/Rollback will be no-ops. All operations
	// updating a DB will be written immediately including partial updates
	// during operation's progress. If any of the updates fails, the DB can
	// become unusable. The same applies to DB crashes and/or any other non
	// clean DB shutdown.
	ACIDNone = iota

	// Enable transactions. BeginUpdate/EndUpdate/Rollback will be
	// effective. All operations on the DB will be automatically performed
	// within a transaction. Operations will thus either succeed completely
	// or have no effect at all - they will be rollbacked in case of any
	// error. If any update fails the DB will not be corrupted. DB crashes
	// and/or any other non clean DB shutdown may still render the DB
	// unusable.
	ACIDTransactions

	// Enable durability. Same as ACIDTransactions plus enables 2PC and
	// WAL.  Updates to the DB will be first made permanent in a WAL and
	// only after that reflected in the DB. A DB will automatically recover
	// from crashes and/or any other non clean DB shutdown. Only last
	// uncommited transaction (transaction in progress ATM of a crash) can
	// get lost.
	ACIDFull
)

// Options are passed to the DB create/open functions to amend the behavior of
// those functions. The compatibility promise is the same as of struct types in
// the Go standard library - introducing changes can be made only by adding new
// exported fields, which is backward compatible as long as client code uses
// field names to assign values of imported struct types literals.
type Options struct {
	// See the OptACID typed constants documentation.
	ACID int

	// The write ahead log pathname. Applicable iff ACID == ACIDFull. May
	// be left empty in which case an unspecified pathname will be chosen,
	// which is computed from the DB name and which will be in the same
	// directory as the DB. Moving or renaming the DB while it is shut down
	// will break it's connection to the automatically computed name.
	// Moving both the files (the DB and the WAL) into another directory
	// with no renaming is safe.
	//
	// On opening an existing DB the WAL file must exist if it should be
	// used. If it is of zero size then a clean shutdown of the DB is
	// assumed, otherwise an automatic DB recovery is performed.
	//
	// On creating a new DB the WAL file must not exist. It's not safe to
	// write to a WAL file as it may contain unprocessed DB recovery data.
	WAL string

	// Time to collect transactions before committing them into the WAL.
	// Applicable if ACID == ACIDFull. All updates are held in memory
	// during the grace period so it should not be more than few seconds at
	// most.  GracePeriod == 0 will 2PC every single DB operation which is
	// very slow. (Not yet implemented)
	GracePeriod time.Duration //TODO
	wal         *os.File
	checked     bool
}

func (o *Options) check(dbname string, new bool) (err error) {
	if o.checked {
		return
	}

	switch o.ACID {
	default:
		return fmt.Errorf("Unsupported Options.ACID: %d", o.ACID)
	case ACIDNone, ACIDTransactions:
	case ACIDFull:
		o.WAL = o.walName(dbname, o.WAL)
		switch new {
		case true:
			if o.wal, err = os.OpenFile(o.WAL, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0666); err != nil {
				return
			}
		case false:
			if o.wal, err = os.OpenFile(o.WAL, os.O_RDWR, 0666); err != nil {
				return
			}
		}
	}

	o.checked = true
	return
}

func (o *Options) walName(dbname, wal string) (r string) {
	if wal != "" {
		return filepath.Clean(wal)
	}

	base := filepath.Base(filepath.Clean(dbname))

	h := sha1.New()
	io.WriteString(h, base)
	return filepath.Join(filepath.Dir(dbname), fmt.Sprintf(".%x", h.Sum(nil)))
}

func (o *Options) acidFiler(db *DB, f lldb.Filer) (r lldb.Filer, err error) {
	if !o.checked {
		panic("internal error")
	}

	switch o.ACID {
	default:
		panic("internal error")
	case ACIDNone:
		r = f
	case ACIDTransactions:
		var rf *lldb.RollbackFiler
		if rf, err = lldb.NewRollbackFiler(
			f,
			func() error {
				sz, err := rf.Size()
				if err != nil {
					return err
				}

				return f.Truncate(sz)
			},
			f,
		); err != nil {
			return
		}

		db.xact = true
		r = rf
	case ACIDFull:
		if r, err = lldb.NewACIDFiler(f, o.wal); err != nil {
			return
		}

		db.xact = true
	}
	return
}
