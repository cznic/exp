// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// A basic os.File backed Filer.

package lldb

import (
	"os"

	"github.com/cznic/fileutil"
	"github.com/cznic/mathutil"
)

var _ Filer = &SimpleFileFiler{} // Ensure SimpleFileFiler is a Filer.

// SimpleFileFiler is an os.File backed Filer intended for use where structural
// consistency can be reached by other means or where persistence is not
// required (temporary/working data sets).
//
// SimpleFileFiler is the most simple os.File backed Filer implementation as it
// does not really implement BeginUpdate and EndUpdate/Rollback in any way
// which would protect the structural integrity of data. If misused e.g. as a
// real database storage w/o other measures, it can easily cause data loss
// when, for example, a power outage occurs or the updating process terminates
// abruptly.
type SimpleFileFiler struct {
	file *os.File
	nest int
	size int64
}

// NewSimpleFileFiler returns a new SimpleFileFiler.
func NewSimpleFileFiler(f *os.File) *SimpleFileFiler {
	fi, err := os.Stat(f.Name())
	if err != nil {
		panic(err) //TODO must return error
	}

	return &SimpleFileFiler{file: f, size: fi.Size()}
}

// BeginUpdate implements Filer.
func (f *SimpleFileFiler) BeginUpdate() {
	f.nest++
}

// Close implements Filer.
func (f *SimpleFileFiler) Close() (err error) {
	if f.nest != 0 {
		return &ErrPERM{(f.Name() + ":Close")}
	}

	return f.file.Close()
}

// EndUpdate implements Filer.
func (f *SimpleFileFiler) EndUpdate() (err error) {
	if f.nest == 0 {
		return &ErrPERM{(f.Name() + ":EndUpdate")}
	}

	f.nest--
	return
}

// Name implements Filer.
func (f *SimpleFileFiler) Name() string {
	return f.file.Name()
}

// PunchHole implements Filer.
func (f *SimpleFileFiler) PunchHole(off, size int64) (err error) {
	return fileutil.PunchHole(f.file, off, size)
}

// ReadAt implements Filer.
func (f *SimpleFileFiler) ReadAt(b []byte, off int64) (n int, err error) {
	return f.file.ReadAt(b, off)
}

// Rollback implements Filer.
func (f *SimpleFileFiler) Rollback() (err error) { return }

// Size implements Filer.
func (f *SimpleFileFiler) Size() int64 {
	return f.size
}

// Truncate implements Filer.
func (f *SimpleFileFiler) Truncate(size int64) (err error) {
	if size < 0 {
		return &ErrINVAL{"Truncate size", size}
	}

	f.size = size
	return f.file.Truncate(size)
}

// WriteAt implements Filer.
func (f *SimpleFileFiler) WriteAt(b []byte, off int64) (n int, err error) {
	f.size = mathutil.MaxInt64(f.size, int64(len(b))+off)
	return f.file.WriteAt(b, off)
}
