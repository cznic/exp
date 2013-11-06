// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package lldb

import (
	"io"
)

var _ Filer = (*OSFiler)(nil)

// OSFile is an os.File like minimal set of methods allowing to construct a
// Filer.
type OSFile interface {
	io.Closer
	io.ReadWriteSeeker
	Sync() (err error)
	Truncate(size int64) (err error)
}

// OSFiler is like a SimpleFileFiler but based on an OSFile.
type OSFiler struct {
	f    OSFile
	nest int
	name string
}

// NewOSFiler returns a Filer from an OSFile. This Filer is like the
// SimpleFileFiler, it does not implement the transaction related methods.
// name is any string. It's used only by Name.
func NewOSFiler(f OSFile, name string) (r *OSFiler) {
	return &OSFiler{
		f:    f,
		name: name,
	}
}

// BeginUpdate implements Filer.
func (f *OSFiler) BeginUpdate() (err error) {
	f.nest++
	return nil
}

// Close implements Filer.
func (f *OSFiler) Close() (err error) {
	if f.nest != 0 {
		return &ErrPERM{(f.Name() + ":Close")}
	}

	return f.f.Close()
}

// EndUpdate implements Filer.
func (f *OSFiler) EndUpdate() (err error) {
	if f.nest == 0 {
		return &ErrPERM{(f.Name() + ":EndUpdate")}
	}

	f.nest--
	return
}

// Name implements Filer.
func (f *OSFiler) Name() string {
	return f.name
}

// PunchHole implements Filer.
func (f *OSFiler) PunchHole(off, size int64) (err error) {
	return
}

// ReadAt implements Filer.
func (f *OSFiler) ReadAt(b []byte, off int64) (n int, err error) {
	if _, err = f.f.Seek(off, 0); err != nil {
		return
	}

	return f.f.Read(b)
}

// Rollback implements Filer.
func (f *OSFiler) Rollback() (err error) { return }

// Size implements Filer.
func (f *OSFiler) Size() (n int64, err error) {
	return f.f.Seek(0, 2)
}

// Sync implements Filer.
func (f *OSFiler) Sync() (err error) {
	return f.f.Sync()
}

// Truncate implements Filer.
func (f *OSFiler) Truncate(size int64) (err error) {
	return f.f.Truncate(size)
}

// WriteAt implements Filer.
func (f *OSFiler) WriteAt(b []byte, off int64) (n int, err error) {
	if _, err = f.f.Seek(off, 0); err != nil {
		return
	}

	return f.f.Write(b)
}
