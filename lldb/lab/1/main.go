// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Helper to decide if and which FLT type will be chosen as the only one. (?)

package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/cznic/exp/lldb"
)

const (
	stIdle = iota
	stCollecting
	stIdleArmed
	stCollectingArmed
	stCollectingTriggered
)

var (
	data       []byte
	maxHandles = flag.Int("n", 1000, "N")
	verify     = flag.Bool("verify", false, "verify the resulting DB")
	verbose    = flag.Bool("v", false, "output more info")
	dsz        = flag.Int("dsz", 65536, "maximum datasize")
	pollN      = flag.Int("poll", 128, "transactions to collect before commit")
	keep       = flag.Bool("keep", false, "do not delete the test DB")
	bkl        sync.Mutex
	filer      lldb.Filer
	a          *lldb.Allocator
	pollcnt    int
	handles    []int64
	commits    int
)

func init() {
	var err error
	data, err = ioutil.ReadFile("data")
	if err != nil {
		panic(err)
	}
}

func doVerify(tag string) {
	if !*verify {
		return
	}

	if err := a.Verify(lldb.NewMemFiler(), nil, nil); err != nil {
		log.Fatal(tag, err)
	}
}

func poll() { // 001,011,101
	pollcnt++
	if pollcnt%*pollN == 0 {
		eu()
		commits++
		bu()
	}
}

func bu() {
	if err := filer.BeginUpdate(); err != nil {
		log.Fatal(err)
	}
}

func eu() {
	if err := filer.EndUpdate(); err != nil {
		log.Fatal(err)
	}
}

func alloc(b []byte) {
	h, err := a.Alloc(b)
	if err != nil {
		log.Fatal(err)
	}

	handles = append(handles, h)
	poll()
}

func x(base string, fltKind int) {
	handles = []int64{}
	name := "testdb" + base + "."
	commits = 0

	f, err := ioutil.TempFile(".", name)
	if err != nil {
		log.Fatal(err)
	}

	fn := f.Name()
	wal, err := ioutil.TempFile("", "")
	if err != nil {
		log.Fatal(err)
	}

	filer, err = lldb.NewACIDFiler(lldb.NewSimpleFileFiler(f), wal)
	if err != nil {
		log.Fatal(err)
	}

	bu()
	a, err = lldb.NewFLTAllocator(filer, fltKind)
	if err != nil {
		log.Fatal(err)
	}

	a.Compress = true

	runtime.GC()
	t0 := time.Now()
	rng := rand.New(rand.NewSource(42))

	for len(handles) < *maxHandles {
		alloc(data[:rng.Intn(*dsz+1)])
	}
	for len(handles) > *maxHandles/2 {
		if len(handles) < 2 {
			break
		}

		x := rng.Intn(len(handles))
		h := handles[x]
		ln := len(handles)
		handles[x] = handles[ln-1]
		handles = handles[:ln-1]
		err := a.Free(h)
		if err != nil {
			log.Fatal(err)
		}

		poll()
	}
	for _, h := range handles {
		ln := rng.Intn(*dsz + 1)
		err := a.Realloc(h, data[:ln])
		if err != nil {
			log.Fatal(err)
		}

		poll()
	}
	for len(handles) < *maxHandles {
		alloc(data[:rng.Intn(*dsz+1)])
	}
	eu()

	sz, err := filer.Size()
	if err != nil {
		log.Fatal(err)
	}

	if *verify {
		var stats lldb.AllocStats
		switch err = a.Verify(lldb.NewMemFiler(), nil, &stats); {
		case err != nil:
			log.Fatal(err)
		default:
			log.Printf("%#v", stats)
		}
		if g, e := stats.Handles, int64(len(handles)); g != e {
			log.Fatalf("used handles mismatch %d != %d", g, e)
		}
	}

	if *verbose {
		log.Printf("PeakWALSize  %d", filer.(*lldb.ACIDFiler0).PeakWALSize())
	}

	if err = filer.Close(); err != nil {
		log.Fatal(err)
	}

	d := time.Since(t0)
	fmt.Printf("typ %d, %d handles, sz %10d time %s, %d commits\n", fltKind, len(handles), sz, d, commits)

	switch *keep {
	case false:
		os.Remove(fn)
	default:
		log.Print(fn)
	}
	fn = wal.Name()
	wal.Close()
	os.Remove(fn)
}

func main() {
	flag.Parse()
	log.SetFlags(log.Lshortfile)
	x("0", lldb.FLTPowersOf2)
	x("1", lldb.FLTFib)
	x("2", lldb.FLTFull)
}
