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
	keep       = flag.Bool("keep", false, "do not delete the test DB")
	bkl        sync.Mutex
	filer      lldb.Filer
	a          *lldb.Allocator
	acidState  int
	acidNest   int
	acidTimer  *time.Timer
	closed     bool
)

func init() {
	var err error
	data, err = ioutil.ReadFile("data")
	if err != nil {
		panic(err)
	}
}

func bu() {
	//println("bu")
	if err := filer.BeginUpdate(); err != nil {
		log.Fatal(err)
	}
}

func eu() {
	//println("eu")
	if err := filer.EndUpdate(); err != nil {
		log.Fatal(err)
	}
}

func enter() {
	//println("enter")
	bkl.Lock()
	switch acidState {
	default:
		panic("internal error")
	case stIdle:
		bu()
		acidNest = 1
		acidTimer = time.AfterFunc(time.Second, timeout)
		acidState = stCollecting
	case stCollecting:
		acidNest++
	case stIdleArmed:
		acidNest = 1
		acidState = stCollectingArmed
	case stCollectingArmed:
		acidNest++
	case stCollectingTriggered:
		acidNest++
	}

	bu()
	return
}

func leave() {
	//println("leave")
	switch acidState {
	default:
		panic("internal error")
	case stIdle:
		panic("internal error")
	case stCollecting:
		acidNest--
		if acidNest == 0 {
			acidState = stIdleArmed
		}
	case stIdleArmed:
		panic("internal error")
	case stCollectingArmed:
		acidNest--
		if acidNest == 0 {
			acidState = stIdleArmed
		}
	case stCollectingTriggered:
		acidNest--
		if acidNest == 0 {
			eu()
			acidState = stIdle
		}
	}

	eu()
	bkl.Unlock()
	return
}

func timeout() {
	bkl.Lock()
	defer bkl.Unlock()

	if closed {
		return
	}

	if filer == nil {
		return
	}

	switch acidState {
	default:
		panic("internal error")
	case stIdle:
		panic("internal error")
	case stCollecting:
		acidState = stCollectingTriggered
	case stIdleArmed:
		eu()
		acidState = stIdle
	case stCollectingArmed:
		acidState = stCollectingTriggered
	case stCollectingTriggered:
		panic("internal error")
	}
}

func x(base string, fltKind int) {
	acidState = stIdle
	acidNest = 0
	acidTimer = nil
	closed = false

	handles := []int64{}
	name := "lldb-lab-1-" + base + "db"

	f, err := ioutil.TempFile("", name)
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

	enter()
	{
		a, err = lldb.NewFLTAllocator(filer, fltKind)
		if err != nil {
			log.Fatal(err)
		}
	}
	leave()

	a.Compress = true

	runtime.GC()
	t0 := time.Now()
	rng := rand.New(rand.NewSource(42))

	for len(handles) < *maxHandles {
		ln := rng.Intn(*dsz + 1)
		enter()
		h, err := a.Alloc(data[:ln])
		if err != nil {
			log.Fatal(err)
		}

		leave()
		handles = append(handles, h)
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
		enter()
		err := a.Free(h)
		if err != nil {
			log.Fatal(err)
		}

		leave()
	}

	for _, h := range handles {
		ln := rng.Intn(*dsz + 1)
		enter()
		err := a.Realloc(h, data[:ln])
		if err != nil {
			log.Fatal(err)
		}

		leave()
	}

	for len(handles) < *maxHandles {
		ln := rng.Intn(*dsz + 1)
		enter()
		h, err := a.Alloc(data[:ln])
		if err != nil {
			log.Fatal(err)
		}

		leave()
		handles = append(handles, h)
	}

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
		log.Printf("PeakWALSize %d", filer.(*lldb.ACIDFiler0).PeakWALSize())
	}

	enter()
	if acidTimer != nil {
		acidTimer.Stop()
		acidTimer = nil
	}
	closed = true

	for acidNest > 0 {
		acidNest--
		eu()
	}
	leave()
	if err = filer.Close(); err != nil {
		log.Fatal(err)
	}

	d := time.Since(t0)
	fmt.Printf("compress true, typ %d, %d handles, sz %10d time %s\n", fltKind, len(handles), sz, d)

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
	panic("ATM broken")
	runtime.GOMAXPROCS(2)
	flag.Parse()
	log.SetFlags(log.Lshortfile)
	x("0", lldb.FLTPowersOf2)
	x("1", lldb.FLTFib)
	x("2", lldb.FLTFull)
}
