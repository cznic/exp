// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Helper to decide if and which FLT type will be chosen as the only one.

package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"runtime"
	"time"

	"github.com/cznic/exp/lldb"
)

var (
	data       []byte
	secs       = time.Tick(time.Second)
	maxHandles = flag.Int("n", 1000, "N")
)

func init() {
	var err error
	data, err = ioutil.ReadFile("data")
	if err != nil {
		panic(err)
	}
}

func poll(f lldb.Filer) {
	select {
	case <-secs:
		if err := f.EndUpdate(); err != nil {
			log.Fatal(err)
		}

		if err := f.BeginUpdate(); err != nil {
			log.Fatal(err)
		}
	default:
	}
}

func x(base string, fltKind int) {
	for zip := 0; zip <= 1; zip++ {
		handles := []int64{}
		name := base
		if zip != 0 {
			name += "-compressed"
		}
		name += ".db"

		f, err := ioutil.TempFile("", name)
		if err != nil {
			log.Fatal(err)
		}

		fn := f.Name()
		wal, err := ioutil.TempFile("", "")
		if err != nil {
			log.Fatal(err)
		}

		var filer lldb.Filer = lldb.NewSimpleFileFiler(f)
		filer, err = lldb.NewACIDFiler(filer, wal)
		if err = filer.BeginUpdate(); err != nil {
			log.Fatal(err)
		}

		a, err := lldb.NewFLTAllocator(filer, fltKind)
		if err != nil {
			log.Fatal(err)
		}

		if err = filer.EndUpdate(); err != nil {
			log.Fatal(err)
		}

		a.Compress = zip != 0

		runtime.GC()
		t0 := time.Now()
		rng := rand.New(rand.NewSource(42))
		if err = filer.BeginUpdate(); err != nil {
			log.Fatal(err)
		}

		for len(handles) < *maxHandles {
			for nalloc := len(handles)/2 + 1; nalloc != 0; nalloc-- {
				ln := rng.Intn(1<<16 + 1)
				h, err := a.Alloc(data[:ln])
				if err != nil {
					log.Fatal(err)
				}

				poll(filer)
				handles = append(handles, h)
			}

			for nrealloc := len(handles) / 2; nrealloc != 0; nrealloc-- {
				h := handles[rng.Intn(len(handles))]
				ln := rng.Intn(1<<16 + 1)
				err := a.Realloc(h, data[:ln])
				if err != nil {
					log.Fatal(err)
				}

				poll(filer)
			}

			for ndel := len(handles) / 4; ndel != 0; ndel-- {
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

				poll(filer)
			}

			for nalloc := len(handles) + 1; nalloc != 0; nalloc-- {
				ln := rng.Intn(1<<16 + 1)
				h, err := a.Alloc(data[:ln])
				if err != nil {
					log.Fatal(err)
				}

				poll(filer)
				handles = append(handles, h)
			}

		}

		for len(handles) > *maxHandles/4 {
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

			poll(filer)
		}

		for len(handles) < *maxHandles {
			ln := rng.Intn(1<<16 + 1)
			h, err := a.Alloc(data[:ln])
			if err != nil {
				log.Fatal(err)
			}

			poll(filer)
			handles = append(handles, h)
		}

		if err = filer.EndUpdate(); err != nil {
			log.Fatal(err)
		}

		sz, err := filer.Size()
		if err != nil {
			log.Fatal(err)
		}

		if err = filer.Close(); err != nil {
			log.Fatal(err)
		}

		d := time.Since(t0)
		fmt.Printf("compress %d, typ %d, %d handles, sz %10d time %s\n", zip, fltKind, len(handles), sz, d)

		os.Remove(fn)
		fn = wal.Name()
		wal.Close()
		os.Remove(fn)
	}
}

func main() {
	flag.Parse()
	log.SetFlags(log.Lshortfile)
	x("0", lldb.FLTPowersOf2)
	x("1", lldb.FLTFib)
	x("2", lldb.FLTFull)
}

/*
jnml@fsc-r630:~/src/github.com/cznic/exp/lldb/tmp$ ./tmp -n 1
compress 0, typ 0, 1 handles, sz      99984 time 109.38506ms
compress 1, typ 0, 1 handles, sz      43088 time 101.849151ms
compress 0, typ 1, 1 handles, sz     100000 time 100.976618ms
compress 1, typ 1, 1 handles, sz      43104 time 100.255218ms
compress 0, typ 2, 1 handles, sz     128656 time 126.548612ms
compress 1, typ 2, 1 handles, sz      71760 time 193.40745ms
jnml@fsc-r630:~/src/github.com/cznic/exp/lldb/tmp$ ./tmp -n 2
compress 0, typ 0, 2 handles, sz      99984 time 127.042255ms
compress 1, typ 0, 2 handles, sz      43088 time 101.012079ms
compress 0, typ 1, 2 handles, sz     100000 time 118.192175ms
compress 1, typ 1, 2 handles, sz      43104 time 84.078136ms
compress 0, typ 2, 2 handles, sz     128656 time 133.180671ms
compress 1, typ 2, 2 handles, sz      71760 time 201.724885ms
jnml@fsc-r630:~/src/github.com/cznic/exp/lldb/tmp$ ./tmp -n 4
compress 0, typ 0, 4 handles, sz     143776 time 134.306097ms
compress 1, typ 0, 4 handles, sz      63648 time 100.879818ms
compress 0, typ 1, 4 handles, sz     143792 time 118.576155ms
compress 1, typ 1, 4 handles, sz      63664 time 117.371864ms
compress 0, typ 2, 4 handles, sz     172448 time 133.676576ms
compress 1, typ 2, 4 handles, sz      92320 time 243.815203ms
jnml@fsc-r630:~/src/github.com/cznic/exp/lldb/tmp$ ./tmp -n 8
compress 0, typ 0, 8 handles, sz     342128 time 201.254517ms
compress 1, typ 0, 8 handles, sz     145984 time 226.740803ms
compress 0, typ 1, 8 handles, sz     342144 time 200.9115ms
compress 1, typ 1, 8 handles, sz     146000 time 253.031241ms
compress 0, typ 2, 8 handles, sz     370800 time 241.989432ms
compress 1, typ 2, 8 handles, sz     174656 time 184.000031ms
jnml@fsc-r630:~/src/github.com/cznic/exp/lldb/tmp$ ./tmp -n 16
compress 0, typ 0, 16 handles, sz     858320 time 477.767937ms
compress 1, typ 0, 16 handles, sz     385744 time 250.671992ms
compress 0, typ 1, 16 handles, sz     836880 time 569.409612ms
compress 1, typ 1, 16 handles, sz     378224 time 225.677278ms
compress 0, typ 2, 16 handles, sz     874432 time 610.780282ms
compress 1, typ 2, 16 handles, sz     382864 time 300.086492ms
jnml@fsc-r630:~/src/github.com/cznic/exp/lldb/tmp$ ./tmp -n 32
compress 0, typ 0, 32 handles, sz    1638848 time 994.35599ms
compress 1, typ 0, 32 handles, sz     723312 time 577.464239ms
compress 0, typ 1, 32 handles, sz    1442320 time 868.734596ms
compress 1, typ 1, 32 handles, sz     721296 time 584.617922ms
compress 0, typ 2, 32 handles, sz    1472672 time 1.044290056s
compress 1, typ 2, 32 handles, sz     627616 time 618.680488ms
jnml@fsc-r630:~/src/github.com/cznic/exp/lldb/tmp$ ./tmp -n 64
compress 0, typ 0, 64 handles, sz    5223728 time 3.925412983s
compress 1, typ 0, 64 handles, sz    2213840 time 1.60250831s
compress 0, typ 1, 64 handles, sz    5052752 time 3.523042219s
compress 1, typ 1, 64 handles, sz    2117616 time 1.677474714s
compress 0, typ 2, 64 handles, sz    4668656 time 3.608432088s
compress 1, typ 2, 64 handles, sz    1963248 time 1.832207259s
jnml@fsc-r630:~/src/github.com/cznic/exp/lldb/tmp$ ./tmp -n 128
compress 0, typ 0, 128 handles, sz   11222592 time 10.814641671s
compress 1, typ 0, 128 handles, sz    4721232 time 4.013207002s
compress 0, typ 1, 128 handles, sz   10771536 time 9.204896066s
compress 1, typ 1, 128 handles, sz    4598448 time 4.428215173s
compress 0, typ 2, 128 handles, sz    9734256 time 8.614139387s
compress 1, typ 2, 128 handles, sz    4161200 time 4.025251766s
jnml@fsc-r630:~/src/github.com/cznic/exp/lldb/tmp$ ./tmp -n 256
compress 0, typ 0, 256 handles, sz   11222592 time 10.941451896s
compress 1, typ 0, 256 handles, sz    4721232 time 4.103562732s
compress 0, typ 1, 256 handles, sz   10771536 time 10.459252293s
compress 1, typ 1, 256 handles, sz    4598448 time 3.643893223s
compress 0, typ 2, 256 handles, sz    9734256 time 8.521265609s
compress 1, typ 2, 256 handles, sz    4161200 time 4.074571583s
jnml@fsc-r630:~/src/github.com/cznic/exp/lldb/tmp$ ./tmp -n 512
compress 0, typ 0, 512 handles, sz   24783600 time 31.052655353s
compress 1, typ 0, 512 handles, sz   10656592 time 10.090458051s
compress 0, typ 1, 512 handles, sz   23773568 time 22.899995597s
compress 1, typ 1, 512 handles, sz   10331648 time 9.697341713s
compress 0, typ 2, 512 handles, sz   21578512 time 21.148669999s
compress 1, typ 2, 512 handles, sz    9230432 time 9.933081226s
jnml@fsc-r630:~/src/github.com/cznic/exp/lldb/tmp$ ./tmp -n 1024
compress 0, typ 0, 1024 handles, sz   57961104 time 1m25.225943121s
compress 1, typ 0, 1024 handles, sz   24192960 time 26.487872689s
compress 0, typ 1, 1024 handles, sz   54910384 time 1m9.936689842s
compress 1, typ 1, 1024 handles, sz   23302912 time 28.887664361s
compress 0, typ 2, 1024 handles, sz   49072256 time 57.268743438s
compress 1, typ 2, 1024 handles, sz   20836160 time 24.912281247s
jnml@fsc-r630:~/src/github.com/cznic/exp/lldb/tmp$ ./tmp -n 2048
compress 0, typ 0, 2048 handles, sz  127151440 time 3m36.265864945s
compress 1, typ 0, 2048 handles, sz   53760080 time 1m21.694238625s
compress 0, typ 1, 2048 handles, sz  120947536 time 2m39.292914936s
compress 1, typ 1, 2048 handles, sz   51786320 time 1m9.697830869s
compress 0, typ 2, 2048 handles, sz  109190272 time 2m18.51168222s
compress 1, typ 2, 2048 handles, sz   46482576 time 1m27.06584586s
jnml@fsc-r630:~/src/github.com/cznic/exp/lldb/tmp$ ./tmp -n 4096
compress 0, typ 0, 4096 handles, sz  287942464 time 9m38.478650406s
compress 1, typ 0, 4096 handles, sz  120799952 time 2m51.564051436s
compress 0, typ 1, 4096 handles, sz  272035440 time 9m7.485142493s
compress 1, typ 1, 4096 handles, sz  116202768 time 2m37.864008052s
compress 0, typ 2, 4096 handles, sz  244662208 time 5m57.966132978s
compress 1, typ 2, 4096 handles, sz  104263184 time 3m28.845209673s

*/
