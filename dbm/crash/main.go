// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Dbm crash test.
package main

import (
	"flag"
	"log"
	"log/syslog"
	"math/rand"
	"os"
	"time"

	"github.com/cznic/exp/dbm"
	"github.com/cznic/exp/lldb"
)

var (
	oFile = flag.String("f", "test.db", "crash test db name")
	opts  = &dbm.Options{ACID: dbm.ACIDFull, GracePeriod: time.Second, WAL: "wal"}
	slg   *log.Logger
)

func dummie() {
	log.SetFlags(log.Flags() | log.Lshortfile)
	db, err := dbm.Create(*oFile, opts)
	if err != nil {
		log.Fatal(err)
	}

	a, err := db.Array("test")
	if err != nil {
		log.Fatal(err)
	}

	c := time.After(time.Minute)
	for i := 0; ; i++ {
		select {
		case <-c:
			log.Fatal("timeout")
		default:
		}

		k, v := i, i^0x55555555
		if err = a.Set(v, k); err != nil {
			log.Fatal(err)
		}
	}
}

func main() {
	slg, err := syslog.NewLogger(syslog.LOG_USER|syslog.LOG_DEBUG, log.Lshortfile)
	if err != nil {
		log.Fatal(err)
	}

	oTest := flag.Bool("test", false, "run as a crash test dummie")
	flag.Parse()
	if *oTest {
		dummie() // does/should not return
		panic("unreachable")
	}

	slg.Print("Master started")
	ncrash := 1
	for {
		os.Remove(*oFile)
		lifespan := time.Duration(10+rand.Intn(10)) * time.Second
		proc, err := os.StartProcess(
			os.Args[0],
			[]string{os.Args[0], "-test", "-f", *oFile},
			&os.ProcAttr{Files: []*os.File{os.Stdin, os.Stdout, os.Stderr}},
		)
		if err != nil {
			slg.Fatal(err)
		}

		<-time.After(lifespan)
		if err = proc.Kill(); err != nil {
			slg.Fatal(err)
		}

		<-time.After(time.Second)

		fi, err := os.Stat(opts.WAL)
		if err != nil {
			slg.Fatal(err)
		}

		wsz := fi.Size()
		t0 := time.Now()
		db, err := dbm.Open(*oFile, opts)
		t := time.Since(t0)
		if err != nil {
			slg.Fatal(err)
		}

		if err = db.Verify(lldb.NewMemFiler(), nil, nil); err != nil {
			slg.Fatal(err)
		}

		a, err := db.Array("test")
		if err != nil {
			slg.Fatal(err)
		}

		s, err := a.Slice(nil, nil)
		if err != nil {
			slg.Fatal(err)
		}

		lastKey := int64(-1)
		if err = s.Do(func(subscripts, value []interface{}) (bool, error) {
			if n := len(subscripts); n != 1 {
				slg.Fatal(n)
			}

			if n := len(value); n != 1 {
				slg.Fatal(n)
			}

			k, ok := subscripts[0].(int64)
			if !ok {
				slg.Fatal(subscripts[0])
			}

			v, ok := value[0].(int64)
			if !ok {
				slg.Fatal(value[0])
			}

			if g, e := k, lastKey+1; g != e {
				slg.Fatal(g, e)
			}

			if g, e := v, k^0x55555555; g != e {
				slg.Fatal(g, e)
			}

			lastKey++
			return true, nil

		}); err != nil {
			slg.Fatal(err)
		}

		if lastKey < 0 {
			slg.Fatal(lastKey)
		}

		if err = db.Close(); err != nil {
			slg.Fatal(err)
		}

		if err = os.Remove(opts.WAL); err != nil {
			slg.Fatal(err)
		}

		log.Printf("#%d: lived %s, WAL size %d, %d keys, opened in %s", ncrash, lifespan, wsz, lastKey, t)
		ncrash++
	}
}
