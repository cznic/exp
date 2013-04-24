// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package dbm

//DONE 2012-04-24 15:56 go test -race -cpu 4 -bench .
//DONE 2012-04-24 16:05 go test -race -cpu 4 -bench . -xact
//DONE 2012-04-24 16:25 go test -race -cpu 4 -bench . -wall

import (
	"bytes"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"path"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cznic/exp/lldb"
)

var (
	oKeep           = flag.Bool("keep", false, "do not delete testing DB (where applicable)")
	oNoZip          = flag.Bool("nozip", false, "disable compression")
	oACIDEnableWAL  = flag.Bool("wal", false, "enable WAL")
	oACIDEnableXACT = flag.Bool("xact", false, "enable structural transactions")
	oACIDGrace      = flag.Duration("grace", time.Second, "Grace period for -wal")
	oBench          = flag.Bool("tbench", false, "enable (long) TestBench* tests")
)

// Bench knobs.
const (
	fileTestChunkSize = 32e3
	fileTotalSize     = 10e6
)

func init() {
	flag.Parse()
	compress = !*oNoZip
	if *oACIDEnableXACT {
		o.ACID = ACIDTransactions
	}
	if *oACIDEnableWAL {
		o.ACID = ACIDFull
		o.GracePeriod = *oACIDGrace
	}
}

var dbg = func(s string, va ...interface{}) {
	_, fn, fl, _ := runtime.Caller(1)
	fmt.Printf("%s:%d: ", path.Base(fn), fl)
	fmt.Printf(s, va...)
	fmt.Println()
}

func TODO(s string, args ...interface{}) {
	_, f, l, _ := runtime.Caller(1)
	log.Fatalf(fmt.Sprintf("[TODO %s.%d]\n", path.Base(f), l)+s, args...)
}

func use(...interface{}) {}

func os_exit(n int) {
	os.Exit(n)
}

const dbname = "test.db"

var o = &Options{}

func preRemove(dbname string, wal bool) (err error) {
	os.Remove(dbname)
	o := Options{}
	wn := o.walName(dbname, "")
	switch wal {
	case false:
		os.Remove(wn)
	case true:
		f, err := os.Create(wn)
		if err != nil {
			return err
		}

		return f.Close()
	}
	return nil
}

func Test0(t *testing.T) {
	preRemove(dbname, false)

	db, err := Create(dbname, o)
	if err != nil {
		t.Fatal(err)
	}

	if !*oKeep {
		defer preRemove(dbname, false)
	}

	if err = db.Close(); err != nil {
		t.Error(err)
		return
	}

	if db, err = Open(dbname, o); err != nil {
		t.Error(err)
		return
	}

	if _, err = db.root(); err != nil {
		t.Error(err)
		return
	}

	if err = db.Close(); err != nil {
		t.Error(err)
		return
	}

	if db, err = Open(dbname, o); err != nil {
		t.Error(err)
		return
	}

	if _, err = db.root(); err != nil {
		t.Error(err)
		return
	}

	var tr *lldb.BTree
	if tr, err = db.acache.getTree(db, arraysPrefix, "Test0", false, aCacheSize); err != nil {
		t.Error(err)
		return
	}

	if tr != nil {
		t.Error(tr)
		return
	}

	if err = db.filer.BeginUpdate(); err != nil {
		t.Error(tr)
		return
	}

	if tr, err = db.acache.getTree(db, arraysPrefix, "Test0", true, aCacheSize); err != nil {
		t.Error(err)
		return
	}

	if err = db.filer.EndUpdate(); err != nil {
		t.Error(tr)
		return
	}

	if tr == nil {
		t.Error(tr)
		return
	}

	if err = db.Close(); err != nil {
		t.Error(err)
		return
	}

	if db, err = Open(dbname, o); err != nil {
		t.Error(err)
		return
	}

	if err = db.filer.BeginUpdate(); err != nil {
		t.Error(tr)
		return
	}

	if tr, err = db.acache.getTree(db, arraysPrefix, "Test0", true, aCacheSize); err != nil {
		t.Error(err)
		return
	}

	if err = db.filer.EndUpdate(); err != nil {
		t.Error(tr)
		return
	}

	if tr == nil {
		t.Error(tr)
		return
	}

	if err = db.Close(); err != nil {
		t.Error(err)
		return
	}
}

func TestSet0(t *testing.T) {
	N := 4000
	if *oACIDEnableWAL {
		N = 50
	}

	preRemove(dbname, false)
	rng := rand.New(rand.NewSource(42))
	ref := map[int]int{}

	db, err := Create(dbname, o)
	if err != nil {
		t.Fatal(err)
	}

	if !*oKeep {
		defer preRemove(dbname, false)
	}

	for i := 0; i < N; i++ {
		k, v := rng.Int(), rng.Int()
		ref[k] = v
		if err := db.Set(v, "TestSet0", k); err != nil {
			t.Fatal(err)
		}
	}

	if err = db.Close(); err != nil {
		t.Error(err)
		return
	}

	if db, err = Open(dbname, o); err != nil {
		t.Error(err)
		return
	}

	for k, v := range ref {
		val, err := db.Get("TestSet0", k)
		if err != nil {
			t.Error(err)
			return
		}

		switch x := val.(type) {
		case int64:
			if g, e := x, int64(v); g != e {
				t.Error(g, e)
				return
			}
		default:
			t.Errorf("%T != int64", x)
		}
	}

	if err = db.Close(); err != nil {
		t.Error(err)
		return
	}
}

func TestDocEx(t *testing.T) {
	preRemove(dbname, false)

	db, err := Create(dbname, o)
	if err != nil {
		t.Fatal(err)
	}

	if !*oKeep {
		defer preRemove(dbname, false)
	}

	var g, e interface{}

	dump := func(name string, clear bool) {
		array, err := db.Array(name)
		if err != nil {
			t.Fatal(err)
		}

		s, err := dump(array.tree)
		if err != nil {
			t.Fatal(err)
		}

		t.Logf("\nDump of %q\n%s", name, s)

		if clear {
			if err = array.Clear(); err != nil {
				t.Fatal(err)
			}
		}

	}

	db.Set(3, "Stock", "slip dress", 4, "blue", "floral")

	g, _ = db.Get("Stock", "slip dress", 4, "blue", "floral") // → 3
	if e = int64(3); g != e {
		t.Error(g, e)
		return
	}

	dump("Stock", true)

	stock, _ := db.Array("Stock")
	stock.Set(3, "slip dress", 4, "blue", "floral")

	g, _ = db.Get("Stock", "slip dress", 4, "blue", "floral") // → 3
	if e = int64(3); g != e {
		t.Error(g, e)
		return
	}

	g, _ = stock.Get("slip dress", 4, "blue", "floral") // → 3
	if e = int64(3); g != e {
		t.Error(g, e)
		return
	}

	dump("Stock", true)

	blueDress, _ := db.Array("Stock", "slip dress", 4, "blue")
	blueDress.Set(3, "floral")

	g, _ = db.Get("Stock", "slip dress", 4, "blue", "floral") // → 3
	if e = int64(3); g != e {
		t.Error(g, e)
		return
	}

	g, _ = blueDress.Get("floral") // → 3
	if e = int64(3); g != e {
		t.Error(g, e)
		return
	}

	dump("Stock", true)

	parts := []struct{ num, qty, price int }{
		{100001, 2, 300},
		{100004, 5, 600},
	}
	invoiceNum := 314159
	customer := "Google"
	when := time.Now().UnixNano()

	invoice, _ := db.Array("Invoice")
	invoice.Set(when, invoiceNum, "Date")
	invoice.Set(customer, invoiceNum, "Customer")
	invoice.Set(len(parts), invoiceNum, "Items") // # of Items in the invoice
	for i, part := range parts {
		invoice.Set(part.num, invoiceNum, "Items", i, "Part")
		invoice.Set(part.qty, invoiceNum, "Items", i, "Quantity")
		invoice.Set(part.price, invoiceNum, "Items", i, "Price")
	}

	g, _ = db.Get("Invoice", invoiceNum, "Customer") // → customer
	if e = customer; g != e {
		t.Errorf("|%#v| |%#v|", g, e)
		return
	}

	g, _ = db.Get("Invoice", invoiceNum, "Date") // → time.Then().UnixName
	if e = when; g != e {
		t.Errorf("|%#v| |%#v|", g, e)
		return
	}

	g, _ = invoice.Get(invoiceNum, "Customer") // → customer
	if e = customer; g != e {
		t.Errorf("|%#v| |%#v|", g, e)
		return
	}

	g, _ = invoice.Get(invoiceNum, "Date") // → time.Then().UnixName
	if e = when; g != e {
		t.Errorf("|%#v| |%#v|", g, e)
		return
	}

	g, _ = invoice.Get(invoiceNum, "Items") // → len(parts)
	if e = int64(len(parts)); g != e {
		t.Errorf("|%#v| |%#v|", g, e)
		return
	}

	for i, part := range parts {
		g, _ = invoice.Get(invoiceNum, "Items", i, "Part") // → part[0].part
		if e = int64(part.num); g != e {
			t.Errorf("|%#v| |%#v|", g, e)
			return
		}

		g, _ = invoice.Get(invoiceNum, "Items", i, "Quantity") // → part[0].qty
		if e = int64(part.qty); g != e {
			t.Errorf("|%#v| |%#v|", g, e)
			return
		}

		g, _ = invoice.Get(invoiceNum, "Items", i, "Price") // → part[0].price
		if e = int64(part.price); g != e {
			t.Errorf("|%#v| |%#v|", g, e)
			return
		}
	}

	dump("Invoice", true)

	invoice, _ = db.Array("Invoice", invoiceNum)
	invoice.Set(when, "Date")
	invoice.Set(customer, "Customer")
	items, _ := invoice.Array("Items")
	items.Set(len(parts)) // # of Items in the invoice
	for i, part := range parts {
		items.Set(part.num, i, "Part")
		items.Set(part.qty, i, "Quantity")
		items.Set(part.price, i, "Price")
	}

	g, _ = db.Get("Invoice", invoiceNum, "Customer") // → customer
	if e = customer; g != e {
		t.Errorf("|%#v| |%#v|", g, e)
		return
	}

	g, _ = db.Get("Invoice", invoiceNum, "Date") // → time.Then().UnixName
	if e = when; g != e {
		t.Errorf("|%#v| |%#v|", g, e)
		return
	}

	g, _ = invoice.Get("Customer") // → customer
	if e = customer; g != e {
		t.Errorf("|%#v| |%#v|", g, e)
		return
	}

	g, _ = invoice.Get("Date") // → time.Then().UnixName
	if e = when; g != e {
		t.Errorf("|%#v| |%#v|", g, e)
		return
	}

	g, _ = items.Get() // → len(parts)
	if e = int64(len(parts)); g != e {
		t.Errorf("|%#v| |%#v|", g, e)
		return
	}

	for i, part := range parts {
		g, _ = items.Get(i, "Part") // → parts[i].part
		if e = int64(part.num); g != e {
			t.Errorf("|%#v| |%#v|", g, e)
			return
		}

		g, _ = items.Get(i, "Quantity") // → part[0].qty
		if e = int64(part.qty); g != e {
			t.Errorf("|%#v| |%#v|", g, e)
			return
		}

		g, _ = items.Get(i, "Price") // → part[0].price
		if e = int64(part.price); g != e {
			t.Errorf("|%#v| |%#v|", g, e)
			return
		}
	}

	dump("Invoice", true)

	invoice, _ = db.Array("Invoice", invoiceNum)
	invoice.Set(when, "Date")
	invoice.Set(customer, "Customer")
	items, _ = invoice.Array("Items")
	items.Set(len(parts)) // # of Items in the invoice
	for i, part := range parts {
		items.Set([]interface{}{part.num, part.qty, part.price}, i)
	}

	dump("Invoice", false)

	g, _ = db.Get("Invoice", invoiceNum, "Customer") // → customer
	if e = customer; g != e {
		t.Errorf("|%#v| |%#v|", g, e)
		return
	}

	g, _ = db.Get("Invoice", invoiceNum, "Date") // → time.Then().UnixName
	if e = when; g != e {
		t.Errorf("|%#v| |%#v|", g, e)
		return
	}

	g, _ = invoice.Get("Customer") // → customer
	if e = customer; g != e {
		t.Errorf("|%#v| |%#v|", g, e)
		return
	}

	g, _ = invoice.Get("Date") // → time.Then().UnixName
	if e = when; g != e {
		t.Errorf("|%#v| |%#v|", g, e)
		return
	}

	g, _ = items.Get() // → len(parts)
	if e = int64(len(parts)); g != e {
		t.Errorf("|%#v| |%#v|", g, e)
		return
	}

	for i, part := range parts {
		g, _ = items.Get(i) // → []interface{parts[i].num, parts[0].qty, parts[i].price}
		gg, ok := g.([]interface{})
		if !ok || len(gg) != 3 {
			t.Error(g)
			return
		}

		if g, e = gg[0], int64(part.num); g != e {
			t.Errorf("|%#v| |%#v|", g, e)
			return
		}

		if g, e = gg[1], int64(part.qty); g != e {
			t.Errorf("|%#v| |%#v|", g, e)
			return
		}

		if g, e = gg[2], int64(part.price); g != e {
			t.Errorf("|%#v| |%#v|", g, e)
			return
		}
	}

	dump("Invoice", true)

	if err = db.Close(); err != nil {
		t.Error(err)
		return
	}
}

func dump(t *lldb.BTree) (r string, err error) {
	var b bytes.Buffer
	if err = t.Dump(&b); err != nil {
		if err = noEof(err); err != nil {
			return "", err
		}
	}

	return fmt.Sprintf("IsMem: %t\n%s", t.IsMem(), b.String()), nil
}

func strings2D(s string) (r [][]interface{}) {
	for _, v := range strings.Split(s, "|") {
		r = append(r, strings1D(v))
	}
	return
}

func strings1D(s string) (r []interface{}) {
	for _, v := range strings.Split(s, ",") {
		if v != "" {
			r = append(r, v)
		}
	}
	return
}

func TestSlice0(t *testing.T) {
	table := []struct{ prefix, keys, from, to, exp string }{
		// Slice.from == nil && Slice.to == nil
		{"", "", "", "", ""},
		{"", "a", "", "", "a"},
		{"", "a|b", "", "", "a|b"},
		{"", "d|c", "", "", "c|d"},
		{"", "a|a,b|a,c|b", "", "", "a|a,b|a,c|b"},

		// Slice.from == nil && Slice.to != nil
		{"", "", "", "a", ""},
		{"", "m", "", "a", ""},
		{"", "m", "", "m", "m"},
		{"", "m", "", "z", "m"},
		{"", "k|p", "", "a", ""},
		{"", "k|p", "", "j", ""},
		{"", "k|p", "", "k", "k"},
		{"", "k|p", "", "l", "k"},
		{"", "k|p", "", "o", "k"},
		{"", "k|p", "", "p", "k|p"},
		{"", "k|p", "", "q", "k|p"},
		{"", "k|m|o", "", "j", ""},
		{"", "k|m|o", "", "k", "k"},
		{"", "k|m|o", "", "l", "k"},
		{"", "k|m|o", "", "m", "k|m"},
		{"", "k|m|o", "", "n", "k|m"},
		{"", "k|m|o", "", "o", "k|m|o"},
		{"", "k|m|o", "", "p", "k|m|o"},
		{"", "k|k,m|k,o|p", "", "j", ""},
		{"", "k|k,m|k,o|p", "", "k", "k"},
		{"", "k|k,m|k,o|p", "", "k,l", "k"},
		{"", "k|k,m|k,o|p", "", "k,m", "k|k,m"},
		{"", "k|k,m|k,o|p", "", "k,n", "k|k,m"},
		{"", "k|k,m|k,o|p", "", "k,o", "k|k,m|k,o"},
		{"", "k|k,m|k,o|p", "", "k,z", "k|k,m|k,o"},
		{"", "k|k,m|k,o|p", "", "o", "k|k,m|k,o"},
		{"", "k|k,m|k,o|p", "", "p", "k|k,m|k,o|p"},
		{"", "k|k,m|k,o|p", "", "q", "k|k,m|k,o|p"},

		// Slice.from != nil && Slice.to == nil
		{"", "", "m", "", ""},
		{"", "a", "0", "", "a"},
		{"", "a", "a", "", "a"},
		{"", "a", "b", "", ""},
		{"", "a|c", "0", "", "a|c"},
		{"", "a|c", "a", "", "a|c"},
		{"", "a|c", "b", "", "c"},
		{"", "a|c", "c", "", "c"},
		{"", "a|c", "d", "", ""},
		{"", "k|k,m|k,o|p", "j", "", "k|k,m|k,o|p"},
		{"", "k|k,m|k,o|p", "k", "", "k|k,m|k,o|p"},
		{"", "k|k,m|k,o|p", "k,l", "", "k,m|k,o|p"},
		{"", "k|k,m|k,o|p", "k,m", "", "k,m|k,o|p"},
		{"", "k|k,m|k,o|p", "k,n", "", "k,o|p"},
		{"", "k|k,m|k,o|p", "k,z", "", "p"},
		{"", "k|k,m|k,o|p", "o", "", "p"},
		{"", "k|k,m|k,o|p", "p", "", "p"},
		{"", "k|k,m|k,o|p", "q", "", ""},

		// Slice.from != nil && Slice.to != nil
		{"", "", "m", "p", ""},

		{"", "b|d|e", "a", "a", ""},
		{"", "b|d|e", "a", "b", "b"},
		{"", "b|d|e", "a", "c", "b"},
		{"", "b|d|e", "a", "d", "b|d"},
		{"", "b|d|e", "a", "e", "b|d|e"},
		{"", "b|d|e", "a", "f", "b|d|e"},

		{"", "b|d|e", "b", "a", ""},
		{"", "b|d|e", "b", "b", "b"},
		{"", "b|d|e", "b", "c", "b"},
		{"", "b|d|e", "b", "d", "b|d"},
		{"", "b|d|e", "b", "e", "b|d|e"},
		{"", "b|d|e", "b", "f", "b|d|e"},

		{"", "b|d|e", "c", "a", ""},
		{"", "b|d|e", "c", "b", ""},
		{"", "b|d|e", "c", "c", ""},
		{"", "b|d|e", "c", "d", "d"},
		{"", "b|d|e", "c", "e", "d|e"},
		{"", "b|d|e", "c", "f", "d|e"},

		{"", "b|d|e", "d", "a", ""},
		{"", "b|d|e", "d", "b", ""},
		{"", "b|d|e", "d", "c", ""},
		{"", "b|d|e", "d", "d", "d"},
		{"", "b|d|e", "d", "e", "d|e"},
		{"", "b|d|e", "d", "f", "d|e"},

		{"", "b|d|e", "d", "a", ""},
		{"", "b|d|e", "d", "b", ""},
		{"", "b|d|e", "d", "c", ""},
		{"", "b|d|e", "d", "d", "d"},
		{"", "b|d|e", "d", "e", "d|e"},
		{"", "b|d|e", "d", "f", "d|e"},

		{"", "b|d|e", "e", "a", ""},
		{"", "b|d|e", "e", "b", ""},
		{"", "b|d|e", "e", "c", ""},
		{"", "b|d|e", "e", "d", ""},
		{"", "b|d|e", "e", "e", "e"},
		{"", "b|d|e", "e", "f", "e"},

		{"", "b|d|e", "f", "a", ""},
		{"", "b|d|e", "f", "b", ""},
		{"", "b|d|e", "f", "c", ""},
		{"", "b|d|e", "f", "d", ""},
		{"", "b|d|e", "f", "e", ""},
		{"", "b|d|e", "f", "f", ""},

		// more levels
		{"", "b|d,f|h,j|l", "a", "a", ""},
		{"", "b|d,f|h,j|l", "a", "z", "b|d,f|h,j|l"},
		{"", "b|d,f|h,j|l", "c", "k", "d,f|h,j"},

		// w/ prefix
		{"B", "", "M", "P", ""},
		{"B", "", "A", "Z", ""},

		{"B", "D|E", "", "", "D|E"},
		{"B", "D|E", "", "A", ""},
		{"B", "D|E", "", "B", ""},
		{"B", "D|E", "", "C", ""},
		{"B", "D|E", "", "D", "D"},
		{"B", "D|E", "", "E", "D|E"},
		{"B", "D|E", "", "F", "D|E"},

		{"B", "D|E", "A", "", "D|E"},
		{"B", "D|E", "A", "A", ""},
		{"B", "D|E", "A", "B", ""},
		{"B", "D|E", "A", "C", ""},
		{"B", "D|E", "A", "D", "D"},
		{"B", "D|E", "A", "E", "D|E"},
		{"B", "D|E", "A", "F", "D|E"},

		{"B", "D|E", "B", "", "D|E"},
		{"B", "D|E", "B", "A", ""},
		{"B", "D|E", "B", "B", ""},
		{"B", "D|E", "B", "C", ""},
		{"B", "D|E", "B", "D", "D"},
		{"B", "D|E", "B", "E", "D|E"},
		{"B", "D|E", "B", "F", "D|E"},

		{"B", "D|E", "C", "", "D|E"},
		{"B", "D|E", "C", "A", ""},
		{"B", "D|E", "C", "B", ""},
		{"B", "D|E", "C", "C", ""},
		{"B", "D|E", "C", "D", "D"},
		{"B", "D|E", "C", "E", "D|E"},
		{"B", "D|E", "C", "F", "D|E"},

		{"B", "D|E", "D", "", "D|E"},
		{"B", "D|E", "D", "A", ""},
		{"B", "D|E", "D", "B", ""},
		{"B", "D|E", "D", "C", ""},
		{"B", "D|E", "D", "D", "D"},
		{"B", "D|E", "D", "E", "D|E"},
		{"B", "D|E", "D", "F", "D|E"},

		{"B", "D|E", "E", "", "E"},
		{"B", "D|E", "E", "A", ""},
		{"B", "D|E", "E", "B", ""},
		{"B", "D|E", "E", "C", ""},
		{"B", "D|E", "E", "D", ""},
		{"B", "D|E", "E", "E", "E"},
		{"B", "D|E", "E", "F", "E"},

		{"B", "D|E", "F", "", ""},
		{"B", "D|E", "F", "A", ""},
		{"B", "D|E", "F", "B", ""},
		{"B", "D|E", "F", "C", ""},
		{"B", "D|E", "F", "D", ""},
		{"B", "D|E", "F", "E", ""},
		{"B", "D|E", "F", "F", ""},
	}

	for i, test := range table {
		prefix := strings1D(test.prefix)
		keys := strings2D(test.keys)
		from := strings1D(test.from)
		to := strings1D(test.to)
		exp := test.exp

		a0, _ := MemArray()

		a, err := a0.Array(prefix...)
		if err != nil {
			t.Fatal(err)
		}

		if test.prefix != "" {
			a0.Set(-1, "@")
			a0.Set(-1, "Z")
		}
		for i, v := range keys {
			if err = a.Set(i, v...); err != nil {
				t.Error(err)
				return
			}
		}
		d, err := dump(a.tree)
		if err != nil {
			t.Fatal(err)
		}

		t.Logf("%q, %q, dump:\n%s", test.prefix, test.keys, d)

		s, err := a.Slice(from, to)
		if err != nil {
			t.Fatal(err)
		}

		var ga []string

		if err := s.Do(func(k, v []interface{}) (more bool, err error) {
			a := []string{}
			for _, v := range k {
				a = append(a, v.(string))
			}
			ga = append(ga, strings.Join(a, ","))
			return true, nil
		}); err != nil {
			if err != io.EOF {
				t.Fatal(err)
			}
		}

		g := strings.Join(ga, "|")
		t.Logf("%q", g)
		if g != exp {
			t.Fatalf("%d\n%s\n%s", i, g, exp)
		}
	}
}

func TestSlice1(t *testing.T) {
	f := func(s, val []interface{}) (k, v string) {
		if len(s) != 1 || len(val) != 1 {
			t.Fatal(s, val)
		}

		k, ok := s[0].(string)
		if !ok {
			t.Fatal(s)
		}

		v, ok = val[0].(string)
		if !ok {
			t.Fatal(val)
		}

		return
	}

	a0, err := MemArray()
	if err != nil {
		t.Fatal(err)
	}

	a, err := a0.Array("b")
	if err != nil {
		t.Fatal(err)
	}

	a.Set("1", "d")
	a.Set("2", "f")

	d, err := dump(a0.tree)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("\n%s", d)

	s, err := a.Slice(nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	state := 0
	err = s.Do(func(s, val []interface{}) (bool, error) {
		k, v := f(s, val)
		switch state {
		case 0:
			if k != "d" || v != "1" {
				t.Error(s, val)
				return false, nil
			}

			a.Set("3", k)
			state++
		case 1:
			if k != "f" || v != "2" {
				t.Error(s, val)
				return false, nil
			}

			a.Set("4", k)
			state++
		default:
			t.Error(state)
			return false, nil
		}
		return true, nil
	})

	if err != nil {
		t.Fatal(err)
	}

	if g, e := state, 2; g != e {
		t.Fatal(state)
	}

	d, err = dump(a0.tree)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("\n%s", d)

	v3, err := a0.Get("b", "d")
	if err != nil {
		t.Fatal(err)
	}

	if g, e := v3, interface{}("3"); g != e {
		t.Fatal(g, e)
	}

	v4, err := a0.Get("b", "f")
	if err != nil {
		t.Fatal(err)
	}

	if g, e := v4, interface{}("4"); g != e {
		t.Fatal(g, e)
	}
}

func TestClear(t *testing.T) {
	table := []struct{ prefix, keys, subscripts, exp string }{
		{"", "", "", ""},

		{"", "b", "", ""},
		{"", "b", "a", "b"},
		{"", "b", "b", ""},
		{"", "b", "c", "b"},

		{"", "b|d|f", "", ""},
		{"", "b|d|f", "a", "b|d|f"},
		{"", "b|d|f", "b", "d|f"},
		{"", "b|d|f", "c", "b|d|f"},
		{"", "b|d|f", "d", "b|f"},
		{"", "b|d|f", "e", "b|d|f"},
		{"", "b|d|f", "f", "b|d"},
		{"", "b|d|f", "g", "b|d|f"},

		{"", "b,d|b,f|d,f|d,h|f,h|f,j", "", ""},
		{"", "b,d|b,f|d,f|d,h|f,h|f,j", "a", "b,d|b,f|d,f|d,h|f,h|f,j"},
		{"", "b,d|b,f|d,f|d,h|f,h|f,j", "b", "d,f|d,h|f,h|f,j"},
		{"", "b,d|b,f|d,f|d,h|f,h|f,j", "b,c", "b,d|b,f|d,f|d,h|f,h|f,j"},
		{"", "b,d|b,f|d,f|d,h|f,h|f,j", "b,d", "b,f|d,f|d,h|f,h|f,j"},
		{"", "b,d|b,f|d,f|d,h|f,h|f,j", "b,e", "b,d|b,f|d,f|d,h|f,h|f,j"},
		{"", "b,d|b,f|d,f|d,h|f,h|f,j", "b,f", "b,d|d,f|d,h|f,h|f,j"},
		{"", "b,d|b,f|d,f|d,h|f,h|f,j", "c", "b,d|b,f|d,f|d,h|f,h|f,j"},
		{"", "b,d|b,f|d,f|d,h|f,h|f,j", "d", "b,d|b,f|f,h|f,j"},
		{"", "b,d|b,f|d,f|d,h|f,h|f,j", "d,e", "b,d|b,f|d,f|d,h|f,h|f,j"},
		{"", "b,d|b,f|d,f|d,h|f,h|f,j", "d,f", "b,d|b,f|d,h|f,h|f,j"},
		{"", "b,d|b,f|d,f|d,h|f,h|f,j", "d,g", "b,d|b,f|d,f|d,h|f,h|f,j"},
		{"", "b,d|b,f|d,f|d,h|f,h|f,j", "d,h", "b,d|b,f|d,f|f,h|f,j"},
		{"", "b,d|b,f|d,f|d,h|f,h|f,j", "d,i", "b,d|b,f|d,f|d,h|f,h|f,j"},
		{"", "b,d|b,f|d,f|d,h|f,h|f,j", "e", "b,d|b,f|d,f|d,h|f,h|f,j"},
		{"", "b,d|b,f|d,f|d,h|f,h|f,j", "f", "b,d|b,f|d,f|d,h"},
		{"", "b,d|b,f|d,f|d,h|f,h|f,j", "f,g", "b,d|b,f|d,f|d,h|f,h|f,j"},
		{"", "b,d|b,f|d,f|d,h|f,h|f,j", "f,h", "b,d|b,f|d,f|d,h|f,j"},
		{"", "b,d|b,f|d,f|d,h|f,h|f,j", "f,i", "b,d|b,f|d,f|d,h|f,h|f,j"},
		{"", "b,d|b,f|d,f|d,h|f,h|f,j", "f,j", "b,d|b,f|d,f|d,h|f,h"},
		{"", "b,d|b,f|d,f|d,h|f,h|f,j", "f,k", "b,d|b,f|d,f|d,h|f,h|f,j"},
		{"", "b,d|b,f|d,f|d,h|f,h|f,j", "g", "b,d|b,f|d,f|d,h|f,h|f,j"},

		{"b", "", "", ""},
		{"b", "d", "c", "b,d"},
		{"b", "d", "d", ""},
		{"b", "d", "e", "b,d"},

		{"b", "d|f", "", ""},
		{"b", "d|f", "c", "b,d|b,f"},
		{"b", "d|f", "d", "b,f"},
		{"b", "d|f", "e", "b,d|b,f"},
		{"b", "d|f", "f", "b,d"},
		{"b", "d|f", "g", "b,d|b,f"},
	}

	for i, test := range table {
		prefix := strings1D(test.prefix)
		keys := strings2D(test.keys)
		subscripts := strings1D(test.subscripts)
		exp := test.exp

		a0, err := MemArray()
		if err != nil {
			t.Fatal(err)
		}

		a, err := a0.Array(prefix...)
		if err != nil {
			t.Fatal(err)
		}

		for i, v := range keys {
			a.Set(i, v...)
		}
		d, err := dump(a.tree)
		if err != nil {
			t.Fatal(err)
		}

		t.Logf("before Clear(%v)\n%s", subscripts, d)

		err = a.Clear(subscripts...)
		if err != nil {
			t.Fatal(err)
		}

		d, err = dump(a.tree)
		if err != nil {
			t.Fatal(err)
		}

		t.Logf(" after Clear(%v)\n%s", subscripts, d)

		s, err := a0.Slice(nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		var ga []string

		if err := s.Do(func(k, v []interface{}) (more bool, err error) {
			a := []string{}
			for _, v := range k {
				a = append(a, v.(string))
			}
			ga = append(ga, strings.Join(a, ","))
			return true, nil
		}); err != nil {
			t.Fatal(err)
		}

		g := strings.Join(ga, "|")
		t.Log(g)
		if g != exp {
			t.Fatalf("i %d\ng: %s\ne: %s", i, g, exp)
		}
	}
}

func BenchmarkClear(b *testing.B) {
	preRemove(dbname, true)

	db, err := Create(dbname, &Options{})
	if err != nil {
		b.Fatal(err)
	}

	defer db.Close()

	if !*oKeep {
		defer preRemove(dbname, false)
	}

	a, err := db.Array("test")
	if err != nil {
		b.Error(err)
		return
	}

	ref := map[int]struct{}{}
	for i := 0; i < b.N; i++ {
		ref[i] = struct{}{}
	}
	for i := range ref {
		a.Set(i, i)
	}
	if err := db.Close(); err != nil {
		b.Fatal(err)
		return
	}

	db2, err := Open(dbname, o)
	if err != nil {
		b.Error(err)
		return
	}

	defer db2.Close()

	a, err = db2.Array("test")
	if err != nil {
		b.Error(err)
		return
	}

	runtime.GC()
	b.ResetTimer()
	a.Clear()
	b.StopTimer()
}

func BenchmarkDelete(b *testing.B) {
	preRemove(dbname, true)

	db, err := Create(dbname, &Options{})
	if err != nil {
		b.Fatal(err)
	}

	defer db.Close()

	if !*oKeep {
		defer preRemove(dbname, false)
	}

	a, err := db.Array("test")
	if err != nil {
		b.Error(err)
		return
	}

	ref := map[int]struct{}{}
	for i := 0; i < b.N; i++ {
		ref[i] = struct{}{}
	}
	for i := range ref {
		a.Set(i, i)
	}
	ref = map[int]struct{}{}
	for i := 0; i < b.N; i++ {
		ref[i] = struct{}{}
	}
	if err := db.Close(); err != nil {
		b.Error(err)
		return
	}

	db2, err := Open(dbname, o)
	if err != nil {
		b.Error(err)
		return
	}

	defer db2.Close()

	a, err = db2.Array("test")
	if err != nil {
		b.Error(err)
		return
	}

	runtime.GC()
	b.ResetTimer()
	for i := range ref {
		a.Delete(i)
	}
	b.StopTimer()
}

func BenchmarkGet(b *testing.B) {
	preRemove(dbname, true)

	db, err := Create(dbname, &Options{})
	if err != nil {
		b.Fatal(err)
	}

	defer db.Close()

	if !*oKeep {
		defer preRemove(dbname, false)
	}

	a, err := db.Array("test")
	if err != nil {
		b.Error(err)
		return
	}

	ref := map[int]struct{}{}
	for i := 0; i < b.N; i++ {
		ref[i] = struct{}{}
	}
	ref = map[int]struct{}{}
	for i := 0; i < b.N; i++ {
		ref[i] = struct{}{}
	}
	if err := db.Close(); err != nil {
		b.Error(err)
		return
	}

	db2, err := Open(dbname, o)
	if err != nil {
		b.Error(err)
		return
	}

	defer db2.Close()

	a, err = db2.Array("test")
	if err != nil {
		b.Error(err)
		return
	}

	runtime.GC()
	b.ResetTimer()
	for i := range ref {
		a.Get(i)
	}
	b.StopTimer()
}

func BenchmarkSet(b *testing.B) {
	preRemove(dbname, false)

	db, err := Create(dbname, o)
	if err != nil {
		b.Fatal(err)
	}

	defer db.Close()

	if !*oKeep {
		defer preRemove(dbname, false)
	}

	a, err := db.Array("test")
	if err != nil {
		b.Error(err)
		return
	}

	ref := map[int]struct{}{}
	for i := 0; i < b.N; i++ {
		ref[i] = struct{}{}
	}
	runtime.GC()
	b.ResetTimer()
	for i := range ref {
		a.Set(i, i)
	}
	b.StopTimer()
}

func BenchmarkDo(b *testing.B) {
	preRemove(dbname, true)

	db, err := Create(dbname, &Options{})
	if err != nil {
		b.Fatal(err)
	}

	defer db.Close()

	if !*oKeep {
		defer preRemove(dbname, false)
	}

	a, err := db.Array("test")
	if err != nil {
		b.Error(err)
		return
	}

	ref := map[int]struct{}{}
	for i := 0; i < b.N; i++ {
		ref[i] = struct{}{}
	}
	for i := range ref {
		a.Set(i, i)
	}
	if err := db.Close(); err != nil {
		b.Error(err)
		return
	}

	db2, err := Open(dbname, o)
	if err != nil {
		b.Error(err)
		return
	}

	a, err = db2.Array("test")
	if err != nil {
		b.Error(err)
		return
	}

	s, err := a.Slice(nil, nil)
	if err != nil {
		b.Error(err)
		return
	}

	runtime.GC()
	b.ResetTimer()
	s.Do(func(subscripts, value []interface{}) (bool, error) {
		return true, nil
	})
	b.StopTimer()
}

func TestRemoveArray0(t *testing.T) {
	const aname = "TestRemoveArray0"

	preRemove(dbname, false)

	db, err := Create(dbname, o)
	if err != nil {
		t.Fatal(err)
	}

	if !*oKeep {
		defer preRemove(dbname, false)
	}

	err = db.Set(42, aname, 1, 2)
	if err != nil {
		t.Error(err)
		return
	}

	_, err = db.Get(aname, 1, 2)
	if err != nil {
		t.Error(err)
		return
	}

	err = db.RemoveArray(aname)
	if err != nil {
		t.Error(err)
		return
	}

	if err = db.enter(); err != nil {
		t.Error(err)
		return
	}

	tr, err := db.acache.getTree(db, arraysPrefix, aname, false, aCacheSize)
	if err != nil {
		db.leave(&err)
		t.Error(err)
		return
	}

	if err = db.leave(&err); err != nil {
		t.Error(err)
		return
	}

	if tr != nil {
		t.Error(tr)
		return
	}

	if err = db.Close(); err != nil {
		t.Error(err)
		return
	}

	if db, err = Open(dbname, o); err != nil {
		t.Error(err)
		return
	}

	for {
		<-time.After(time.Second)
		if atomic.LoadInt32(&activeVictors) == 0 {
			break
		}
	}

	if err := db.BeginUpdate(); err != nil {
		t.Error(err)
		return
	}

	err = db.alloc.Verify(
		lldb.NewMemFiler(),
		func(err error) bool {
			t.Error(err)
			return true
		},
		nil,
	)

	if err != nil {
		t.Error(err)
		return
	}

	if err := db.EndUpdate(); err != nil {
		t.Error(err)
		return
	}

	if err = db.Close(); err != nil {
		t.Error(err)
		return
	}
}

func (db *DB) dumpAll(w io.Writer, msg string) {
	fmt.Fprintln(w, msg)
	root, err := db.root()
	if err != nil {
		fmt.Fprintln(w, "\nerror: ", err)
		return
	}

	fmt.Fprintln(w, "====\nroot\n====")
	if err = root.tree.Dump(w); err != nil {
		fmt.Fprintln(w, "\nerror: ", err)
		return
	}

	s, err := root.Slice(nil, nil)
	if err != nil {
		fmt.Fprintln(w, "\nerror: ", err)
		return
	}

	if err = s.Do(func(subscripts, value []interface{}) (bool, error) {
		v, err := root.get(subscripts...)
		if err != nil {
			fmt.Fprintln(w, "\nerror: ", err)
			return false, nil
		}

		h := v.(int64)
		t, err := lldb.OpenBTree(db.alloc, collate, h)
		if err != nil {
			fmt.Fprintln(w, "\nerror: ", err)
			return false, err
		}

		fmt.Fprintf(w, "----\n%#v @ %d\n----\n", subscripts[1], h)
		if err = t.Dump(w); err != nil {
			fmt.Fprintln(w, "\nerror: ", err)
			return false, err
		}

		return true, nil
	}); err != nil {
		fmt.Fprintln(w, "\nerror: ", err)
		return
	}
}

func TestRemoveFile0(t *testing.T) {
	const fname = "TestRemoveFile0"

	preRemove(dbname, false)

	db, err := Create(dbname, o)
	if err != nil {
		t.Fatal(err)
	}

	if !*oKeep {
		defer preRemove(dbname, false)
	}

	f, err := db.File(fname)
	if err != nil {
		t.Error(err)
		return
	}

	n, err := f.WriteAt([]byte{42}, 314)
	if n != 1 || err != nil {
		t.Error(err)
		return
	}

	files, err := db.Files()
	if err != nil {
		t.Error(err)
		return
	}

	v, err := files.Get(fname)
	if v == nil || err != nil {
		t.Error(err, v)
		return
	}

	err = db.RemoveFile(fname)
	if err != nil {
		t.Error(err)
		return
	}

	v, err = files.Get(fname)
	if v != nil || err != nil {
		t.Errorf("%#v %#v", err, v)
		return
	}

	if err = db.Close(); err != nil {
		t.Error(err)
		return
	}
}

func TestRemove1(t *testing.T) {
	const (
		aname = "TestRemove1"
		N     = 100
	)

	compress = false // Test may correctly fail w/ compression.
	preRemove(dbname, false)

	db, err := Create(dbname, o)
	if err != nil {
		t.Fatal(err)
	}

	if !*oKeep {
		defer preRemove(dbname, false)
	}

	sz0, err := db.Size()
	if err != nil {
		t.Error(err)
		return
	}

	for i := 0; i < N; i++ {
		if err = db.Set(fmt.Sprintf("V%06d", i), aname, fmt.Sprintf("K%06d", i)); err != nil {
			t.Error(err)
			return
		}
	}
	sz1, err := db.Size()
	if err != nil {
		t.Error(err)
		return
	}

	err = db.RemoveArray(aname)
	if err != nil {
		t.Error(err)
		return
	}

	err = db.Close()
	if err != nil {
		t.Error(err)
		return
	}

	fi, err := os.Stat(dbname)
	if err != nil {
		t.Error(err)
		return
	}

	sz2 := fi.Size()

	if db, err = Open(dbname, o); err != nil {
		t.Error(err)
		return
	}

	for i := 0; i < N/2+1; i++ {
		runtime.Gosched()
	}
	sz3, err := db.Size()
	if err != nil {
		t.Error(err)
		return
	}

	if err = db.Close(); err != nil {
		t.Error(err)
		return
	}

	if db, err = Open(dbname, o); err != nil {
		t.Error(err)
		return
	}

	for i := 0; i < 2*N; i++ {
		runtime.Gosched()
	}
	sz4, err := db.Size()
	if err != nil {
		t.Error(err)
		return
	}

	if err = db.Close(); err != nil {
		t.Error(err)
		return
	}

	t.Log(sz0)
	t.Log(sz1)
	t.Log(sz2)
	t.Log(sz3)
	t.Log(sz4)

	// Unstable
	//	if !(sz4 < sz3) {
	//		t.Error(sz3, sz4)
	//	}
}

func enumStrKeys(a Array) (k []string, err error) {
	s, err := a.Slice(nil, nil)
	if err != nil {
		return
	}

	return k, s.Do(func(subscripts, value []interface{}) (bool, error) {
		if len(subscripts) != 1 {
			return false, (fmt.Errorf("internal error: %#v", subscripts))
		}

		v, ok := subscripts[0].(string)
		if !ok {
			return false, (fmt.Errorf("internal error: %T %#v", subscripts, subscripts))
		}

		k = append(k, v)
		return true, nil
	})
}

func TestArrays(t *testing.T) {
	preRemove(dbname, false)

	db, err := Create(dbname, o)
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	if !*oKeep {
		defer preRemove(dbname, false)
	}

	a, err := db.Arrays()
	if err != nil {
		t.Error(err)
		return
	}

	names, err := enumStrKeys(a)
	if err != nil {
		t.Error(err)
		return
	}

	if g, e := len(names), 0; g != e {
		t.Error(g, e)
		return
	}

	if err = db.Set(nil, "foo"); err != nil {
		t.Error(err)
		return
	}

	names, err = enumStrKeys(a)
	if err != nil {
		t.Error(err)
		return
	}

	if g, e := len(names), 1; g != e {
		t.Error(g, e)
		return
	}

	if g, e := names[0], "foo"; g != e {
		t.Error(g, e)
		return
	}

	if err = db.Close(); err != nil {
		t.Error(err)
		return
	}

}

func TestFiles(t *testing.T) {
	preRemove(dbname, false)

	db, err := Create(dbname, o)
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	if !*oKeep {
		defer preRemove(dbname, false)
	}

	a, err := db.Files()
	if err != nil {
		t.Error(err)
		return
	}

	names, err := enumStrKeys(a)
	if err != nil {
		t.Error(err)
		return
	}

	if g, e := len(names), 0; g != e {
		t.Error(g, e)
		return
	}

	f, err := db.File("foo")
	if err != nil {
		t.Error(err)
		return
	}

	if n, err := f.WriteAt([]byte{42}, 0); n != 1 {
		t.Error(err)
		return
	}

	names, err = enumStrKeys(a)
	if err != nil {
		t.Error(err)
		return
	}

	if g, e := len(names), 1; g != e {
		t.Error(g, e)
		return
	}

	if g, e := names[0], "foo"; g != e {
		t.Error(g, e)
		return
	}

	if err = db.Close(); err != nil {
		t.Error(err)
		return
	}

}

func TestInc0(t *testing.T) {
	preRemove(dbname, false)

	db, err := Create(dbname, o)
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	if !*oKeep {
		defer preRemove(dbname, false)
	}

	db.Set(10, "TestInc", "ten")
	db.Set(nil, "TestInc", "nil")
	db.Set("string", "TestInc", "string")

	a, err := db.Array("TestInc")
	if err != nil {
		t.Fatal(err)
	}

	d, err := dump(a.tree)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("\n%s", d)

	n, err := db.Inc(1, "TestInc", "nonexisting")
	if err != nil || n != 1 {
		t.Error(n, err)
		return
	}

	n, err = db.Inc(2, "TestInc", "ten")
	if err != nil || n != 12 {
		t.Error(n, err)
		return
	}

	n, err = db.Inc(3, "TestInc", "nil")
	if err != nil || n != 3 {
		t.Error(n, err)
		return
	}

	n, err = db.Inc(4, "TestInc", "string")
	if err != nil || n != 4 {
		t.Error(n, err)
		return
	}

	d, err = dump(a.tree)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("\n%s", d)
}

func TestInc1(t *testing.T) {
	const (
		M = 3
	)
	N := 10000
	if *oACIDEnableWAL {
		N = 20
	}

	preRemove(dbname, false)

	db, err := Create(dbname, o)
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	if !*oKeep {
		defer preRemove(dbname, false)
	}

	runtime.GOMAXPROCS(M)
	c := make(chan int64, M)
	for i := 0; i < M; i++ {
		go func() {
			sum := int64(0)
			for i := 0; i < N; i++ {
				n, err := db.Inc(1, "TestInc1", "Invoice", 314159, "Items")
				if err != nil {
					t.Error(err)
					break
				}

				sum += n
			}
			c <- sum
		}()
	}
	total := int64(0)
	for i := 0; i < M; i++ {
		select {
		case <-time.After(time.Second * 10):
			t.Error("timeouted")
			return
		case v := <-c:
			total += v
		}
	}

	nn := int64(M * N)
	if g, e := total, int64((nn*nn+nn)/2); g != e {
		t.Error(g, e)
		return
	}
}

func BenchmarkInc(b *testing.B) {
	preRemove(dbname, false)

	db, err := Create(dbname, o)
	if err != nil {
		b.Fatal(err)
	}

	defer db.Close()

	if !*oKeep {
		defer preRemove(dbname, false)
	}

	a, err := db.Array("test")
	if err != nil {
		b.Error(err)
		return
	}

	runtime.GC()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		a.Inc(279, 314)
	}
	b.StopTimer()
}

func TestFile0(t *testing.T) {
	preRemove(dbname, false)

	db, err := Create(dbname, o)
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	if !*oKeep {
		defer preRemove(dbname, false)
	}

	a, err := db.Files()
	if err != nil {
		t.Error(err)
		return
	}

	names, err := enumStrKeys(a)
	if err != nil {
		t.Error(err)
		return
	}

	if g, e := len(names), 0; g != e {
		t.Error(g, e)
		return
	}

	f, err := db.File("foo")
	if err != nil {
		t.Error(err)
		return
	}

	if _, err = f.WriteAt([]byte("ABCDEF"), 4096); err != nil {
		t.Error(err)
		return
	}

	names, err = enumStrKeys(a)
	if err != nil {
		t.Error(err)
		return
	}

	if g, e := len(names), 1; g != e {
		t.Error(g, e)
		return
	}

	if g, e := names[0], "foo"; g != e {
		t.Error(g, e)
		return
	}

	if err = db.Close(); err != nil {
		t.Error(err)
		return
	}
}

func TestFileTruncate0(t *testing.T) {
	preRemove(dbname, false)

	db, err := Create(dbname, o)
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	if !*oKeep {
		defer preRemove(dbname, false)
	}

	f, err := db.File("TestFileTruncate")
	if err != nil {
		t.Error(err)
		return
	}

	fsz := func() int64 {
		n, err := f.Size()
		if err != nil {
			t.Fatal(err)
		}
		return n
	}

	// Check Truncate works.
	sz := int64(1e6)
	if err := f.Truncate(sz); err != nil {
		t.Error(err)
		return
	}

	if g, e := fsz(), sz; g != e {
		t.Error(g, e)
		return
	}

	sz *= 2
	if err := f.Truncate(sz); err != nil {
		t.Error(err)
		return
	}

	if g, e := fsz(), sz; g != e {
		t.Error(g, e)
		return
	}

	sz = 0
	if err := f.Truncate(sz); err != nil {
		t.Error(err)
		return
	}

	if g, e := fsz(), sz; g != e {
		t.Error(g, e)
		return
	}

	// Check Truncate(-1) doesn't work.
	sz = -1
	if err := f.Truncate(sz); err == nil {
		t.Error(err)
		return
	}

	d, err := dump(f.tree)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("\n%s", d)
}

func TestFileReadAtWriteAt(t *testing.T) {
	preRemove(dbname, false)

	db, err := Create(dbname, o)
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	if !*oKeep {
		defer preRemove(dbname, false)
	}

	f, err := db.File("TestFileReadAtWriteAt")
	if err != nil {
		t.Error(err)
		return
	}

	fsz := func() int64 {
		n, err := f.Size()
		if err != nil {
			t.Fatal(err)
		}
		return n
	}

	const (
		N = 1 << 16
		M = 200
	)

	s := make([]byte, N)
	e := make([]byte, N)
	rnd := rand.New(rand.NewSource(42))
	for i := range e {
		s[i] = byte(rnd.Intn(256))
	}
	n2 := 0
	for i := 0; i < M; i++ {
		var from, to int
		for {
			from = rnd.Intn(N)
			to = rnd.Intn(N)
			if from != to {
				break
			}
		}
		if from > to {
			from, to = to, from
		}
		for i := range s[from:to] {
			s[from+i] = byte(rnd.Intn(256))
		}
		copy(e[from:to], s[from:to])
		if to > n2 {
			n2 = to
		}
		n, err := f.WriteAt(s[from:to], int64(from))
		if err != nil {
			t.Error(err)
			return
		}

		if g, e := n, to-from; g != e {
			t.Error(g, e)
			return
		}
	}

	if g, e := fsz(), int64(n2); g != e {
		t.Error(g, e)
		return
	}

	b := make([]byte, n2)
	for i := 0; i <= M; i++ {
		from := rnd.Intn(n2)
		to := rnd.Intn(n2)
		if from > to {
			from, to = to, from
		}
		if i == M {
			from, to = 0, n2
		}
		n, err := f.ReadAt(b[from:to], int64(from))
		if err != nil && (err != io.EOF && n != 0) {
			t.Error(fsz(), from, to, err)
			return
		}

		if g, e := n, to-from; g != e {
			t.Error(g, e)
			return
		}

		if g, e := b[from:to], e[from:to]; !bytes.Equal(g, e) {
			t.Errorf(
				"i %d from %d to %d len(g) %d len(e) %d\n---- got ----\n%s\n---- exp ----\n%s",
				i, from, to, len(g), len(e), hex.Dump(g), hex.Dump(e),
			)
			return
		}
	}

	mf := f
	buf := &bytes.Buffer{}
	if _, err := mf.WriteTo(buf); err != nil {
		t.Error(err)
		return
	}

	if g, e := buf.Bytes(), e[:n2]; !bytes.Equal(g, e) {
		t.Errorf("\nlen %d\n%s\nlen %d\n%s", len(g), hex.Dump(g), len(e), hex.Dump(e))
		return
	}

	if err := mf.Truncate(0); err != nil {
		t.Error(err)
		return
	}

	if _, err := mf.ReadFrom(buf); err != nil {
		t.Error(err)
		return
	}

	roundTrip := make([]byte, n2)
	if n, err := mf.ReadAt(roundTrip, 0); err != nil && n == 0 {
		t.Error(err)
		return
	}

	if g, e := roundTrip, e[:n2]; !bytes.Equal(g, e) {
		t.Errorf("\nlen %d\n%s\nlen %d\n%s", len(g), hex.Dump(g), len(e), hex.Dump(e))
		return
	}
}

func TestFileReadAtHole(t *testing.T) {
	preRemove(dbname, false)

	db, err := Create(dbname, o)
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	if !*oKeep {
		defer preRemove(dbname, false)
	}

	f, err := db.File("TestFileReadAtHole")
	if err != nil {
		t.Error(err)
		return
	}

	n, err := f.WriteAt([]byte{1}, 40000)
	if err != nil {
		t.Error(err)
		return
	}

	if n != 1 {
		t.Error(n)
		return
	}

	n, err = f.ReadAt(make([]byte, 1000), 20000)
	if err != nil {
		t.Error(err)
		return
	}

	if n != 1000 {
		t.Error(n)
		return
	}
}

func BenchmarkFileWrSeq(b *testing.B) {
	preRemove(dbname, false)

	db, err := Create(dbname, o)
	if err != nil {
		b.Fatal(err)
	}

	defer db.Close()
	defer preRemove(dbname, false)

	buf := make([]byte, fileTestChunkSize)
	for i := range buf {
		buf[i] = byte(rand.Int())
	}
	b.SetBytes(fileTestChunkSize)
	f, err := db.File("BenchmarkMemFilerWrSeq")
	if err != nil {
		b.Error(err)
		return
	}

	runtime.GC()
	b.ResetTimer()
	var ofs int64
	for i := 0; i < b.N; i++ {
		_, err := f.WriteAt(buf, ofs)
		if err != nil {
			b.Fatal(err)
		}

		ofs = (ofs + fileTestChunkSize) % fileTotalSize
	}
	b.StopTimer()
}

func BenchmarkFileRdSeq(b *testing.B) {
	preRemove(dbname, true)

	db, err := Create(dbname, &Options{})
	if err != nil {
		b.Fatal(err)
	}

	defer db.Close()
	defer preRemove(dbname, false)

	buf := make([]byte, fileTestChunkSize)
	for i := range buf {
		buf[i] = byte(rand.Int())
	}
	b.SetBytes(fileTestChunkSize)
	f, err := db.File("BenchmarkFileRdSeq")
	if err != nil {
		b.Error(err)
		return
	}

	var ofs int64
	for i := 0; i < b.N; i++ {
		_, err := f.WriteAt(buf, ofs)
		if err != nil {
			b.Fatal(err)
		}

		ofs = (ofs + fileTestChunkSize) % fileTotalSize
	}
	if err := db.Close(); err != nil {
		b.Fatal(err)
		return
	}

	db2, err := Open(dbname, o)
	if err != nil {
		b.Error(err)
		return
	}

	defer db2.Close()

	f, err = db2.File("BenchmarkFileRdSeq")
	if err != nil {
		b.Error(err)
		return
	}

	runtime.GC()
	b.ResetTimer()
	ofs = 0
	for i := 0; i < b.N; i++ {
		n, err := f.ReadAt(buf, ofs)
		if err != nil && n == 0 {
			b.Fatal(err)
		}

		ofs = (ofs + fileTestChunkSize) % fileTotalSize
	}
	b.StopTimer()
}

func TestBits0(t *testing.T) {
	const (
		M = 1024
	)

	N := 100
	if *oACIDEnableWAL {
		N = 50
	}

	preRemove(dbname, false)

	db, err := Create(dbname, o)
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	if !*oKeep {
		defer preRemove(dbname, false)
	}

	f, err := db.File("TestBits0")
	if err != nil {
		t.Error(err)
		return
	}

	b := f.Bits()
	ref := map[uint64]bool{}

	rng := rand.New(rand.NewSource(42))
	for i := 0; i < N; i++ {
		bit := uint64(rng.Int63())
		run := uint64(rng.Intn(M))
		if rng.Int()&1 == 1 {
			run = 1
		}
		op := rng.Intn(3)

		switch op {
		case opOn:
			if err = b.On(bit, run); err != nil {
				t.Error(err)
				return
			}
			for i := bit; i < bit+run; i++ {
				ref[i] = true
			}
		case opOff:
			if err = b.Off(bit, run); err != nil {
				t.Error(err)
				return
			}
			for i := bit; i < bit+run; i++ {
				ref[i] = false
			}
		case opCpl:
			if err = b.Cpl(bit, run); err != nil {
				t.Error(err)
				return
			}
			for i := bit; i < bit+run; i++ {
				ref[i] = !ref[i]
			}
		}

	}

	for bit, v := range ref {
		gv, err := b.Get(bit)
		if err != nil {
			t.Error(err)
			return
		}

		if gv != v {
			d, err := dump(f.tree)
			if err != nil {
				t.Log(err)
			}
			t.Logf("\n%s", d)
			t.Errorf("%#x %t %t", bit, gv, v)
			return
		}
	}
}

func benchmarkBitsOn(b *testing.B, n uint64) {
	preRemove(dbname, false)

	db, err := Create(dbname, o)
	if err != nil {
		b.Fatal(err)
	}

	defer db.Close()

	if !*oKeep {
		defer preRemove(dbname, false)
	}

	f, err := db.File("TestBits0")
	if err != nil {
		b.Error(err)
		return
	}

	bits := f.Bits()

	rng := rand.New(rand.NewSource(42))
	a := make([]uint64, 1024*1024)
	for i := range a {
		a[i] = uint64(rng.Int63())
	}

	b.SetBytes(int64(n) / 8)
	runtime.GC()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bits.On(a[i&0xfffff], n)
	}

	b.StopTimer()
}

func BenchmarkBitsOn16(b *testing.B) {
	benchmarkBitsOn(b, 16)
}

func BenchmarkBitsOn1024(b *testing.B) {
	benchmarkBitsOn(b, 1024)
}

func BenchmarkBitsOn65536(b *testing.B) {
	benchmarkBitsOn(b, 65536)
}

func BenchmarkBitsGetSeq(b *testing.B) {
	preRemove(dbname, false)

	db, err := Create(dbname, o)
	if err != nil {
		b.Fatal(err)
	}

	defer db.Close()

	if !*oKeep {
		defer preRemove(dbname, false)
	}

	f, err := db.File("TestBitsGetSeq")
	if err != nil {
		b.Error(err)
		return
	}

	rng := rand.New(rand.NewSource(42))
	buf := make([]byte, 1024*1024)
	for i := range buf {
		buf[i] = byte(rng.Int63())
	}

	if _, err := f.WriteAt(buf, 0); err != nil {
		b.Fatal(err)
	}

	bits := f.Bits()
	runtime.GC()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bits.Get(uint64(i) & 0x7fffff)
	}
	b.StopTimer()
}

func BenchmarkBitsGetRnd(b *testing.B) {
	preRemove(dbname, false)

	db, err := Create(dbname, o)
	if err != nil {
		b.Fatal(err)
	}

	defer db.Close()

	if !*oKeep {
		defer preRemove(dbname, false)
	}

	f, err := db.File("TestBitsGetRnd")
	if err != nil {
		b.Error(err)
		return
	}

	rng := rand.New(rand.NewSource(42))
	buf := make([]byte, 1024*1024)
	for i := range buf {
		buf[i] = byte(rng.Int63())
	}

	if _, err := f.WriteAt(buf, 0); err != nil {
		b.Fatal(err)
	}

	bits := f.Bits()

	a := make([]uint64, 1024*1024)
	for i := range a {
		a[i] = uint64(rng.Int63() & 0x7fffff)
	}

	runtime.GC()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bits.Get(a[i&0xfffff])
	}
	b.StopTimer()
}

func TestTmpDirRemoval(t *testing.T) {
	preRemove(dbname, false)

	db, err := Create(dbname, o)
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	if !*oKeep {
		defer preRemove(dbname, false)
	}

	names := []string{"b", "/b", "/b/", "tmp", "/tmp", "/tmp/", "/tmp/foo", "z", "/z", "/z/"}

	for i, name := range names {
		if err := db.Set(i, name, 1, 2, 3); err != nil {
			t.Error(err)
			return
		}
	}

	for i, name := range names {

		f, err := db.File(name)
		if err != nil {
			t.Error(err)
			return
		}

		if _, err := f.WriteAt([]byte{byte(i)}, int64(i)); err != nil {
			t.Error(err)
			return
		}
	}

	if err = db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = Open(dbname, o)
	if err != nil {
		t.Fatal(err)
	}

	ref := map[string]bool{}
	for _, name := range names {
		ref[name] = true
	}

	aa, err := db.Arrays()
	if err != nil {
		t.Error(err)
		return
	}

	s, err := aa.Slice(nil, nil)
	if err := s.Do(func(subscripts, value []interface{}) (bool, error) {
		k := subscripts[0].(string)
		delete(ref, k)
		return true, nil
	}); err != nil {
		t.Error(err)
		return
	}

	if len(ref) == 0 {
		t.Error(0)
		return
	}

	for k := range ref {
		if !strings.HasPrefix(k, "/tmp/") {
			t.Error(k)
			return
		}
	}

	ref = map[string]bool{}
	for _, name := range names {
		ref[name] = true
	}

	ff, err := db.Files()
	if err != nil {
		t.Error(err)
		return
	}

	s, err = ff.Slice(nil, nil)
	if err := s.Do(func(subscripts, value []interface{}) (bool, error) {
		k := subscripts[0].(string)
		delete(ref, k)
		return true, nil
	}); err != nil {
		t.Error(err)
		return
	}

	if len(ref) == 0 {
		t.Error(0)
		return
	}

	for k := range ref {
		if !strings.HasPrefix(k, "/tmp/") {
			t.Error(k)
			return
		}
	}

}

/*

2013-04-24
==========

(13:48) jnml@fsc-r550:~/src/github.com/cznic/exp/dbm$ . bench
++ go test -v -run Bench -keep -tbench -cpu 4
=== RUN TestBenchArraySetGet-4
--- PASS: TestBenchArraySetGet-4 (114.56 seconds)
	all_test.go:2678: WR: 52696 ops in 6.000e+01 s, 8.783e+02 ops/s, 1.139e-03 s/op
	all_test.go:2727: RD: 52696 ops in 5.451e+01 s, 9.667e+02 ops/s, 1.034e-03 s/op
PASS
ok  	github.com/cznic/exp/dbm	114.575s
++ go test -v -run Bench -keep -tbench -cpu 4 -xact
=== RUN TestBenchArraySetGet-4
--- PASS: TestBenchArraySetGet-4 (112.89 seconds)
	all_test.go:2678: WR: 46591 ops in 6.000e+01 s, 7.765e+02 ops/s, 1.288e-03 s/op
	all_test.go:2727: RD: 46591 ops in 5.284e+01 s, 8.818e+02 ops/s, 1.134e-03 s/op
PASS
ok  	github.com/cznic/exp/dbm	112.909s
++ go test -v -run Bench -keep -tbench -cpu 4 -wal -grace 0ms
=== RUN TestBenchArraySetGet-4
--- PASS: TestBenchArraySetGet-4 (60.35 seconds)
	all_test.go:2678: WR: 606 ops in 6.005e+01 s, 1.009e+01 ops/s, 9.909e-02 s/op, max WAL size 2157568
	all_test.go:2727: RD: 606 ops in 1.252e-01 s, 4.841e+03 ops/s, 2.066e-04 s/op, max WAL size 0
PASS
ok  	github.com/cznic/exp/dbm	60.363s
++ go test -v -run Bench -keep -tbench -cpu 4 -wal -grace 1ms
=== RUN TestBenchArraySetGet-4
--- PASS: TestBenchArraySetGet-4 (87.66 seconds)
	all_test.go:2678: WR: 28027 ops in 6.000e+01 s, 4.671e+02 ops/s, 2.141e-03 s/op, max WAL size 3241648
	all_test.go:2727: RD: 28027 ops in 2.746e+01 s, 1.021e+03 ops/s, 9.799e-04 s/op, max WAL size 0
PASS
ok  	github.com/cznic/exp/dbm	87.676s
++ go test -v -run Bench -keep -tbench -cpu 4 -wal -grace 10ms
=== RUN TestBenchArraySetGet-4
--- PASS: TestBenchArraySetGet-4 (89.65 seconds)
	all_test.go:2678: WR: 29882 ops in 6.000e+01 s, 4.980e+02 ops/s, 2.008e-03 s/op, max WAL size 2778448
	all_test.go:2727: RD: 29882 ops in 2.942e+01 s, 1.016e+03 ops/s, 9.846e-04 s/op, max WAL size 0
PASS
ok  	github.com/cznic/exp/dbm	89.663s
++ go test -v -run Bench -keep -tbench -cpu 4 -wal -grace 100ms
=== RUN TestBenchArraySetGet-4
--- PASS: TestBenchArraySetGet-4 (95.21 seconds)
	all_test.go:2678: WR: 34607 ops in 6.000e+01 s, 5.768e+02 ops/s, 1.734e-03 s/op, max WAL size 2242960
	all_test.go:2727: RD: 34607 ops in 3.494e+01 s, 9.903e+02 ops/s, 1.010e-03 s/op, max WAL size 0
PASS
ok  	github.com/cznic/exp/dbm	95.225s
++ go test -v -run Bench -keep -tbench -cpu 4 -wal -grace 1s
=== RUN TestBenchArraySetGet-4
--- PASS: TestBenchArraySetGet-4 (108.43 seconds)
	all_test.go:2678: WR: 44756 ops in 6.000e+01 s, 7.459e+02 ops/s, 1.341e-03 s/op, max WAL size 1375056
	all_test.go:2727: RD: 44756 ops in 4.823e+01 s, 9.279e+02 ops/s, 1.078e-03 s/op, max WAL size 0
PASS
ok  	github.com/cznic/exp/dbm	108.443s
++ go test -v -run Bench -keep -tbench -cpu 4 -wal -grace 10s
=== RUN TestBenchArraySetGet-4
--- PASS: TestBenchArraySetGet-4 (111.52 seconds)
	all_test.go:2678: WR: 47580 ops in 6.000e+01 s, 7.930e+02 ops/s, 1.261e-03 s/op, max WAL size 704256
	all_test.go:2727: RD: 47580 ops in 5.126e+01 s, 9.283e+02 ops/s, 1.077e-03 s/op, max WAL size 0
PASS
ok  	github.com/cznic/exp/dbm	111.529s
(14:01) jnml@fsc-r550:~/src/github.com/cznic/exp/dbm$

===============================================================================

(14:02) jnml@fsc-r550:~/src/github.com/cznic/exp/dbm$ OPTS=-nozip . bench
++ go test -v -run Bench -keep -tbench -cpu 4 -nozip
=== RUN TestBenchArraySetGet-4
--- PASS: TestBenchArraySetGet-4 (100.73 seconds)
	all_test.go:2737: WR: 186071 ops in 6.000e+01 s, 3.101e+03 ops/s, 3.225e-04 s/op
	all_test.go:2786: RD: 186071 ops in 4.057e+01 s, 4.586e+03 ops/s, 2.181e-04 s/op
PASS
ok  	github.com/cznic/exp/dbm	100.739s
++ go test -v -run Bench -keep -tbench -cpu 4 -xact -nozip
=== RUN TestBenchArraySetGet-4
--- PASS: TestBenchArraySetGet-4 (103.31 seconds)
	all_test.go:2737: WR: 72776 ops in 6.000e+01 s, 1.213e+03 ops/s, 8.245e-04 s/op
	all_test.go:2786: RD: 72776 ops in 4.322e+01 s, 1.684e+03 ops/s, 5.939e-04 s/op
PASS
ok  	github.com/cznic/exp/dbm	103.318s
++ go test -v -run Bench -keep -tbench -cpu 4 -wal -grace 0ms -nozip
=== RUN TestBenchArraySetGet-4
--- PASS: TestBenchArraySetGet-4 (60.27 seconds)
	all_test.go:2737: WR: 503 ops in 6.003e+01 s, 8.379e+00 ops/s, 1.193e-01 s/op, max WAL size 5005056
	all_test.go:2786: RD: 503 ops in 4.467e-02 s, 1.126e+04 ops/s, 8.881e-05 s/op, max WAL size 0
PASS
ok  	github.com/cznic/exp/dbm	60.281s
++ go test -v -run Bench -keep -tbench -cpu 4 -wal -grace 1ms -nozip
=== RUN TestBenchArraySetGet-4
--- PASS: TestBenchArraySetGet-4 (82.80 seconds)
	all_test.go:2737: WR: 51227 ops in 6.000e+01 s, 8.538e+02 ops/s, 1.171e-03 s/op, max WAL size 9436816
	all_test.go:2786: RD: 51227 ops in 2.239e+01 s, 2.288e+03 ops/s, 4.371e-04 s/op, max WAL size 0
PASS
ok  	github.com/cznic/exp/dbm	82.818s
++ go test -v -run Bench -keep -tbench -cpu 4 -wal -grace 10ms -nozip
=== RUN TestBenchArraySetGet-4
--- PASS: TestBenchArraySetGet-4 (85.57 seconds)
	all_test.go:2737: WR: 58299 ops in 6.000e+01 s, 9.716e+02 ops/s, 1.029e-03 s/op, max WAL size 10214656
	all_test.go:2786: RD: 58299 ops in 2.536e+01 s, 2.299e+03 ops/s, 4.350e-04 s/op, max WAL size 0
PASS
ok  	github.com/cznic/exp/dbm	85.577s
++ go test -v -run Bench -keep -tbench -cpu 4 -wal -grace 100ms -nozip
=== RUN TestBenchArraySetGet-4
--- PASS: TestBenchArraySetGet-4 (87.81 seconds)
	all_test.go:2739: WR: 62461 ops in 6.000e+01 s, 1.041e+03 ops/s, 9.606e-04 s/op, max WAL size 9486960
	all_test.go:2788: RD: 62461 ops in 2.747e+01 s, 2.274e+03 ops/s, 4.398e-04 s/op, max WAL size 0
PASS
ok  	github.com/cznic/exp/dbm	87.818s
++ go test -v -run Bench -keep -tbench -cpu 4 -wal -grace 1s -nozip
=== RUN TestBenchArraySetGet-4
--- PASS: TestBenchArraySetGet-4 (93.33 seconds)
	all_test.go:2739: WR: 73658 ops in 6.000e+01 s, 1.228e+03 ops/s, 8.146e-04 s/op, max WAL size 7266464
	all_test.go:2788: RD: 73658 ops in 3.295e+01 s, 2.236e+03 ops/s, 4.473e-04 s/op, max WAL size 0
PASS
ok  	github.com/cznic/exp/dbm	93.338s
++ go test -v -run Bench -keep -tbench -cpu 4 -wal -grace 10s -nozip
=== RUN TestBenchArraySetGet-4
--- PASS: TestBenchArraySetGet-4 (97.34 seconds)
	all_test.go:2739: WR: 75944 ops in 6.000e+01 s, 1.266e+03 ops/s, 7.901e-04 s/op, max WAL size 5116016
	all_test.go:2788: RD: 75944 ops in 3.690e+01 s, 2.058e+03 ops/s, 4.859e-04 s/op, max WAL size 0
PASS
ok  	github.com/cznic/exp/dbm	97.354s
(14:14) jnml@fsc-r550:~/src/github.com/cznic/exp/dbm$

*/
func TestBenchArraySetGet(t *testing.T) {
	if !*oBench {
		t.Log("Must be enabled by -tbench")
		return
	}

	preRemove(dbname, false)

	db, err := Create(dbname, o)
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	if !*oKeep {
		defer preRemove(dbname, false)
	}

	a, err := db.Array("test")
	if err != nil {
		t.Error(err)
		return
	}

	c := time.After(time.Minute)
	t0 := time.Now()
	var maxSet int64
loop:
	for i := 0; ; {
		select {
		case <-c:
			maxSet = int64(i - 1)
			ftot := float64(time.Since(t0)) / float64(time.Second)
			s := ""
			if af, ok := db.filer.(*lldb.ACIDFiler0); ok {
				s = fmt.Sprintf(", max WAL size %d", af.PeakWALSize())
			}
			t.Logf("WR: %d ops in %8.3e s, %8.3e ops/s, %8.3e s/op%s", i, ftot, float64(i)/ftot, ftot/float64(i), s)
			break loop
		default:
		}

		if err = a.Set(i^0x55555555, i); err != nil {
			t.Error(err)
			return
		}

		i++
	}

	if err = db.Close(); err != nil {
		t.Error(err)
		return
	}

	if db, err = Open(dbname, o); err != nil {
		t.Error(err)
		return
	}

	a, err = db.Array("test")
	if err != nil {
		t.Error(err)
		return
	}

	t0 = time.Now()
	for i := int64(0); i <= maxSet; i++ {
		v, err := a.Get(i)
		if err != nil {
			t.Error(err)
			return
		}

		if g, e := v, int64(i^0x55555555); g != e {
			t.Errorf("i %d: %T(%v) %T(%v)", i, g, g, e, e)
			return
		}
	}

	ftot := float64(time.Since(t0)) / float64(time.Second)
	i := maxSet + 1
	s := ""
	if af, ok := db.filer.(*lldb.ACIDFiler0); ok {
		s = fmt.Sprintf(", max WAL size %d", af.PeakWALSize())
	}
	t.Logf("RD: %d ops in %8.3e s, %8.3e ops/s, %8.3e s/op%s", i, ftot, float64(i)/ftot, ftot/float64(i), s)
}
