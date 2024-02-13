package main

import (
	"compress/gzip"
	"fmt"
	"os"

	"github.com/asdine/storm"
	"github.com/asdine/storm/codec/msgpack"
	"github.com/jszwec/csvutil"
)

type Act struct {
	ID int64   `storm:"index"` // Timestamp
	T  uint16  // Type
	V  float64 // Value
}

func main() {
	fmt.Println("Usage: analytic <db file>")
	file := ""
	if len(os.Args) > 1 {
		file = os.Args[1]
	}
	db, err := storm.Open(file, storm.Codec(msgpack.Codec))
	if err != nil {
		fmt.Println("cannot open db file:", err)
		return
	}
	var acts []Act
	err = db.AllByIndex("ID", &acts)
	if err != nil {
		fmt.Println("cannot read data:", err)
		return
	}
	d, err := csvutil.Marshal(&acts)
	if err != nil {
		fmt.Println("error saving records:", err)
		return
	}
	f, err := os.Create(file + ".csv.gz")
	if err != nil {
		fmt.Println("error creating file:", err)
		return
	}
	defer f.Close()

	gz := gzip.NewWriter(f)
	defer gz.Close()

	_, err = gz.Write(d)
	if err != nil {
		fmt.Println("error saving file:", err)
	}
}
