package main

import "C"
import (
	"flag"
	"io/ioutil"
	"log"
	"time"
)

var metaFile = flag.String("metaJson", "", "json file contains table meta used")
// ../experiments/data/tablesMeta.json
var query = flag.String("query", "", "query string about TDC")
// "DECLARE s, t IN customer, s.c_custkey = t.c_custkey AND s.c_phone != t.c_phone"
// "DECLARE s,t IN sales, s.Product number = t.Product number AND s.Date = t.Date AND s.Unit price != t.Unit price"
var epsilon = flag.Float64("epsilon", 0.0, "epsilon")

func main() {
	flag.Parse()

	if *metaFile == "" {
		log.Fatal("metaJson can't be empty")
	}
	if *query == "" {
		log.Fatal("query can't be empty")
	}

	meta, err := ioutil.ReadFile(*metaFile)
	if err != nil {
		log.Fatal(err.Error())
	}

	c := MakeCoordinator(meta)
	go c.Server()

	// wait for worker to register
	time.Sleep(time.Duration(30) * time.Second)

	c.InitConnect()

	ok, detail := c.CheckQuery(*query)
	if !ok {
		log.Fatal(detail)
	}

	if *epsilon == 0.0 {
		c.ConflictDetect()
		c.Repair(true)
	} else {
		c.Evaluate(*epsilon)
	}

	c.Close()
}
