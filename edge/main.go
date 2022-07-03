package main

import (
	"flag"
	"log"
	"net/http"
	_ "net/http/pprof"
	"strconv"
)

var coordinatorIp = flag.String("coordinatorIp", "", "Ip of coordinator")
var port = flag.Int("port", 6789, "port of edge service")
var dataDir = flag.String("dataDir", "", "dir of table file")
// ../experiments/data

func main() {
	flag.Parse()

	if *coordinatorIp == "" {
		log.Fatal("CoordinatorIp can't be empty")
	}
	if *dataDir == "" {
		log.Fatal("DataDir can't be empty")
	}

	w := MakeWorker(*coordinatorIp, *port, *dataDir)
	go func() {
		addr := "0.0.0.0:" + strconv.Itoa(6060 + w.id)
		http.ListenAndServe(addr, nil)
	} ()
	w.Server()
}
