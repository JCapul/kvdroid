package main

import (
	"flag"
	"os"
	"log"

	"github.com/JCapul/kvdroid"
	"github.com/sevlyar/go-daemon"
)

func main() {
	bind := flag.String("bind", "127.0.0.1", "network interface to listen on")
	port := flag.Int("port", 8001, "port number")
	buckets := flag.Int("buckets", 100, "number of buckets")
	daemonize := flag.Bool("daemonize", false, "run the server as a daemon")
	flag.Parse()

	if *daemonize {
		cntxt := &daemon.Context{
			PidFileName: "kvdroid.pid",
			PidFilePerm: 0644,
			LogFileName: "kvdroid.log",
			LogFilePerm: 0640,
			WorkDir:     "./",
			Umask:       027,
			Args:        flag.Args(),
		}

		cwd, err := os.Getwd()
		if err != nil {
			panic(err)
		}
		log.Printf("Starting kvdroid as a daemon (pid file: %s/kvdroid.pid)", cwd)
	
		d, err := cntxt.Reborn()
		if err != nil {
			log.Fatal("Unable to run: ", err)
		}
		if d != nil {
			return
		}
		defer cntxt.Release()
	}

	opts := kvdroid.ServerOptions{
		Bind: *bind,
		Port: *port,
		Buckets: *buckets,
	}
	server := kvdroid.NewServer(&opts)
	server.Start()
}
