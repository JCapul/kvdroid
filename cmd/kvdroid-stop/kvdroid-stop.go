package main

import (
	"flag"
	"fmt"

	"github.com/JCapul/kvdroid"
)

func main() {
	host := flag.String("host", "", "kvdroid server hostname")
	port := flag.Int("port", 8001, "kvdroid server port")
	flag.Parse()

	client := kvdroid.NewClient(fmt.Sprintf("%s:%d", *host, *port))
	client.Shutdown()
}
