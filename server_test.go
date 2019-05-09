package kvdroid_test

import (
	"testing"
	"time"

	"github.com/JCapul/kvdroid"
)

func TestStartShutdown(t *testing.T) {
	server := kvdroid.NewServer(&kvdroid.ServerOptions{Port: -1})
	go server.Start()
	time.Sleep(1 * time.Second)
	server.Shutdown()
}
