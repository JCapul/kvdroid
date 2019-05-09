package kvdroid_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/JCapul/kvdroid"
	"github.com/JCapul/kvdroid/util"
)

func initClientServer() (*kvdroid.Server, *kvdroid.Client) {
	server := kvdroid.NewServer(&kvdroid.ServerOptions{Port: -1})
	go server.Start()
	client := kvdroid.NewClient(server.Addr())
	return server, client
}

func TestConnect(t *testing.T) {
	server, client := initClientServer()
	defer server.Shutdown()
	client.Close()
}
func TestShutdown(t *testing.T) {
	_, client := initClientServer()
	client.Shutdown()
}

func TestNoKey(t *testing.T) {
	server, client := initClientServer()
	defer server.Shutdown()
	defer client.Close()

	_, err := client.GetBytes("foo")
	util.Equals(t, kvdroid.ErrKeyNotFound, err, "should raise KeyNotFound error")

	_, err = client.GetBytes("bar")
	util.Equals(t, kvdroid.ErrKeyNotFound, err, "should raise KeyNotFound error")

	recv := make([]byte, 20, 20)
	_, err = client.GetBytesInto("foo", recv)
	util.Equals(t, kvdroid.ErrKeyNotFound, err, "should raise KeyNotFound error")

	_, err = client.GetBytesRange("foo", uint32(0), uint32(20))
	util.Equals(t, kvdroid.ErrKeyNotFound, err, "should raise KeyNotFound error")

	_, err = client.GetBytesRangeInto("foo", uint32(0), uint32(20), recv)
	util.Equals(t, kvdroid.ErrKeyNotFound, err, "should raise KeyNotFound error")

	err = client.TruncateBytes("foo", uint32(3))
	util.Equals(t, kvdroid.ErrKeyNotFound, err, "should raise KeyNotFound error")

	_, err = client.GetUint("foo")
	util.Equals(t, kvdroid.ErrKeyNotFound, err, "should raise KeyNotFound error")

	err = client.DelUint("foo")
	util.Equals(t, kvdroid.ErrKeyNotFound, err, "should raise KeyNotFound error")
}

func setGetBytes(t *testing.T, client *kvdroid.Client, key string, data []byte) {
	client.SetBytes(key, data)
	recv, err := client.GetBytes(key)
	util.Ok(t, err)
	util.Equals(t, data, recv, "received data does not match sent data")
}

func TestSetGetBytes(t *testing.T) {
	server, client := initClientServer()
	defer server.Shutdown()
	defer client.Close()

	setGetBytes(t, client, "foo", bytes.Repeat([]byte("0123456789"), 100))
	setGetBytes(t, client, "bar", bytes.Repeat([]byte("9876543210"), 10))
	setGetBytes(t, client, "empty", []byte(""))
}

func TestSetBytes(t *testing.T) {
	server, client := initClientServer()
	defer server.Shutdown()
	defer client.Close()

	client.SetBytes("foo", []byte("bar"))
	_, err := client.GetBytes("foo")
	util.Ok(t, err)

	err = client.DelBytes("foo")
	util.Ok(t, err)

	_, err = client.GetBytes("foo")
	util.Equals(t, kvdroid.ErrKeyNotFound, err, "should raise KeyNotFound error")

	// test not-existing key
	err = client.DelBytes("baz")
	util.Equals(t, kvdroid.ErrKeyNotFound, err, "should raise KeyNotFound error")
}

func TestGetBytesInto(t *testing.T) {
	server, client := initClientServer()
	defer server.Shutdown()
	defer client.Close()

	sent := []byte("0123456789")
	sentLen := uint32(len(sent))
	client.SetBytes("foo", sent)

	// Get entire value
	recv := make([]byte, sentLen, sentLen)
	n, err := client.GetBytesInto("foo", recv)
	util.Ok(t, err)
	util.Equals(t, sentLen, n, "number of received bytes does not match what was sent")
	util.Equals(t, sent, recv, "received data does not match sent data")

	// Get smaller value
	recv = make([]byte, 5, 5)
	n, err = client.GetBytesInto("foo", recv)
	util.Ok(t, err)
	util.Equals(t, uint32(5), n, "number of received bytes does not match what was sent")
	util.Equals(t, []byte("01234"), recv, "received data does not match sent data")

	// Get larger value
	recv = make([]byte, sentLen+3, sentLen+3)
	n, err = client.GetBytesInto("foo", recv)
	util.Equals(t, io.EOF, err, "err should be EOF")
	util.Equals(t, sentLen, n, "number of received bytes does not match what was sent")
	util.Equals(t, []byte("0123456789\000\000\000"), recv, "received data does not match sent data")
}

func TestGetBytesRange(t *testing.T) {
	server, client := initClientServer()
	defer server.Shutdown()
	defer client.Close()

	sent := bytes.Repeat([]byte("0123456789"), 10)
	sentLen := uint32(len(sent))
	client.SetBytes("foo", sent)

	// Get entire value
	recv, err := client.GetBytes("foo")
	util.Ok(t, err)
	util.Equals(t, sent, recv, "received data does not match sent data")

	// GetRange entire value
	recv, err = client.GetBytesRange("foo", uint32(0), sentLen)
	util.Ok(t, err)
	util.Equals(t, sent, recv, "received data does not match sent data")

	// GetRange first item
	recv, err = client.GetBytesRange("foo", uint32(0), uint32(0))
	util.Ok(t, err)
	util.Equals(t, []byte("0"), recv, "received data does not match sent data")

	// GetRange last item
	recv, err = client.GetBytesRange("foo", sentLen-1, sentLen-1)
	util.Ok(t, err)
	util.Equals(t, []byte("9"), recv, "received data does not match sent data")

	// GetRange one itme past the array
	recv, err = client.GetBytesRange("foo", sentLen, sentLen)
	util.Ok(t, err)
	util.Equals(t, []byte(""), recv, "received data does not match sent data")

	// GetRange a range outside the array
	recv, err = client.GetBytesRange("foo", sentLen+3, sentLen+100)
	util.Ok(t, err)
	util.Equals(t, []byte(""), recv, "received data does not match sent data")

	// SetRange a new value, GetRange inside the array
	recv, err = client.GetBytesRange("foo", uint32(10), uint32(20))
	util.Ok(t, err)
	util.Equals(t, []byte("01234567890"), recv, "received data does not match sent data")

	// SetRange a new value, GetRange starting inside and ending outside
	recv, err = client.GetBytesRange("foo", sentLen-3, sentLen+4)
	util.Ok(t, err)
	util.Equals(t, []byte("789"), recv, "received data does not match sent data")
}

func TestGetBytesRangeInto(t *testing.T) {
	server, client := initClientServer()
	defer server.Shutdown()
	defer client.Close()

	sent := []byte("0123456789")
	sentLen := uint32(len(sent))
	client.SetBytes("foo", sent)

	// get range = sent range = recv buffer size
	recv := make([]byte, sentLen, sentLen)
	n, err := client.GetBytesRangeInto("foo", uint32(0), sentLen, recv)
	util.Ok(t, err)
	util.Equals(t, sentLen, n, "number of received bytes does not match what was sent")
	util.Equals(t, sent, recv, "received data does not match sent data")

	// get range past  sent range (outside)
	recv = make([]byte, sentLen, sentLen)
	n, err = client.GetBytesRangeInto("foo", sentLen+2, sentLen+5, recv)
	util.Equals(t, io.EOF, err, "err should be EOF")
	util.Equals(t, uint32(0), n, "number of received bytes does not match what was sent")
	empty := make([]byte, sentLen, sentLen)
	util.Equals(t, empty, recv, "received data does not match sent data")

	// get range < recv buffer size
	recv = make([]byte, sentLen, sentLen)
	n, err = client.GetBytesRangeInto("foo", uint32(0), uint32(5), recv)
	util.Equals(t, io.EOF, err, "err should be EOF")
	util.Equals(t, uint32(6), n, "number of received bytes does not match what was sent")
	util.Equals(t, []byte("012345\000\000\000\000"), recv, "received data does not match sent data")

	// get range > recv buffer size
	recv = make([]byte, 4, 4)
	n, err = client.GetBytesRangeInto("foo", uint32(0), sentLen, recv)
	util.Ok(t, err)
	util.Equals(t, uint32(4), n, "number of received bytes does not match what was sent")
	util.Equals(t, []byte("0123"), recv, "received data does not match sent data")

}

func TestSetBytesRange(t *testing.T) {
	server, client := initClientServer()
	defer server.Shutdown()
	defer client.Close()

	client.SetBytesRange("foo", uint32(3), []byte("3456789"))
	recv, err := client.GetBytes("foo")
	util.Ok(t, err)
	util.Equals(t, []byte("\000\000\0003456789"), recv, "received data does not match sent data")

	client.SetBytesRange("foo", uint32(0), []byte("012"))
	recv, err = client.GetBytes("foo")
	util.Ok(t, err)
	util.Equals(t, []byte("0123456789"), recv, "received data does not match sent data")

	// SetRange first item
	client.SetBytesRange("foo", uint32(0), []byte("a"))
	recv, err = client.GetBytes("foo")
	util.Ok(t, err)
	util.Equals(t, []byte("a123456789"), recv, "received data does not match sent data")

	// SetRange last item
	client.SetBytesRange("foo", uint32(9), []byte("j"))
	recv, err = client.GetBytes("foo")
	util.Ok(t, err)
	util.Equals(t, []byte("a12345678j"), recv, "received data does not match sent data")

	// SetRange inside existing value
	client.SetBytesRange("foo", uint32(3), []byte("def"))
	recv, err = client.GetBytes("foo")
	util.Ok(t, err)
	util.Equals(t, []byte("a12def678j"), recv, "received data does not match sent data")

	// SetRange just past existing value
	client.SetBytesRange("foo", uint32(10), []byte("klm"))
	recv, err = client.GetBytes("foo")
	util.Ok(t, err)
	util.Equals(t, []byte("a12def678jklm"), recv, "received data does not match sent data")

	// SetRange past existing value with some null-byte padding
	client.SetBytesRange("foo", uint32(15), []byte("pqr"))
	recv, err = client.GetBytes("foo")
	util.Ok(t, err)
	util.Equals(t, []byte("a12def678jklm\000\000pqr"), recv, "received data does not match sent data")

	// SetRange starting inside and ending outside existing array
	client.SetBytesRange("foo", uint32(16), []byte("QRSTU"))
	recv, err = client.GetBytes("foo")
	util.Ok(t, err)
	util.Equals(t, []byte("a12def678jklm\000\000pQRSTU"), recv, "received data does not match sent data")
}

func TestTruncateBytes(t *testing.T) {
	server, client := initClientServer()
	defer server.Shutdown()
	defer client.Close()

	sent := []byte("0123456789")
	client.SetBytes("foo", sent)

	err := client.TruncateBytes("foo", 3)
	util.Ok(t, err)

	recv, err := client.GetBytes("foo")
	util.Ok(t, err)
	util.Equals(t, []byte("012"), recv, "received data does not match sent data")

	// truncate larger than acutal size -> no op
	err = client.TruncateBytes("foo", 6)
	util.Ok(t, err)

	recv, err = client.GetBytes("foo")
	util.Ok(t, err)
	util.Equals(t, []byte("012"), recv, "received data does not match sent data")
}

func TestSetGetDelUint(t *testing.T) {
	server, client := initClientServer()
	defer server.Shutdown()
	defer client.Close()

	client.SetUint("foo", uint32(4))
	recv, err := client.GetUint("foo")
	util.Ok(t, err)
	util.Equals(t, uint32(4), recv, "values are different")

	err = client.DelUint("foo")
	util.Ok(t, err)

	_, err = client.GetUint("foo")
	util.Equals(t, kvdroid.ErrKeyNotFound, err, "key should not exist")
}

func TestSetUintIfMax(t *testing.T) {
	server, client := initClientServer()
	defer server.Shutdown()
	defer client.Close()

	client.SetUintIfMax("foo", uint32(4))
	recv, err := client.GetUint("foo")
	util.Ok(t, err)
	util.Equals(t, uint32(4), recv, "values are different")

	// set lower value -> no op
	client.SetUintIfMax("foo", uint32(2))
	recv, err = client.GetUint("foo")
	util.Ok(t, err)
	util.Equals(t, uint32(4), recv, "values are different")

	// set higher value
	client.SetUintIfMax("foo", uint32(100))
	recv, err = client.GetUint("foo")
	util.Ok(t, err)
	util.Equals(t, uint32(100), recv, "values are different")
}
