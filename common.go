package kvdroid

import (
	"encoding/binary"
	"io"
	"net"
)

/* Helpers */

func try(err error) {
	if err != nil {
		panic(err)
	}
}

var check = try

/* Enums */

// Message enum
type Message byte

const (
	getBytesCmd Message = 'a' + iota
	getBytesIntoCmd
	getBytesRangeCmd
	getBytesRangeIntoCmd
	setBytesCmd
	setBytesRangeCmd
	delBytesCmd
	truncateBytesCmd
	setUintCmd
	getUintCmd
	setUintIfMaxCmd
	delUintCmd
	stopCmd
	ackReply
	errNoKeyReply
)

// Single I/O protocol helpers

func readMessage(conn net.Conn) (Message, error) {
	b := make([]byte, 1, 1)
	_, err := io.ReadAtLeast(conn, b, 1)
	if err == io.ErrUnexpectedEOF {
		panic(err)
	}
	return Message(b[0]), err
}

func sendMessage(conn net.Conn, m Message) error {
	_, err := conn.Write([]byte{byte(m)})
	return err
}

func readUint32(conn net.Conn) (uint32, error) {
	b := make([]byte, 4, 4)
	_, err := io.ReadAtLeast(conn, b, 4)
	if err == io.ErrUnexpectedEOF {
		panic(err)
	}
	return binary.LittleEndian.Uint32(b), err
}

func sendUint32(conn net.Conn, value uint32) error {
	b := make([]byte, 4, 4)
	binary.LittleEndian.PutUint32(b, value)
	_, err := conn.Write(b)
	return err
}

func readFillBuf(conn net.Conn, dst []byte) error {
	_, err := io.ReadAtLeast(conn, dst, len(dst))
	if err == io.ErrUnexpectedEOF {
		panic(err)
	}
	return err
}

// multi I/O protocol helpers

func sendBytes(conn net.Conn, data []byte) error {
	sendUint32(conn, uint32(len(data)))
	_, err := conn.Write(data)
	return err
}

func readBytes(conn net.Conn) ([]byte, error) {
	size, err := readUint32(conn)
	if err != nil {
		return nil, err
	}
	b := make([]byte, size, size)
	err = readFillBuf(conn, b)
	return b, err
}

func readBytesInto(conn net.Conn, dst []byte) (uint32, error) {
	size, err := readUint32(conn)
	if err != nil {
		return 0, err
	}
	_, err = io.ReadAtLeast(conn, dst, int(size))
	if err == io.ErrUnexpectedEOF {
		panic(err)
	}
	if size < uint32(len(dst)) {
		return size, io.EOF
	}
	return size, nil
}

func readString(conn net.Conn) (string, error) {
	b, err := readBytes(conn)
	return string(b), err
}
