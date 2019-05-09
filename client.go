package kvdroid

import (
	"errors"
	"fmt"
	"io"
	"net"
)

var (
	// ErrKeyNotFound is raised when the key requested by the client is not found
	ErrKeyNotFound = errors.New("key not found")
)

// Client ...
type Client struct {
	conn net.Conn
}

// NewClient ...
func NewClient(addr string) *Client {
	conn, err := net.Dial("tcp", addr)
	check(err)
	return &Client{
		conn: conn,
	}
}

// Close ...
func (c *Client) Close() error {
	return c.conn.Close()
}

// Shutdown ...
func (c *Client) Shutdown() {
	try(sendMessage(c.conn, stopCmd))
	reply, err := readMessage(c.conn)
	check(err)
	switch reply {
	case ackReply:
		return
	default:
		panic(fmt.Errorf("Server error: %s", string(reply)))
	}
}

// GetBytes ...
func (c *Client) GetBytes(key string) ([]byte, error) {
	try(sendMessage(c.conn, getBytesCmd))
	try(sendBytes(c.conn, []byte(key)))
	reply, err := readMessage(c.conn)
	check(err)
	switch reply {
	case errNoKeyReply:
		return nil, ErrKeyNotFound
	case ackReply:
		data, err := readBytes(c.conn)
		check(err)
		return data, nil
	default:
		panic(fmt.Errorf("Server error: %s", string(reply)))
	}
}

// GetBytesInto ...
func (c *Client) GetBytesInto(key string, dst []byte) (uint32, error) {
	try(sendMessage(c.conn, getBytesIntoCmd))
	try(sendBytes(c.conn, []byte(key)))
	try(sendUint32(c.conn, uint32(len(dst))))
	reply, err := readMessage(c.conn)
	check(err)
	switch reply {
	case errNoKeyReply:
		return 0, ErrKeyNotFound
	case ackReply:
		n, err := readBytesInto(c.conn, dst)
		if err != nil && err != io.EOF {
			panic(err)
		}
		return n, err
	default:
		panic(fmt.Errorf("Server error: %s", string(reply)))
	}
}

// GetBytesRange ...
func (c *Client) GetBytesRange(key string, start, end uint32) ([]byte, error) {
	try(sendMessage(c.conn, getBytesRangeCmd))
	try(sendBytes(c.conn, []byte(key)))
	try(sendUint32(c.conn, start))
	try(sendUint32(c.conn, end))
	reply, err := readMessage(c.conn)
	check(err)
	switch reply {
	case errNoKeyReply:
		return nil, ErrKeyNotFound
	case ackReply:
		data, err := readBytes(c.conn)
		check(err)
		return data, nil
	default:
		panic(fmt.Errorf("Server error: %s", string(reply)))
	}
}

// GetBytesRangeInto ...
func (c *Client) GetBytesRangeInto(key string, start, end uint32, dst []byte) (uint32, error) {
	try(sendMessage(c.conn, getBytesRangeIntoCmd))
	try(sendBytes(c.conn, []byte(key)))
	try(sendUint32(c.conn, start))
	try(sendUint32(c.conn, end))
	try(sendUint32(c.conn, uint32(len(dst))))
	reply, err := readMessage(c.conn)
	check(err)
	switch reply {
	case errNoKeyReply:
		return 0, ErrKeyNotFound
	case ackReply:
		n, err := readBytesInto(c.conn, dst)
		if err != nil && err != io.EOF {
			panic(err)
		}
		return n, err
	default:
		panic(fmt.Errorf("Server error: %s", string(reply)))
	}
}

// SetBytes ...
func (c *Client) SetBytes(key string, data []byte) {
	try(sendMessage(c.conn, setBytesCmd))
	try(sendBytes(c.conn, []byte(key)))
	try(sendBytes(c.conn, data))
	reply, err := readMessage(c.conn)
	check(err)
	switch reply {
	case ackReply:
		return
	default:
		panic(fmt.Errorf("Server error: %s", string(reply)))
	}
}

// SetBytesRange ...
func (c *Client) SetBytesRange(key string, start uint32, data []byte) {
	try(sendMessage(c.conn, setBytesRangeCmd))
	try(sendBytes(c.conn, []byte(key)))
	try(sendUint32(c.conn, start))
	try(sendBytes(c.conn, data))
	reply, err := readMessage(c.conn)
	check(err)
	switch reply {
	case ackReply:
		return
	default:
		panic(fmt.Errorf("Server error: %s", string(reply)))
	}
}

// DelBytes ...
func (c *Client) DelBytes(key string) error {
	try(sendMessage(c.conn, delBytesCmd))
	try(sendBytes(c.conn, []byte(key)))
	reply, err := readMessage(c.conn)
	check(err)
	switch reply {
	case errNoKeyReply:
		return ErrKeyNotFound
	case ackReply:
		return nil
	default:
		panic(fmt.Errorf("Server error: %s", string(reply)))
	}
}

// TruncateBytes ...
func (c *Client) TruncateBytes(key string, size uint32) error {
	try(sendMessage(c.conn, truncateBytesCmd))
	try(sendBytes(c.conn, []byte(key)))
	try(sendUint32(c.conn, size))
	reply, err := readMessage(c.conn)
	check(err)
	switch reply {
	case errNoKeyReply:
		return ErrKeyNotFound
	case ackReply:
		return nil
	default:
		panic(fmt.Errorf("Server error: %s", string(reply)))
	}
}

// SetUint ...
func (c *Client) SetUint(key string, val uint32) {
	try(sendMessage(c.conn, setUintCmd))
	try(sendBytes(c.conn, []byte(key)))
	try(sendUint32(c.conn, val))
	reply, err := readMessage(c.conn)
	check(err)
	switch reply {
	case ackReply:
		return
	default:
		panic(fmt.Errorf("Server error: %s", string(reply)))
	}
}

// GetUint ...
func (c *Client) GetUint(key string) (uint32, error) {
	try(sendMessage(c.conn, getUintCmd))
	try(sendBytes(c.conn, []byte(key)))
	reply, err := readMessage(c.conn)
	check(err)
	switch reply {
	case errNoKeyReply:
		return 0, ErrKeyNotFound
	case ackReply:
		val, err := readUint32(c.conn)
		check(err)
		return val, nil
	default:
		panic(fmt.Errorf("Server error: %s", string(reply)))
	}
}

// SetUintIfMax ...
func (c *Client) SetUintIfMax(key string, val uint32) {
	try(sendMessage(c.conn, setUintIfMaxCmd))
	try(sendBytes(c.conn, []byte(key)))
	try(sendUint32(c.conn, val))
	reply, err := readMessage(c.conn)
	check(err)
	switch reply {
	case ackReply:
		return
	default:
		panic(fmt.Errorf("Server error: %s", string(reply)))
	}
}

// DelUint ...
func (c *Client) DelUint(key string) error {
	try(sendMessage(c.conn, delUintCmd))
	try(sendBytes(c.conn, []byte(key)))
	reply, err := readMessage(c.conn)
	check(err)
	switch reply {
	case errNoKeyReply:
		return ErrKeyNotFound
	case ackReply:
		return nil
	default:
		panic(fmt.Errorf("Server error: %s", string(reply)))
	}
}
