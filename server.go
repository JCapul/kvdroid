package kvdroid

import (
	"fmt"
	"io"
	"log"
	"net"
	"sync"
)

// Bucket stores data
type Bucket struct {
	bytedata map[string][]byte
	uintdata map[string]uint32
	mtx      *sync.RWMutex
}

// Store manages requests and buckets
type Store struct {
	buckets  map[string]*Bucket
	hash     *ConsistentHash
	stopChan chan bool
}

// NewStore ...
func NewStore(n int, stopChan chan bool) *Store {
	hash := NewConsistentHash(100, nil)
	buckets := make(map[string]*Bucket)
	for i := 0; i <= n; i++ {
		name := fmt.Sprintf("%d", i)
		buckets[name] = &Bucket{
			bytedata: make(map[string][]byte),
			uintdata: make(map[string]uint32),
			mtx:      &sync.RWMutex{},
		}
		hash.Add(name)
	}
	return &Store{
		buckets:  buckets,
		hash:     hash,
		stopChan: stopChan,
	}
}

func (s *Store) getBucket(key string) *Bucket {
	hash := s.hash.Get(key)
	return s.buckets[hash]
}

func (s *Store) handleRequest(conn net.Conn, wg *sync.WaitGroup) {
	defer wg.Done()
	defer conn.Close()
	for {
		cmd, err := readMessage(conn)
		if err == io.EOF {
			log.Printf("Connection closed by client %v", conn.RemoteAddr())
			return
		}
		check(err)

		if cmd == stopCmd {
			try(sendMessage(conn, ackReply))
			s.stopChan <- true
			return
		}

		key, err := readString(conn)
		check(err)

		bucket := s.getBucket(key)

		switch cmd {
		case getBytesCmd:
			s.GetBytes(bucket, key, conn)
		case getBytesIntoCmd:
			s.GetBytesInto(bucket, key, conn)
		case getBytesRangeCmd:
			s.GetBytesRange(bucket, key, conn)
		case getBytesRangeIntoCmd:
			s.GetBytesRangeInto(bucket, key, conn)
		case setBytesCmd:
			s.SetBytes(bucket, key, conn)
		case setBytesRangeCmd:
			s.SetBytesRange(bucket, key, conn)
		case delBytesCmd:
			s.DelBytes(bucket, key, conn)
		case truncateBytesCmd:
			s.TruncateBytes(bucket, key, conn)
		case setUintCmd:
			s.SetUint(bucket, key, conn)
		case getUintCmd:
			s.GetUint(bucket, key, conn)
		case delUintCmd:
			s.DelUint(bucket, key, conn)
		case setUintIfMaxCmd:
			s.SetUintIfMax(bucket, key, conn)
		default:
			panic(fmt.Errorf("Unknown command: %s", string(cmd)))
		}
	}
}

/* Store Protocol */

// GetBytes ...
func (s *Store) GetBytes(bucket *Bucket, key string, conn net.Conn) {
	bucket.mtx.RLock()
	defer bucket.mtx.RUnlock()
	data, ok := bucket.bytedata[key]
	if !ok {
		try(sendMessage(conn, errNoKeyReply))
	} else {
		try(sendMessage(conn, ackReply))
		try(sendBytes(conn, data))
	}
}

// Get could be merged into GetInto with dstSize=-1 (at the cost of an extra uint32 sent)

// GetBytesInto ...
func (s *Store) GetBytesInto(bucket *Bucket, key string, conn net.Conn) {
	bucket.mtx.RLock()
	defer bucket.mtx.RUnlock()
	dstSize, err := readUint32(conn)
	check(err)
	data, ok := bucket.bytedata[key]
	if !ok {
		try(sendMessage(conn, errNoKeyReply))
	} else {
		try(sendMessage(conn, ackReply))
		if dstSize < uint32(len(data)) {
			try(sendBytes(conn, data[:dstSize]))
		} else {
			try(sendBytes(conn, data))
		}
	}
}

// GetBytesRange ...
func (s *Store) GetBytesRange(bucket *Bucket, key string, conn net.Conn) {
	bucket.mtx.RLock()
	defer bucket.mtx.RUnlock()
	start, err := readUint32(conn)
	check(err)
	end, err := readUint32(conn)
	check(err)
	end++ // client API considers end to be last item including (like Redis)
	data, ok := bucket.bytedata[key]
	if !ok {
		try(sendMessage(conn, errNoKeyReply))
	} else {
		try(sendMessage(conn, ackReply))
		actualSize := uint32(len(data))
		if start > actualSize {
			try(sendUint32(conn, uint32(0)))
		} else {
			if end > actualSize {
				// truncate range to actual size
				end = actualSize
			}
			try(sendBytes(conn, data[start:end]))
		}
	}
}

// GetRange could be merged into GetRangeInto with dstSize=-1 (at the cost of an extra uint32 sent)

// GetBytesRangeInto ...
func (s *Store) GetBytesRangeInto(bucket *Bucket, key string, conn net.Conn) {
	bucket.mtx.RLock()
	defer bucket.mtx.RUnlock()
	start, err := readUint32(conn)
	check(err)
	end, err := readUint32(conn)
	check(err)
	end++ // client API considers end to be last item including (like Redis)
	dstSize, err := readUint32(conn)
	check(err)
	data, ok := bucket.bytedata[key]
	if !ok {
		try(sendMessage(conn, errNoKeyReply))
	} else {
		try(sendMessage(conn, ackReply))
		actualSize := uint32(len(data))
		if start > actualSize {
			try(sendUint32(conn, uint32(0)))
		} else {
			if end > actualSize {
				// truncate range to actual size
				end = actualSize
			}
			if (end - start) > dstSize {
				// truncate range to fit in dstSize
				end = start + dstSize
			}
			try(sendBytes(conn, data[start:end]))
		}
	}
}

// SetBytes ...
func (s *Store) SetBytes(bucket *Bucket, key string, conn net.Conn) {
	bucket.mtx.Lock()
	defer bucket.mtx.Unlock()
	data, err := readBytes(conn)
	check(err)
	bucket.bytedata[key] = data
	try(sendMessage(conn, ackReply))
}

// SetBytesRange ...
func (s *Store) SetBytesRange(bucket *Bucket, key string, conn net.Conn) {
	bucket.mtx.Lock()
	defer bucket.mtx.Unlock()
	start, err := readUint32(conn)
	check(err)
	newSize, err := readUint32(conn)
	check(err)
	actualData, ok := bucket.bytedata[key]
	if !ok {
		buf := make([]byte, start+newSize, start+newSize)
		try(readFillBuf(conn, buf[start:start+newSize]))
		bucket.bytedata[key] = buf
		try(sendMessage(conn, ackReply))
	} else {
		actualSize := uint32(len(actualData))
		if start+newSize <= actualSize {
			// range is within existing array
			try(readFillBuf(conn, actualData[start:start+newSize]))
		} else {
			// range is beyond existing array
			if start < actualSize {
				// range start within existing array
				try(readFillBuf(conn, actualData[start:actualSize]))
				newSize = newSize - (actualSize - start)
				start = actualSize
			}
			// get and append extended array
			extendSize := start + newSize - actualSize
			extendData := make([]byte, extendSize, extendSize)
			try(readFillBuf(conn, extendData[extendSize-newSize:extendSize]))
			bucket.bytedata[key] = append(actualData, extendData...)
		}
		try(sendMessage(conn, ackReply))
	}
}

// DelBytes ...
func (s *Store) DelBytes(bucket *Bucket, key string, conn net.Conn) {
	bucket.mtx.Lock()
	defer bucket.mtx.Unlock()
	_, ok := bucket.bytedata[key]
	if !ok {
		try(sendMessage(conn, errNoKeyReply))
	} else {
		delete(bucket.bytedata, key)
		try(sendMessage(conn, ackReply))
	}
}

//FIXME: add an unlink command similar to Redis unlink (delete in goroutine)

// TruncateBytes ...
func (s *Store) TruncateBytes(bucket *Bucket, key string, conn net.Conn) {
	bucket.mtx.Lock()
	defer bucket.mtx.Unlock()
	size, err := readUint32(conn)
	check(err)
	data, ok := bucket.bytedata[key]
	if !ok {
		try(sendMessage(conn, errNoKeyReply))
	} else {
		try(sendMessage(conn, ackReply))
		if size < uint32(len(data)) {
			bucket.bytedata[key] = data[:size]
		}
	}
}

// SetUint ...
func (s *Store) SetUint(bucket *Bucket, key string, conn net.Conn) {
	bucket.mtx.Lock()
	defer bucket.mtx.Unlock()
	val, err := readUint32(conn)
	check(err)
	bucket.uintdata[key] = val
	try(sendMessage(conn, ackReply))
}

// GetUint ...
func (s *Store) GetUint(bucket *Bucket, key string, conn net.Conn) {
	bucket.mtx.RLock()
	defer bucket.mtx.RUnlock()
	val, ok := bucket.uintdata[key]
	if !ok {
		try(sendMessage(conn, errNoKeyReply))
	} else {
		try(sendMessage(conn, ackReply))
		try(sendUint32(conn, val))
	}
}

// SetUintIfMax ...
func (s *Store) SetUintIfMax(bucket *Bucket, key string, conn net.Conn) {
	bucket.mtx.Lock()
	defer bucket.mtx.Unlock()
	val, err := readUint32(conn)
	check(err)
	actualVal, ok := bucket.uintdata[key]
	if !ok {
		bucket.uintdata[key] = val
	} else {
		if val > actualVal {
			bucket.uintdata[key] = val
		}
	}
	try(sendMessage(conn, ackReply))
}

// DelUint ...
func (s *Store) DelUint(bucket *Bucket, key string, conn net.Conn) {
	bucket.mtx.Lock()
	defer bucket.mtx.Unlock()
	_, ok := bucket.uintdata[key]
	if !ok {
		try(sendMessage(conn, errNoKeyReply))
	} else {
		delete(bucket.uintdata, key)
		try(sendMessage(conn, ackReply))
	}
}

// Server ...
type Server struct {
	opt      *ServerOptions
	addr     string
	listener net.Listener
	store    *Store
	stop     chan bool
}

// ServerOptions ...
type ServerOptions struct {
	Bind    string
	Port    int
	Buckets int
}

func (o *ServerOptions) normalize() {
	if o.Bind == "" {
		// string zero-value, default to localhost
		o.Bind = "127.0.0.1"
	}
	if o.Bind == "*" {
		// listen on all interfaces
		o.Bind = ""
	}
	if o.Port == 0 {
		o.Port = 8001
	}
	if o.Port == -1 {
		// let the Listener select a port automatically
		o.Port = 0
	}
	if o.Buckets == 0 {
		o.Buckets = 20
	}
}

// NewServer ...
func NewServer(opt *ServerOptions) *Server {
	opt.normalize()
	addr := fmt.Sprintf("%s:%d", opt.Bind, opt.Port)
	l, err := net.Listen("tcp", addr)
	check(err)
	stopChan := make(chan bool, 1)
	return &Server{
		opt:      opt,
		addr:     l.Addr().String(),
		listener: l,
		store:    NewStore(opt.Buckets, stopChan),
		stop:     stopChan,
	}
}

// Start ...
func (s *Server) Start() {
	log.Printf("kvdroid: start listening on %s", s.addr)
	wg := sync.WaitGroup{}

loop:
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.stop:
				break loop
			default:
				panic(err)
			}
		}
		wg.Add(1)
		go s.store.handleRequest(conn, &wg)
		if <-s.stop {
			break loop
		}
	}
	wg.Wait()
	log.Print("kvdroid: stop listening")
}

// Addr ...
func (s Server) Addr() string {
	return s.addr
}

// Shutdown ...
func (s *Server) Shutdown() {
	s.stop <- true
	s.listener.Close()
}
