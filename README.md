# kvdroid

**This project is experimental and not actively maintained.**

kvdroid is a simple experimental distributed key-value store in memory. It's primary purpose is to store large byte arrays chunked between multiple kvdroid-server instances and allow (userspace) zero-copy reads in client. 

Features:
- type-safe client API (value types are []byte and Uint32 only)
- clients ring API with consistent hashing

Missing features:
- client pool
- high level API to manage byte array chunking 

## Server set up

Clone the repo and build the server:
```
$ git clone https://github.com/JCapul/kvdroid
$ cd kvdroid
$ make
```
This will produce the executables ```kvdroid-server``` and ```kvdroid-stop``` in the build/bin directory.

To launch the server on localhost and default port (8001):
```
$ build/bin/kvdroid-server
```
Launch a daemonized server:
```
$ build/bin/kvdroid-server -daemonize
```
To stop the server:
```
$ build/bin/kvdroid-stop
```

## Client API basics

Get the go package:
```
$ go get github.com/JCapul/kvdroid
```

Create a client:
```golang
package main

import "github.com/JCapul/kvdroid"

func main() {
    kvdroid := kvdroid.NewClient([]byte(":8001")
```

Use ```SetBytes``` and ```GetBytes``` to store bytes.
```
    kvdroid.SetBytes("foo", []byte("bar"))
    b, err := kvdroid.GetBytes("foo")
    ...
```
Use ```GetBytesInto``` to read bytes directly into a user-defined byte slice, the call returns the number of bytes read:
```
    dst := make([]byte, 20, 20)
    n, err = client.GetBytesInto("foo", recv)
```

A set of API calls handle range of bytes similar to Redis SETRANGE/GETRANGE commands: ```SetBytesRange```, ```GetBytesRange```, ```SetBytesRangeInto```.
