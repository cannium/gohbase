// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package region

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"

	"github.com/cannium/gohbase/hrpc"
	"github.com/cannium/gohbase/internal/pb"
	"github.com/golang/protobuf/proto"
)

// ClientType is a type alias to represent the type of this region client
type ClientType string

var (
	// ErrShortWrite is used when the writer thread only succeeds in writing
	// part of its buffer to the socket, and not all of the buffer was sent
	ErrShortWrite = errors.New("Short write occurred while writing to socket")

	// ErrMissingCallID is used when HBase sends us a response message for a
	// request that we didn't send
	ErrMissingCallID = errors.New("Got a response with a nonsensical call ID")

	// ErrClientClosed is returned to rpcs when Close() is called
	ErrClientClosed = errors.New("Client closed")

	ErrRegionUnavailable = errors.New("Region is unavailable")

	// javaRetryableExceptions is a map where all Java exceptions that signify
	// the RPC should be sent again are listed (as keys). If a Java exception
	// listed here is returned by HBase, the client should attempt to resend
	// the RPC message, potentially via a different region client.
	javaRetryableExceptions = map[string]struct{}{
		"org.apache.hadoop.hbase.NotServingRegionException":                 struct{}{},
		"org.apache.hadoop.hbase.exceptions.RegionMovedException":           struct{}{},
		"org.apache.hadoop.hbase.exceptions.RegionOpeningException":         struct{}{},
		"org.apache.hadoop.hbase.regionserver.RegionServerStoppedException": struct{}{},
		"org.apache.hadoop.hbase.UnknownScannerException":                   struct{}{},
	}

	// queue size of region client
	QUEUE_SIZE = 100
	// timeout before flushing the queue in region client
	FLUSH_INTERVAL = 20 * time.Millisecond
)

const (
	// RegionClient is a ClientType that means this will be a normal client
	RegionClient = ClientType("ClientService")

	// MasterClient is a ClientType that means this client will talk to the
	// master server
	MasterClient = ClientType("MasterService")
)

// UnrecoverableError is an error that this region.Client can't recover from.
// The connection to the RegionServer has to be closed and all queued and
// outstanding RPCs will be failed / retried.
type UnrecoverableError struct {
	error
}

func (e UnrecoverableError) Error() string {
	return e.error.Error()
}

// RetryableError is an error that indicates the RPC should be retried because
// the error is transient (e.g. a region being momentarily unavailable).
type RetryableError struct {
	error
}

func (e RetryableError) Error() string {
	return e.error.Error()
}

// client manages a connection to a RegionServer.
type Client struct {
	conn io.ReadWriteCloser

	// Hostname or IP address of the RegionServer.
	host string

	// Port of the RegionServer.
	port uint16

	// Regions this client serves
	regions     map[string]*Region
	regionsLock sync.Mutex

	// err is set once a write or read fails.
	err  error
	errM sync.RWMutex // protects err

	rpcs chan hrpc.RpcCall
	done chan struct{}

	// sent contains the mapping of sent call IDs to RPC calls, so that when
	// a response is received it can be tied to the correct RPC
	sent  map[uint32]hrpc.RpcCall
	sentM sync.Mutex // protects sent

	rpcQueueSize  int
	flushInterval time.Duration
}

type call struct {
	id uint32
	hrpc.RpcCall
}

func (c *Client) AddRegion(r *Region) {
	c.regionsLock.Lock()
	defer c.regionsLock.Unlock()
	c.regions[string(r.Table())+string(r.StartKey())] = r
}

func (c *Client) RemoveRegion(r *Region) {
	c.regionsLock.Lock()
	defer c.regionsLock.Unlock()
	delete(c.regions, string(r.Table())+string(r.StartKey()))
}

func (c *Client) ClientDown() {
	c.regionsLock.Lock()
	defer c.regionsLock.Unlock()
	for _, r := range c.regions {
		r.MarkUnavailable()
	}
	c.Close()
}

// QueueRPC will add an rpc call to the queue for processing by the writer
// goroutine
func (c *Client) QueueRPC(rpc hrpc.RpcCall) error {
	c.errM.RLock()
	err := c.err
	if err != nil {
		c.errM.RUnlock()
		return err
	}
	c.rpcs <- rpc
	c.errM.RUnlock()
	return nil
}

// Close asks this region.Client to close its connection to the RegionServer.
// All queued and outstanding RPCs, if any, will be failed as if a connection
// error had happened.
func (c *Client) Close() {
	c.fail(ErrClientClosed)
}

// Host returns the host that this client talks to
func (c *Client) Host() string {
	return c.host
}

// Port returns the port that this client talks over
func (c *Client) Port() uint16 {
	return c.port
}

func (c *Client) fail(err error) {
	c.errM.Lock()
	if c.err != nil {
		c.errM.Unlock()
		return
	}
	err = fmt.Errorf("Region client (%s:%d) error: %s", c.host, c.port, err)
	c.err = err
	c.errM.Unlock()
	log.Errorln(err)

	// tell goroutines to stop
	close(c.done)
	// close rpcs channel for consistency and to ensure nobody sends to
	// it by mistake
	close(c.rpcs)
	// we close connection to the regionserver,
	// to let it know that we can't receive anymore
	c.conn.Close()

	// fail queued rpcs
	var sent map[uint32]hrpc.RpcCall
	c.sentM.Lock()
	sent = c.sent
	c.sent = make(map[uint32]hrpc.RpcCall)
	c.sentM.Unlock()
	// send error to rpcs
	res := hrpc.RpcResult{Error: err}
	for _, rpc := range sent {
		rpc.ResultChan() <- res
	}
}

func (c *Client) processRPCs() {
	batch := make([]*call, 0, c.rpcQueueSize)
	ticker := time.NewTicker(c.flushInterval)
	var currID uint32
	defer ticker.Stop()
	for {
		select {
		case <-c.done:
			return
		case <-ticker.C:
			go c.sendBatch(batch)
			batch = make([]*call, 0, c.rpcQueueSize)
		case rpc, ok := <-c.rpcs:
			if !ok {
				return
			}
			currID++
			// track the rpc
			c.sentM.Lock()
			c.sent[currID] = rpc
			c.sentM.Unlock()
			// associate with id and append
			batch = append(batch, &call{
				id:      currID,
				RpcCall: rpc,
			})
			if len(batch) == c.rpcQueueSize {
				go c.sendBatch(batch)
				batch = make([]*call, 0, c.rpcQueueSize)
			}
		}
	}
}

func (c *Client) sendBatch(rpcs []*call) {
	for _, rpc := range rpcs {
		select {
		case <-c.done:
			return
		case _, ok := <-rpc.Context().Done():
			// If the deadline has been exceeded, don't bother sending the
			// request. The function that placed the RPC in our queue should
			// stop waiting for a result and return an error.
			if !ok {
				continue
			}
		default:
			err := c.send(rpc)
			if _, ok := err.(UnrecoverableError); ok {
				c.ClientDown()
				c.fail(err)
				return
			}
		}
	}
}

func (c *Client) receiveRPCs() {
	for {
		select {
		case <-c.done:
			return
		default:
			if err := c.receive(); err != nil {
				c.fail(err)
				return
			}
		}
	}
}

func (c *Client) receive() error {
	var sz [4]byte
	err := c.readFully(sz[:])
	if err != nil {
		return err
	}

	buf := make([]byte, binary.BigEndian.Uint32(sz[:]))
	err = c.readFully(buf)
	if err != nil {
		return err
	}

	resp := &pb.ResponseHeader{}
	respLen, nb := proto.DecodeVarint(buf)
	buf = buf[nb:]
	err = proto.UnmarshalMerge(buf[:respLen], resp)
	buf = buf[respLen:]
	if err != nil {
		// Failed to deserialize the response header
		return err
	}
	if resp.CallId == nil {
		// Response doesn't have a call ID
		return ErrMissingCallID
	}

	c.sentM.Lock()
	rpc, ok := c.sent[*resp.CallId]
	if !ok {
		c.sentM.Unlock()
		return fmt.Errorf("Got a response with an unexpected call ID: %d", *resp.CallId)
	}
	delete(c.sent, *resp.CallId)
	c.sentM.Unlock()

	var rpcResp proto.Message
	if resp.Exception == nil {
		respLen, nb = proto.DecodeVarint(buf)
		buf = buf[nb:]
		rpcResp = rpc.NewResponse()
		err = proto.UnmarshalMerge(buf, rpcResp)
	} else {
		javaClass := *resp.Exception.ExceptionClassName
		err = fmt.Errorf("HBase Java exception %s: \n%s", javaClass, *resp.Exception.StackTrace)
		if _, ok := javaRetryableExceptions[javaClass]; ok {
			// This is a recoverable error. The client should retry.
			err = RetryableError{err}
		}
	}
	rpc.ResultChan() <- hrpc.RpcResult{Msg: rpcResp, Error: err}
	return nil
}

// write sends the given buffer to the RegionServer.
func (c *Client) write(buf []byte) error {
	n, err := c.conn.Write(buf)
	if err != nil {
		// There was an error while writing
		return err
	}
	if n != len(buf) {
		// We failed to write the entire buffer
		// TODO: Perhaps handle this in another way than closing down
		return ErrShortWrite
	}
	return nil
}

// Tries to read enough data to fully fill up the given buffer.
func (c *Client) readFully(buf []byte) error {
	_, err := io.ReadFull(c.conn, buf)
	if err != nil {
		return fmt.Errorf("Failed to read from the RS: %s", err)
	}
	return nil
}

// sendHello sends the "hello" message needed when opening a new connection.
func (c *Client) sendHello(ctype ClientType) error {
	connHeader := &pb.ConnectionHeader{
		UserInfo: &pb.UserInformation{
			EffectiveUser: proto.String("gopher"),
		},
		ServiceName: proto.String(string(ctype)),
		//CellBlockCodecClass: "org.apache.hadoop.hbase.codec.KeyValueCodec",
	}
	data, err := proto.Marshal(connHeader)
	if err != nil {
		return fmt.Errorf("Failed to marshal connection header: %s", err)
	}

	const header = "HBas\x00\x50" // \x50 = Simple Auth.
	buf := make([]byte, 0, len(header)+4+len(data))
	buf = append(buf, header...)
	buf = buf[:len(header)+4]
	binary.BigEndian.PutUint32(buf[6:], uint32(len(data)))
	buf = append(buf, data...)
	return c.write(buf)
}

// send sends an RPC out to the wire.
// Returns the response (for now, as the call is synchronous).
func (c *Client) send(rpc *call) error {
	reqheader := &pb.RequestHeader{
		CallId:       &rpc.id,
		MethodName:   proto.String(rpc.Name()),
		RequestParam: proto.Bool(true),
	}

	payload, err := rpc.Serialize()
	if err != nil {
		return fmt.Errorf("Failed to serialize RPC: %s", err)
	}
	payloadLen := proto.EncodeVarint(uint64(len(payload)))

	headerData, err := proto.Marshal(reqheader)
	if err != nil {
		return fmt.Errorf("Failed to marshal Get request: %s", err)
	}

	buf := make([]byte, 5, 4+1+len(headerData)+len(payloadLen)+len(payload))
	binary.BigEndian.PutUint32(buf, uint32(cap(buf)-4))
	buf[4] = byte(len(headerData))
	buf = append(buf, headerData...)
	buf = append(buf, payloadLen...)
	buf = append(buf, payload...)

	err = c.write(buf)
	if err != nil {
		return UnrecoverableError{err}
	}
	return nil
}
