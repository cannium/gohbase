// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package gohbase

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/cznic/b"
	"github.com/tsuna/gohbase/hrpc"
	"github.com/tsuna/gohbase/internal/pb"
	"github.com/tsuna/gohbase/internal/zk"
	"github.com/tsuna/gohbase/region"
	"golang.org/x/net/context"
)

const (
	standardClient = iota
	adminClient
)

// Client a regular HBase client
type Client interface {
	Scan(s *hrpc.Scan) ([]*hrpc.Result, error)
	Get(g *hrpc.Get) (*hrpc.Result, error)
	Put(p *hrpc.Mutate) (*hrpc.Result, error)
	Delete(d *hrpc.Mutate) (*hrpc.Result, error)
	Append(a *hrpc.Mutate) (*hrpc.Result, error)
	Increment(i *hrpc.Mutate) (int64, error)
	CheckAndPut(p *hrpc.Mutate, family string, qualifier string,
		expectedValue []byte) (bool, error)
	Close()
}

type Option func(*client)

func SetZnodeParentOption(znodeParent string) Option {
	return func(*client) {
		zk.Meta = zk.ResourceName(znodeParent + "/meta-region-server")
		zk.Master = zk.ResourceName(znodeParent + "/master")
	}
}

// A Client provides access to an HBase cluster.
type client struct {
	clientType int

	zkquorum string

	regions keyRegionCache

	// TODO: document what this protects.
	regionsLock sync.Mutex

	// Maps a hrpc.RegionInfo to the *region.Client that we think currently
	// serves it.
	clients clientRegionCache

	metaRegionInfo hrpc.RegionInfo

	adminRegionInfo hrpc.RegionInfo

	// The maximum size of the RPC queue in the region client
	rpcQueueSize int

	// zkClient is zookeeper for retrieving meta and admin information
	zkClient zk.Client

	// The timeout before flushing the RPC queue in the region client
	flushInterval time.Duration
}

// NewClient creates a new HBase client.
func NewClient(zkquorum string, options ...Option) Client {
	return newClient(zkquorum, options...)
}

func newClient(zkquorum string, options ...Option) *client {
	log.WithFields(log.Fields{
		"Host": zkquorum,
	}).Debug("Creating new client.")
	c := &client{
		clientType: standardClient,
		regions:    keyRegionCache{regions: b.TreeNew(region.CompareGeneric)},
		clients: clientRegionCache{
			regions: make(map[hrpc.RegionClient][]hrpc.RegionInfo),
		},
		zkquorum:      zkquorum,
		rpcQueueSize:  100,
		flushInterval: 20 * time.Millisecond,
		metaRegionInfo: region.NewInfo(
			[]byte("hbase:meta"),
			[]byte("hbase:meta,,1"),
			nil,
			nil),
		adminRegionInfo: region.NewInfo(
			nil,
			nil,
			nil,
			nil),
		zkClient: zk.NewClient(zkquorum),
	}
	for _, option := range options {
		option(c)
	}
	return c
}

// RpcQueueSize will return an option that will set the size of the RPC queues
// used in a given client
func RpcQueueSize(size int) Option {
	return func(c *client) {
		c.rpcQueueSize = size
	}
}

// FlushInterval will return an option that will set the timeout for flushing
// the RPC queues used in a given client
func FlushInterval(interval time.Duration) Option {
	return func(c *client) {
		c.flushInterval = interval
	}
}

// Close closes connections to hbase master and regionservers
func (c *client) Close() {
	// TODO: do we need a lock for metaRegionInfo and adminRegionInfo
	if mc := c.metaRegionInfo.Client(); mc != nil {
		mc.Close()
	}
	if ac := c.adminRegionInfo.Client(); ac != nil {
		ac.Close()
	}
	c.clients.closeAll()
}

// Scan retrieves the values specified in families from the given range.
func (c *client) Scan(s *hrpc.Scan) ([]*hrpc.Result, error) {
	var results []*pb.Result
	var scanResp *pb.ScanResponse
	ctx := s.Context()
	table := s.Table()
	families := s.Families()
	filters := s.Filter()
	startRow := s.StartRow()
	stopRow := s.StopRow()
	fromTs, toTs := s.TimeRange()
	maxVersions := s.MaxVersions()
	numberOfRows := s.NumberOfRows()

	var receivedRows uint32
	for {
		var rowsToFetch uint32
		if numberOfRows > 0 {
			rowsToFetch = numberOfRows - receivedRows
		}
		// Make a new Scan RPC for this region
		// TODO: would be nicer to clone it in some way
		rpc, err := hrpc.NewScanRange(ctx, table, startRow, stopRow,
			hrpc.Families(families), hrpc.Filters(filters),
			hrpc.TimeRangeUint64(fromTs, toTs),
			hrpc.MaxVersions(maxVersions),
			hrpc.NumberOfRows(rowsToFetch))
		if err != nil {
			return nil, err
		}

		res, err := c.sendRPC(rpc)
		if err != nil {
			return nil, err
		}
		scanResp = res.(*pb.ScanResponse)
		results = append(results, scanResp.Results...)
		receivedRows += len(scanResp.Results)

		for scanResp.GetMoreResultsInRegion() ||
			(len(scanResp.Results) > 0  && scanResp.GetMoreResults()) {

			rpc = hrpc.NewScanFromID(ctx, table, *scanResp.ScannerId, rpc.Key())

			res, err = c.sendRPC(rpc)
			if err != nil {
				return nil, err
			}
			scanResp = res.(*pb.ScanResponse)
			results = append(results, scanResp.Results...)
			receivedRows += len(scanResp.Results)
		}

		rpc = hrpc.NewCloseFromID(ctx, table, *scanResp.ScannerId, rpc.Key())
		if err != nil {
			return nil, err
		}
		res, err = c.sendRPC(rpc)
		// FIXME error would raise if this error is handled
		// if err != nil {
		// 	return nil, err
		// }

		// Check to see if this region is the last we should scan (either
		// because (1) it's the last region or (3) because its stop_key is
		// greater than or equal to the stop_key of this scanner provided
		// that (2) we're not trying to scan until the end of the table).
		// (1)
		if len(rpc.RegionStop()) == 0 ||
			// (2)                (3)
			(len(stopRow) != 0 && bytes.Compare(stopRow, rpc.RegionStop()) <= 0) ||
			(receivedRows != 0 && receivedRows >= numberOfRows) {

			// Do we want to be returning a slice of Result objects or should we just
			// put all the Cells into the same Result object?
			localResults := make([]*hrpc.Result, len(results))
			for idx, result := range results {
				localResults[idx] = hrpc.ToLocalResult(result)
			}
			return localResults, nil
		} else {
			startRow = rpc.RegionStop()
		}
	}
}

func (c *client) Get(g *hrpc.Get) (*hrpc.Result, error) {
	pbmsg, err := c.sendRPC(g)
	if err != nil {
		return nil, err
	}

	r, ok := pbmsg.(*pb.GetResponse)
	if !ok {
		return nil, fmt.Errorf("sendRPC returned not a GetResponse")
	}

	return hrpc.ToLocalResult(r.Result), nil
}

func (c *client) Put(p *hrpc.Mutate) (*hrpc.Result, error) {
	return c.mutate(p)
}

func (c *client) Delete(d *hrpc.Mutate) (*hrpc.Result, error) {
	return c.mutate(d)
}

func (c *client) Append(a *hrpc.Mutate) (*hrpc.Result, error) {
	return c.mutate(a)
}

func (c *client) Increment(i *hrpc.Mutate) (int64, error) {
	r, err := c.mutate(i)
	if err != nil {
		return 0, err
	}

	if len(r.Cells) != 1 {
		return 0, fmt.Errorf("Increment returned %d cells, but we expected exactly one.",
			len(r.Cells))
	}

	val := binary.BigEndian.Uint64(r.Cells[0].Value)
	return int64(val), nil
}

func (c *client) mutate(m *hrpc.Mutate) (*hrpc.Result, error) {
	pbmsg, err := c.sendRPC(m)
	if err != nil {
		return nil, err
	}

	r, ok := pbmsg.(*pb.MutateResponse)
	if !ok {
		return nil, fmt.Errorf("sendRPC returned not a MutateResponse")
	}

	return hrpc.ToLocalResult(r.Result), nil
}

func (c *client) CheckAndPut(p *hrpc.Mutate, family string,
	qualifier string, expectedValue []byte) (bool, error) {
	cas, err := hrpc.NewCheckAndPut(p, family, qualifier, expectedValue)
	if err != nil {
		return false, err
	}

	pbmsg, err := c.sendRPC(cas)
	if err != nil {
		return false, err
	}

	r, ok := pbmsg.(*pb.MutateResponse)
	if !ok {
		return false, fmt.Errorf("sendRPC returned a %T instead of MutateResponse", pbmsg)
	}

	if r.Processed == nil {
		return false, fmt.Errorf("Protobuf in the response didn't contain the field "+
			"indicating whether the CheckAndPut was successful or not: %s", r)
	}

	return r.GetProcessed(), nil
}

func (c *client) checkProcedureWithBackoff(pContext context.Context, procID uint64) error {
	backoff := backoffStart
	ctx, cancel := context.WithTimeout(pContext, 30*time.Second)
	defer cancel()

	for {
		req := hrpc.NewGetProcedureState(ctx, procID)
		pbmsg, err := c.sendRPC(req)
		if err != nil {
			return err
		}

		statusRes, ok := pbmsg.(*pb.GetProcedureResultResponse)
		if !ok {
			return fmt.Errorf("sendRPC returned not a GetProcedureResultResponse")
		}

		switch statusRes.GetState() {
		case pb.GetProcedureResultResponse_NOT_FOUND:
			return fmt.Errorf("Procedure not found")
		case pb.GetProcedureResultResponse_FINISHED:
			return nil
		default:
			backoff, err = sleepAndIncreaseBackoff(ctx, backoff)
			if err != nil {
				return err
			}
		}
	}
}
