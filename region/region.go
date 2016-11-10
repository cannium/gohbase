// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package region

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"strconv"
	"sync"

	"github.com/cannium/gohbase/hrpc"
	"github.com/cannium/gohbase/internal/pb"
	"github.com/golang/protobuf/proto"
)

type Region struct {
	table []byte
	// The attributes above are supposed to be immutable.

	name     []byte
	startKey []byte
	stopKey  []byte
	lock     sync.RWMutex
	client   *Client
}

// NewInfo creates a new region info
func NewRegion(table, name, startKey, stopKey []byte) *Region {
	return &Region{
		table:    table,
		name:     name,
		startKey: startKey,
		stopKey:  stopKey,
	}
}

// regionFromCell parses a KeyValue from the meta table and creates the
// corresponding Region object.
func regionFromCell(cell *hrpc.Cell) (*Region, error) {
	value := cell.Value
	if len(value) == 0 {
		return nil, fmt.Errorf("empty value in %q", cell)
	} else if value[0] != 'P' {
		return nil, fmt.Errorf("unsupported region info version %d in %q",
			value[0], cell)
	}
	const pbufMagic = 1346524486 // 4 bytes: "PBUF"
	magic := binary.BigEndian.Uint32(value)
	if magic != pbufMagic {
		return nil, fmt.Errorf("invalid magic number in %q", cell)
	}
	regInfo := &pb.RegionInfo{}
	err := proto.UnmarshalMerge(value[4:len(value)-4], regInfo)
	if err != nil {
		return nil, fmt.Errorf("failed to decode %q: %s", cell, err)
	}
	return &Region{
		table:    regInfo.TableName.Qualifier,
		name:     cell.Row,
		startKey: regInfo.StartKey,
		stopKey:  regInfo.EndKey,
	}, nil
}

// ParseRegionInfo parses the contents of a row from the meta table.
// It's guaranteed to return a region info and a host/port OR return an error.
func ParseRegionInfo(metaRow *hrpc.Result) (*Region, string, uint16, error) {
	var reg *Region
	var host string
	var port uint16

	for _, cell := range metaRow.Cells {
		switch string(cell.Qualifier) {
		case "regioninfo":
			var err error
			reg, err = regionFromCell(cell)
			if err != nil {
				return nil, "", 0, err
			}
		case "server":
			value := cell.Value
			if len(value) == 0 {
				continue // Empty during NSRE.
			}
			colon := bytes.IndexByte(value, ':')
			if colon < 1 { // Colon can't be at the beginning.
				return nil, "", 0,
					fmt.Errorf("broken meta: no colon found in info:server %q", cell)
			}
			host = string(value[:colon])
			portU64, err := strconv.ParseUint(string(value[colon+1:]), 10, 16)
			if err != nil {
				return nil, "", 0, err
			}
			port = uint16(portU64)
		default:
			// Other kinds of qualifiers: ignore them.
			// TODO: If this is the parent of a split region, there are two other
			// KVs that could be useful: `info:splitA' and `info:splitB'.
			// Need to investigate whether we can use those as a hint to update our
			// regions_cache with the daughter regions of the split.
		}
	}

	if reg == nil {
		// There was no region in the row in meta, this is really not
		// expected.
		err := fmt.Errorf("Meta seems to be broken, there was no region in %v",
			metaRow)
		return nil, "", 0, err
	} else if port == 0 { // Either both `host' and `port' are set, or both aren't.
		return nil, "", 0, fmt.Errorf("Meta doesn't have a server location in %v",
			metaRow)
	}

	return reg, host, port, nil
}

// Available returns true if this region is available
func (r *Region) Available() bool {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return r.client != nil
}

func (r *Region) MarkUnavailable() *Client {
	r.lock.Lock()
	defer r.lock.Unlock()
	client := r.client
	r.client = nil
	return client
}

func (r *Region) String() string {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return fmt.Sprintf("*region.info{Table: %q, Name: %q, StopKey: %q}",
		r.table, r.name, r.stopKey)
}

// Name returns region name
func (r *Region) Name() []byte {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return r.name
}

func (r *Region) SetName(name []byte) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.name = name
}

// StopKey return region stop key
func (r *Region) StopKey() []byte {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return r.stopKey
}

// StartKey return region start key
func (r *Region) StartKey() []byte {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return r.startKey
}

// Table returns region table
func (r *Region) Table() []byte {
	return r.table
}

func (r *Region) Client() *Client {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return r.client
}

func (r *Region) SetClient(client *Client) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.client = client
}

func (r *Region) Connect(ctx context.Context, host string, port uint16) *Client {
	r.lock.Lock()
	defer r.lock.Unlock()
	if r.client != nil {
		// another routine already setups client for this region
		return r.client
	}
	client, err := NewClient(ctx, host, port, RegionClient, QUEUE_SIZE, FLUSH_INTERVAL)
	if err != nil {
		// TODO should log specific error
		return nil
	}
	r.client = client
	return client
}

// CompareGeneric is the same thing as Compare but for interface{}.
func GenericCompare(a, b interface{}) int {
	return bytes.Compare(a.([]byte), b.([]byte))
}

// Compare compares two region names.
// We can't just use bytes.Compare() because it doesn't play nicely
// with the way META keys are built as the first region has an empty start
// key.  Let's assume we know about those 2 regions in our cache:
//   .META.,,1
//   tableA,,1273018455182
// We're given an RPC to execute on "tableA", row "\x00" (1 byte row key
// containing a 0).  If we use Compare() to sort the entries in the cache,
// when we search for the entry right before "tableA,\000,:"
// we'll erroneously find ".META.,,1" instead of the entry for first
// region of "tableA".
//
// Since this scheme breaks natural ordering, we need this comparator to
// implement a special version of comparison to handle this scenario.
func Compare(a, b []byte) int {
	var length int
	if la, lb := len(a), len(b); la < lb {
		length = la
	} else {
		length = lb
	}
	// Reminder: region names are of the form:
	//   table_name,start_key,timestamp[.MD5.]
	// First compare the table names.
	var i int
	for i = 0; i < length; i++ {
		ai := a[i]    // Saves one pointer deference every iteration.
		bi := b[i]    // Saves one pointer deference every iteration.
		if ai != bi { // The name of the tables differ.
			if ai == ',' {
				return -1001 // `a' has a smaller table name.  a < b
			} else if bi == ',' {
				return 1001 // `b' has a smaller table name.  a > b
			}
			return int(ai) - int(bi)
		}
		if ai == ',' { // Remember: at this point ai == bi.
			break // We're done comparing the table names.  They're equal.
		}
	}

	// Now find the last comma in both `a' and `b'.  We need to start the
	// search from the end as the row key could have an arbitrary number of
	// commas and we don't know its length.
	aComma := findCommaFromEnd(a, i)
	bComma := findCommaFromEnd(b, i)
	// If either `a' or `b' is followed immediately by another comma, then
	// they are the first region (it's the empty start key).
	i++ // No need to check against `length', there MUST be more bytes.

	// Compare keys.
	var firstComma int
	if aComma < bComma {
		firstComma = aComma
	} else {
		firstComma = bComma
	}
	for ; i < firstComma; i++ {
		ai := a[i]
		bi := b[i]
		if ai != bi { // The keys differ.
			return int(ai) - int(bi)
		}
	}
	if aComma < bComma {
		return -1002 // `a' has a shorter key.  a < b
	} else if bComma < aComma {
		return 1002 // `b' has a shorter key.  a > b
	}

	// Keys have the same length and have compared identical.  Compare the
	// rest, which essentially means: use start code as a tie breaker.
	for ; /*nothing*/ i < length; i++ {
		ai := a[i]
		bi := b[i]
		if ai != bi { // The start codes differ.
			return int(ai) - int(bi)
		}
	}

	return len(a) - len(b)
}

// Because there is no `LastIndexByte()' in the standard `bytes' package.
func findCommaFromEnd(b []byte, offset int) int {
	for i := len(b) - 1; i > offset; i-- {
		if b[i] == ',' {
			return i
		}
	}
	panic(fmt.Errorf("No comma found in %q after offset %d", b, offset))
}
