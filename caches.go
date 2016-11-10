// Copyright (C) 2016  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package gohbase

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	"github.com/cannium/gohbase/region"
	"github.com/cznic/b"
)

type clientCache struct {
	lock *sync.RWMutex
	// maps connection string(host:port) -> region client
	clients map[string]*region.Client
}

func newClientCache() *clientCache {
	return &clientCache{
		lock:    new(sync.RWMutex),
		clients: make(map[string]*region.Client),
	}
}

func (c *clientCache) get(host string, port uint16) *region.Client {
	connection := fmt.Sprintf("%s:%d", host, port)
	c.lock.RLock()
	client, hit := c.clients[connection]
	c.lock.RUnlock()
	if hit {
		return client
	} else {
		return nil
	}
}

func (c *clientCache) put(host string, port uint16, client *region.Client) {
	connection := fmt.Sprintf("%s:%d", host, port)
	c.lock.Lock()
	defer c.lock.Unlock()
	c.clients[connection] = client
}

func (c *clientCache) del(host string, port uint16) {
	connection := fmt.Sprintf("%s:%d", host, port)
	c.lock.Lock()
	defer c.lock.Unlock()
	delete(c.clients, connection)
}

func (c *clientCache) closeAll() {
	c.lock.Lock()
	defer c.lock.Unlock()
	for _, client := range c.clients {
		client.ClientDown()
	}
}

type regionCache struct {
	lock *sync.Mutex
	// maps table -> start key -> region
	// type: string -> []byte -> *region.Region
	regions map[string]*b.Tree
	// table lock protects corresponding tree in this cache
	tableLocks map[string]*sync.Mutex

	// `clients` caches region clients, excluding meta region client
	// and admin region client
	clients *clientCache
}

func newRegionCache() *regionCache {
	return &regionCache{
		lock:       new(sync.Mutex),
		regions:    make(map[string]*b.Tree),
		tableLocks: make(map[string]*sync.Mutex),
		clients:    newClientCache(),
	}
}

// Check if the B+ tree for specific table exists, if not, create the tree
// with a sentinel region inside
// Note the sentinel regions have empty name and they should be set before use
func (c *regionCache) getTree(table []byte) *b.Tree {
	c.lock.Lock()
	defer c.lock.Unlock()

	tree, ok := c.regions[string(table)]
	if ok {
		return tree
	}

	tree = b.TreeNew(region.GenericCompare)
	r := region.NewRegion(table, nil, []byte(""), []byte(""))
	tree.Set([]byte(""), r)
	c.regions[string(table)] = tree
	c.tableLocks[string(table)] = new(sync.Mutex)
	return tree
}

// return a working region for (table, key) pair
func (c *regionCache) get(ctx context.Context, table, key []byte,
	onUnavailable func(context.Context) (*region.Region, string, uint16, error)) (*region.Region, error) {

	tree := c.getTree(table)
	tableLock := c.tableLocks[string(table)]

	tableLock.Lock()
	enumerator, _ := tree.Seek(key)
	// get current item and move the enumerator to next; we don't care
	// about next, though
	_, v, err := enumerator.Next()
	enumerator.Close()
	if err != nil { // surely not io.EOF because we have a sentinel region
		tableLock.Unlock()
		return nil, err
	}

	cachedRegion := v.(*region.Region)
	if cachedRegion.Available() {
		tableLock.Unlock()
		return cachedRegion, nil
	}

	// cachedRegion unavailable, find it from HBase
	fetchedRegion, host, port, err := onUnavailable(ctx)
	if err != nil {
		tableLock.Unlock()
		return nil, err
	}
	if rangeEqual(cachedRegion, fetchedRegion) {
		tableLock.Unlock()
		cachedRegion.SetName(fetchedRegion.Name())
		c.establishRegion(ctx, cachedRegion, host, port)
		return cachedRegion, nil
	}

	// find the full key interval affected by fetchedRegion, remove those regions,
	// insert fetchedRegion and other necessary regions
	var affectedStart, affectedStop []byte
	enumerator, _ = tree.Seek(fetchedRegion.StartKey())
	_, v, err = enumerator.Next()
	for err == nil {
		r := v.(*region.Region)
		if !isRegionOverlap(fetchedRegion, r) {
			break
		}

		if affectedStart == nil || bytes.Compare(r.StartKey(), affectedStart) < 0 {
			affectedStart = r.StartKey()
		}
		// []byte("") is the smallest for bytes.Compare, but it means +inf
		// for stop keys
		if affectedStop == nil || bytes.Compare(affectedStop, r.StopKey()) < 0 ||
			len(r.StopKey()) == 0 {
			affectedStop = r.StopKey()
		}
		markRegionUnavailable(r)
		tree.Delete(r.StartKey())

		_, v, err = enumerator.Next()
	}
	enumerator.Close()
	if !bytes.Equal(affectedStart, fetchedRegion.StartKey()) {
		newRegion := region.NewRegion(table, nil, affectedStart, fetchedRegion.StartKey())
		tree.Set(newRegion.StartKey(), newRegion)
	}
	if !bytes.Equal(affectedStop, fetchedRegion.StopKey()) {
		newRegion := region.NewRegion(table, nil, fetchedRegion.StopKey(), affectedStop)
		tree.Set(newRegion.StartKey(), newRegion)
	}
	tree.Set(fetchedRegion.StartKey(), fetchedRegion)
	tableLock.Unlock()

	c.establishRegion(ctx, fetchedRegion, host, port)
	return fetchedRegion, nil
}

// Mark region as unavailable, maintain both Region and Client
func markRegionUnavailable(r *region.Region) {
	client := r.MarkUnavailable()
	if client != nil {
		client.RemoveRegion(r)
	}
}

func (c *regionCache) establishRegion(ctx context.Context, r *region.Region,
	host string, port uint16) {

	// get client from cache
	client := c.clients.get(host, port)
	if client != nil {
		r.SetClient(client)
		client.AddRegion(r)
		return
	}

	// create new client if client not exists yet
	client = r.Connect(ctx, host, port)
	if client != nil {
		client.AddRegion(r)
		c.clients.put(host, port, client)
	}
}

func (c *regionCache) close() {
	c.clients.closeAll()
}

func rangeEqual(A, B *region.Region) bool {
	return bytes.Equal(A.StartKey(), B.StartKey()) &&
		bytes.Equal(A.StopKey(), B.StopKey())
}

func isRegionOverlap(regA, regB *region.Region) bool {
	// []byte("") is the smallest for bytes.Compare, but it means +inf
	// for stop keys
	return (bytes.Compare(regA.StartKey(), regB.StopKey()) < 0 ||
		len(regB.StopKey()) == 0) &&
		(bytes.Compare(regA.StopKey(), regB.StartKey()) > 0 ||
			len(regA.StopKey()) == 0)
}
