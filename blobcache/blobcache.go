package blobcache

import (
	"appengine"
	"appengine/memcache"
	"bytes"
	"fmt"
	"hash/crc64"
)

var (
	crc64Table = crc64.MakeTable(crc64.ECMA)
)

const (
	// See https://developers.google.com/appengine/docs/go/memcache/#Go_Limits
	MAX_CHUNK_SIZE = uint64(900 * 1000)
)

func Set(c appengine.Context, item *memcache.Item) (err error) {
	checksum := crc64.Checksum(item.Value, crc64Table)
	chunkItem := *item
	valueSize := uint64(len(item.Value))
	for i := uint64(0); i < valueSize; i += MAX_CHUNK_SIZE {
		chunkItem.Key = fmt.Sprintf("%016X%016X%s", checksum, i, item.Key)
		iNext := i + MAX_CHUNK_SIZE
		if iNext > valueSize {
			iNext = valueSize
		}
		chunkItem.Value = item.Value[i : iNext]
		if err = memcache.Set(c, &chunkItem); err != nil {
			c.Errorf("Error storing chunkItem[%016X:%016X] in memcache under key=%s: %v", i, iNext, chunkItem.Key, err)
			return err
		}
	}
	masterItem := *item
	masterItem.Value = []byte(fmt.Sprintf("%016X%016X", checksum, valueSize))
	if err = memcache.Set(c, &masterItem); err != nil {
		c.Errorf("Error stroing masterItem in memcache under key=%s: %v", item.Key, err)
		return err
	}
	return
}

func Get(c appengine.Context, key string) (item *memcache.Item, err error) {
	masterItem, err := memcache.Get(c, key)
	if err != nil {
		if err != memcache.ErrCacheMiss {
			c.Errorf("Error obtaining master item from memcache under key=%s: %v", key, err)
		}
		return nil, err
	}
	var checksum, valueSize uint64
	n, err := fmt.Sscanf(string(masterItem.Value), "%016X%016X", &checksum, &valueSize)
	if err != nil {
		c.Errorf("Error when parsing masterItem.Value=[%s] for key=%s: %v", masterItem.Value, key, err)
		return nil, memcache.ErrCacheMiss
	}
	if n != 2 {
		c.Errorf("Unexpected number of arguments parsed in masterItem.Value for key=%s: %d. Expected 2", key, n)
		return nil, memcache.ErrCacheMiss
	}

	var chunks [][]byte
	for i := uint64(0); i < valueSize; i += MAX_CHUNK_SIZE {
		chunkKey := fmt.Sprintf("%016X%016X%s", checksum, i, key)
		chunkItem, err := memcache.Get(c, chunkKey)
		if err != nil {
			c.Errorf("Error when obtaining chunkItem[%016X:%016X] for key=%s: %v", i, i+MAX_CHUNK_SIZE, chunkKey, err)
			return nil, memcache.ErrCacheMiss
		}
		if i+MAX_CHUNK_SIZE <= valueSize && uint64(len(chunkItem.Value)) != MAX_CHUNK_SIZE {
			c.Errorf("Unexpected length for chunkItem[%016X:016X] for key=%s: %d. Expected %d", len(chunkItem.Value), MAX_CHUNK_SIZE)
			return nil, memcache.ErrCacheMiss
		} else if i+MAX_CHUNK_SIZE > valueSize && uint64(len(chunkItem.Value)) != valueSize-i {
			c.Errorf("Unexpected length for chunkItem[%016X:016X] for key=%s: %d. Expected %d", len(chunkItem.Value), valueSize-i)
			return nil, memcache.ErrCacheMiss
		}
		chunks = append(chunks, chunkItem.Value)
	}

	item = masterItem
	item.Value = bytes.Join(chunks, []byte{})
	return item, nil
}
