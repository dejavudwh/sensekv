/*
 * @Author: dejavudwh
 * @Date: 2022-07-07 14:04:44
 * @LastEditTime: 2022-07-08 03:19:22
 */
package cache

import (
	"container/list"
	"sync"
	"unsafe"

	"github.com/cespare/xxhash/v2"
)

/*
	For data that is accessed only once,
	it is quickly eliminated in the LRU and does not occupy cache space.
	For bursty sparse traffic,
	that is, data that may be accessed frequently in a short period of time, the Windows-LRU can fit this access model very well
	For really hot data,
	it will soon go from Window-lRU to Protected area and will survive through the preservation mechanism
*/
type Cache struct {
	mtx       sync.RWMutex
	lru       *windowLRU
	slru      *segmentedLRU
	door      *BloomFilter
	sketch    *cmSketch
	total     int32 // Total number of elements visited
	threshold int32
	data      map[uint64]*list.Element
}

func NewCache(size int) *Cache {
	// window LRU memory usage ratio
	const lruPct = 1
	lruSz := (lruPct * size) / 100

	if lruSz < 1 {
		lruSz = 1
	}

	// Calculating the cache capacity of the LFU section
	slruSz := int(float64(size) * ((100 - lruPct) / 100.0))
	// the stageOne part accounts for 20%.
	slruOneSz := int(0.2 * float64(slruSz))

	if slruOneSz < 1 {
		slruOneSz = 1
	}

	data := make(map[uint64]*list.Element, size)

	return &Cache{
		lru:    newWindowLRU(lruSz, data),
		slru:   newSLRU(data, slruOneSz, slruSz-slruOneSz),
		door:   newFilter(size, 0.01),
		sketch: newCmSketch(int64(size)),
		data:   data,
	}
}

func (c *Cache) Set(key, value interface{}) bool {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	return c.set(key, value)
}

func (c *Cache) set(key, value interface{}) bool {
	keyHash, conflictHash := c.keyToHash(key)
	i := storeItem{
		stage:    0,
		key:      keyHash,
		conflict: conflictHash,
		value:    value,
	}

	// If the window is full, return the data that was eliminated
	eitem, evicted := c.lru.add(i)
	if !evicted {
		return true
	}

	// If there are eliminated data in the window,
	// it will go here, you need to find an eliminator from the stageOne part of LFU,
	// and compare the two
	victim := c.slru.victim()
	// LFU is not full, then window lru's elimination data, can enter stageOne
	if victim == nil {
		c.slru.add(eitem)
		return true
	}

	// Must have appeared once in bloomfilter to allow comparison,
	// otherwise it means it appears too infrequently
	if !c.door.AllowAndRecord(uint32(eitem.key)) {
		return true
	}
	// Those who visit more frequently are considered more qualified to stay
	vcount := c.sketch.Estimate(victim.key)
	ocount := c.sketch.Estimate(eitem.key)

	if ocount < vcount {
		return true
	}

	// into stageOne
	c.slru.add(eitem)
	return true
}

func (c *Cache) Get(key interface{}) (interface{}, bool) {
	c.mtx.RLock()
	defer c.mtx.RUnlock()
	return c.get(key)
}

func (c *Cache) get(key interface{}) (interface{}, bool) {
	c.total++
	// Reset to prevent old data from staying too long
	if c.total == c.threshold {
		c.sketch.Reset()
		c.door.reset()
		c.total = 0
	}

	keyHash, conflictHash := c.keyToHash(key)
	val, ok := c.data[keyHash]
	if !ok {
		// No cache found, and counting
		c.door.AllowAndRecord(uint32(keyHash))
		c.sketch.Increment(keyHash)
		return nil, false
	}

	item := val.Value.(*storeItem)
	// Because two hash values are set at the same time before setting the data,
	// so if the confilctHash is not equal, it means that a hash collision of key values has occurred
	if item.conflict != conflictHash {
		c.door.AllowAndRecord(uint32(keyHash))
		c.sketch.Increment(keyHash)
		return nil, false
	}

	c.door.AllowAndRecord(uint32(keyHash))
	c.sketch.Increment(item.key)

	v := item.value

	// found
	if item.stage == 0 {
		c.lru.get(val)
	} else {
		c.slru.get(val)
	}

	return v, true
}

func (c *Cache) Del(key interface{}) (interface{}, bool) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	return c.del(key)
}

func (c *Cache) del(key interface{}) (interface{}, bool) {
	keyHash, conflictHash := c.keyToHash(key)
	// Not found, sb
	val, ok := c.data[keyHash]
	if !ok {
		return 0, false
	}

	item := val.Value.(*storeItem)

	//
	if conflictHash != 0 && (conflictHash != item.conflict) {
		return 0, false
	}

	delete(c.data, keyHash)
	return item.conflict, true
}

func (c *Cache) keyToHash(key interface{}) (uint64, uint64) {
	if key == nil {
		return 0, 0
	}
	switch k := key.(type) {
	case uint64:
		return k, 0
	case string:
		return MemHashString(k), xxhash.Sum64String(k)
	case []byte:
		return MemHash(k), xxhash.Sum64(k)
	case byte:
		return uint64(k), 0
	case int:
		return uint64(k), 0
	case int32:
		return uint64(k), 0
	case uint32:
		return uint64(k), 0
	case int64:
		return uint64(k), 0
	default:
		panic("Key type not supported")
	}
}

type stringStruct struct {
	str unsafe.Pointer
	len int
}

//go:noescape
//go:linkname memhash runtime.memhash
func memhash(p unsafe.Pointer, h, s uintptr) uintptr

/*
	MemHashString is the hash function used by go map, it utilizes available hardware instructions
	(behaves as aeshash if aes instruction is available).
	NOTE: The hash seed changes for every process. So, this cannot be used as a persistent hash.
*/
func MemHashString(str string) uint64 {
	ss := (*stringStruct)(unsafe.Pointer(&str))
	return uint64(memhash(ss.str, 0, uintptr(ss.len)))
}

func MemHash(data []byte) uint64 {
	ss := (*stringStruct)(unsafe.Pointer(&data))
	return uint64(memhash(ss.str, 0, uintptr(ss.len)))
}

func (c *Cache) String() string {
	var s string
	s += c.lru.String() + " | " + c.slru.String()
	return s
}
