package cache

import (
	"container/list"
	xxhash "github.com/cespare/xxhash/v2"
	"sync"
	"unsafe"
)

type Cache struct {
	m sync.RWMutex
	// 应对稀疏流量
	lru *windowLRU
	// 保存真正的热点数据
	slru *segmentedLRU
	// 看门作用，对于只访问一次的流量，将其拒之门外
	door *BloomFilter
	// 大概的频率统计
	c *cmSketch
	// 一共的访问次数
	t int32
	// 阈值作用，如果访问次数达到了一定的值，对所有的次数进行减半，进行保鲜机制
	threshold int32
	// 使用一个大的map来进行存储，不区分lru和slru
	data map[uint64]*list.Element
}

type Options struct {
	lruPct uint8
}

// size是总容量
func NewCache(size int) *Cache {
	const lruPct = 1
	// 这里是w-lru的容量
	lruSz := (lruPct * size) / 100

	if lruSz < 1 {
		lruSz = 1
	}
	// slru的容量
	slruSz := int(float64(size) * ((100 - lruPct) / 100.0))

	if slruSz < 1 {
		slruSz = 1
	}
	// 链表1的容量
	slruO := int(0.2 * float64(slruSz))

	if slruO < 1 {
		slruO = 1
	}

	data := make(map[uint64]*list.Element, size)

	return &Cache{
		lru:  newWindowLRU(lruSz, data),
		slru: newSLRU(data, slruO, slruSz-slruO),
		// 设置假阳性率0。01
		door: newFilter(size, 0.01),
		c:    newCmSketch(int64(size)),
		data: data,
	}

}

func (c *Cache) Set(key interface{}, value interface{}) bool {
	c.m.Lock()
	defer c.m.Unlock()
	return c.set(key, value)
}

// 如何把key和value进行存储
func (c *Cache) set(key, value interface{}) bool {
	// 得到两个hash值，一个是key的hash，另一个是冲突的hash
	keyHash, conflictHash := c.keyToHash(key)

	// 要保存的一个item
	i := storeItem{
		stage:    0,
		key:      keyHash,
		conflict: conflictHash,
		value:    value,
	}

	// 分别是淘汰出来的数据，和是否有数据淘汰
	eitem, evicted := c.lru.add(i)

	// 如果没有淘汰数据的话，直接返回
	if !evicted {
		return true
	}
	// 如果有淘汰的数据，从slru中拿出一个数据
	victim := c.slru.victim()
	// 如果没满，不需要被淘汰，直接进行添加
	if victim == nil {
		c.slru.add(eitem)
		return true
	}
	// 如果只是访问一次的数据，直接抛弃掉，return true
	if !c.door.Allow(uint32(keyHash)) {
		return true
	}

	// 进行pk
	vcount := c.c.Estimate(victim.key)
	ocount := c.c.Estimate(eitem.key)

	if ocount < vcount {
		return true
	}

	c.slru.add(eitem)
	return true
}

func (c *Cache) Get(key interface{}) (interface{}, bool) {
	c.m.RLock()
	defer c.m.RUnlock()
	return c.get(key)
}

func (c *Cache) get(key interface{}) (interface{}, bool) {
	// 每get一次计数器就新增一次
	c.t++
	// 如果达到限度，进行保活机制
	if c.t == c.threshold {
		// 所有计数器减半
		c.c.Reset()
		// 布隆过滤器所有计数清0
		c.door.reset()
		c.t = 0
	}

	keyHash, conflictHash := c.keyToHash(key)

	val, ok := c.data[keyHash]
	// 如果这个值不存在
	if !ok {
		// 计数+1
		c.c.Increment(keyHash)
		return nil, false
	}

	item := val.Value.(*storeItem)

	// 这里是hash冲突的情况下，并不是真正需要的值
	if item.conflict != conflictHash {
		c.c.Increment(keyHash)
		return nil, false
	}

	// 对具体值的key也是要计数
	c.c.Increment(item.key)

	v := item.value

	if item.stage == 0 {
		c.lru.get(val)
	} else {
		c.slru.get(val)
	}

	return v, true

}

func (c *Cache) Del(key interface{}) (interface{}, bool) {
	c.m.Lock()
	defer c.m.Unlock()
	return c.del(key)
}

func (c *Cache) del(key interface{}) (interface{}, bool) {
	keyHash, conflictHash := c.keyToHash(key)

	val, ok := c.data[keyHash]
	if !ok {
		return 0, false
	}

	item := val.Value.(*storeItem)

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

// MemHashString is the hash function used by go map, it utilizes available hardware instructions
// (behaves as aeshash if aes instruction is available).
// NOTE: The hash seed changes for every process. So, this cannot be used as a persistent hash.
func MemHashString(str string) uint64 {
	ss := (*stringStruct)(unsafe.Pointer(&str))
	return uint64(memhash(ss.str, 0, uintptr(ss.len)))
}

func MemHash(data []byte) uint64 {
	ss := (*stringStruct)(unsafe.Pointer(&data))
	return uint64(memhash(ss.str, 0, uintptr(ss.len)))
}
