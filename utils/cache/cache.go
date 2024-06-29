package cache

import (
	"container/list"
	xxhash "github.com/cespare/xxhash/v2"
	"sync"
	"unsafe"
)

type Cache struct {
	m sync.RWMutex
	// windowLRU，主要是应对稀疏流量
	lru *windowLRU
	// 保存真正的热点数据
	slru *segmentedLRU
	// 看门作用，对于只访问一次的流量，将其拒之门外
	door *BloomFilter
	// 大概的次数统计
	c *cmSketch
	// 一共访问多少次
	t int32
	// 阈值作用，如果访问次数到达一定的数值，对所有的次数减半，进行保鲜机制
	threshold int32
	// 使用一个大的map来进行存储，不区分lru和slru
	data map[uint64]*list.Element
}

type Options struct {
	lruPct uint8
}

func NewCache(size int) *Cache {
	// 计算windowLRU的容量
	lruSz := size / 100

	if lruSz < 1 {
		lruSz = 1
	}

	// 计算slru的容量
	slruSz := int(float64(size) * (99 / 100.0))

	if slruSz < 1 {
		slruSz = 1
	}

	// 链表1的容量
	slru0 := int(0.2 * float64(slruSz))

	if slru0 < 1 {
		slru0 = 1
	}

	data := make(map[uint64]*list.Element, size)
	return &Cache{
		lru:  newWindowLRU(lruSz, data),
		slru: newSLRU(data, slru0, slruSz-slru0),
		// 设置元素个数和假阳性率
		door: newFilter(size, 0.01),
		c:    newCMSketch(int64(size)),
		data: data,
	}

}

func (c *Cache) Set(key interface{}, value interface{}) bool {
	c.m.Lock()
	defer c.m.Unlock()
	return c.set(key, value)
}

func (c *Cache) set(key interface{}, value interface{}) bool {
	// 得到两个hash值，一个是key的hash，另一个是冲突的hash
	keyHash, conflictHash := c.keyToHash(key)

	// 要保存的一个item
	i := storeItem{
		stage:    0,
		key:      keyHash,
		conflict: conflictHash,
		val:      value,
	}
	// 首先向windows-lru中添加数据
	// 返回的内容为淘汰的数据和是否有数据被淘汰
	eitem, evicted := c.lru.add(i)

	//如果没有数据淘汰的话,说明直接在lru中添加成功了
	if !evicted {
		return true
	}

	// 如果有数据淘汰，放到slru当中
	// 如果有数据淘汰，从slru中拿出一个数据
	victim := c.slru.victim()

	// 如果没满，直接放到probation区
	if victim == nil {
		c.slru.add(eitem)
		return true
	}

	// 如果只是访问一次的数据，直接抛弃掉，return true
	// 也就是说这种情况针对的是已经满了的
	if !c.door.Allow(uint32(keyHash)) {
		return true
	}

	// 如果这个probation区也满了，数据也不是第一次访问了
	// 这个时候需要将此数据和probation区的最后一个数据进行pk
	vcount := c.c.Estimate(victim.key)
	ocount := c.c.Estimate(eitem.key)

	// 如果要插入进来的数小于probation的最小值，说明插入失败，直接进行返回
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
	// 每get一次，计数计就新增一次
	c.t++
	// 如果达到限度，进行保活机制
	if c.t >= c.threshold {
		// 所有计数减半
		c.c.Reset()
		// 布隆过滤器所有计数清0
		c.door.reset()
		// 计数器清0
		c.t = 0
	}

	keyHash, conflictHash := c.keyToHash(key)
	val, ok := c.data[keyHash]
	// 不存在
	if !ok {
		// 不存在也进行+1，是为了bloom filter放行之后方便pk
		c.c.Increment(keyHash)
		return nil, false
	}

	item := val.Value.(*storeItem)

	// 这里是hash冲突的情况，并不是真正需要的值
	if item.conflict != conflictHash {
		c.c.Increment(keyHash)
		return nil, false
	}

	// 对具体值key也需要计数
	c.c.Increment(keyHash)

	v := item.val

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
	if conflictHash != 0 && item.conflict != conflictHash {
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

func MemHashString(str string) uint64 {
	ss := (*stringStruct)(unsafe.Pointer(&str))
	return uint64(memhash(ss.str, 0, uintptr(ss.len)))
}

func MemHash(data []byte) uint64 {
	ss := (*stringStruct)(unsafe.Pointer(&data))
	return uint64(memhash(ss.str, 0, uintptr(ss.len)))
}
