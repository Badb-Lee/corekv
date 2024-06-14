package utils

import (
	"corekv/utils/codec"
	"math"
	"sync/atomic"
	_ "unsafe"
)

/**
跳表，实现索引的数据结构
*/

const (
	defaultMaxHeight = 48
	maxHeight        = 20
	heightIncrease   = math.MaxUint32 / 3
)

type node struct {
	// 将offset和length合并
	value     uint64
	keyOffset uint32
	keySize   uint16
	// 所处的层级，代表了这个节点有几个next指针
	height uint16

	// 该node的next指针数组，默认初始化最大高度是maxHeight
	// 实际上不需要占用全部的maxHeight
	tower [maxHeight]uint32
}

type Element struct {
	// levels代表的是该节点在第i个level所指的节点
	levels []*Element
	entry  *codec.Entry
	score  float64
}

type SkipList struct {
	// 当前高度
	// 使用cas来保证原子化的替换，避免使用锁
	height int32
	// 头节点
	headOffset uint32
	// 引用计数，用于追踪一个对象被引用的次数，当引用计数达到0的时候，可以安全的释放该内存占用的内存
	ref int32
	// 内存池
	arena   *Arena
	OnClose func()
}

func (s *SkipList) IncrRef() {
	atomic.AddInt32(&s.ref, 1)
}

// DecrRef 如果引用为0，回收跳表
func (s *SkipList) DecrRef() {
	newRef := atomic.AddInt32(&s.ref, -1)
	if newRef > 0 {
		return
	}

	if s.OnClose != nil {
		s.OnClose()
	}

	s.arena = nil
}

func newNode(arena *Arena, key []byte, v ValueStruct, height int) *node {
	nodeOffset := arena.putNode(height)
	keyOffset := arena.putKey(key)
	// 将offset和size合并为一个val
	val := encodeValue(arena.putVal(v), v.EncodedSize())

	node := arena.getNode(nodeOffset)
	node.height = uint16(height)
	node.value = val
	node.keyOffset = keyOffset
	node.keySize = uint16(len(key))
	return node
}

func encodeValue(valOffset uint32, valSize uint32) uint64 {
	// 前32位是size，后32位是offset
	return uint64(valSize)<<32 | uint64(valOffset)
}

func decodeValue(value uint64) (valOffset uint32, valSize uint32) {
	// 也就是只保留后面的n位
	valOffset = uint32(value)
	valSize = uint32(value >> 32)
	return valOffset, valSize
}

func NewSkipList(arenaSize int64) *SkipList {
	// 申请一块内存池大小
	arena := newArena(arenaSize)
	// 这里是空的头节点，保证head拥有最大高度
	head := newNode(arena, nil, ValueStruct{}, maxHeight)
	ho := arena.getNodeOffset(head)
	return &SkipList{
		height:     1,
		headOffset: ho,
		arena:      arena,
		ref:        1,
	}
}

// 获取value的偏移量
func (s *node) getValueOffset() (uint32, uint32) {
	value := atomic.LoadUint64(&s.value)
	return decodeValue(value)
}

// 获取key
func (s *node) key(arena *Arena) []byte {
	return arena.getKey(s.keyOffset, s.keySize)
}

// 设置node的value
func (s *node) setValue(vo uint64) {
	atomic.StoreUint64(&s.value, vo)
}

// 下一个节点的偏移量
func (s *node) getNexOffset(h int) uint32 {
	return atomic.LoadUint32(&s.tower[h])
}

// 设置一个新的next指针
func (s *node) casNextOffset(h int, old, val uint32) bool {
	return atomic.CompareAndSwapUint32(&s.tower[h], old, val)
}

// 随机高度
func (s *SkipList) randomHeight() int {
	h := 1
	for h < maxHeight && FastRand() <= heightIncrease {
		h++
	}

	return h
}

// 某个节点某一层的下一个节点
func (s *SkipList) getNext(n *node, height int) *node {
	return s.arena.getNode(n.getNexOffset(height))
}

// 获取头节点
func (s *SkipList) getHead() *node {
	return s.arena.getNode(s.headOffset)
}

//go:linkname FastRand runtime.fastrand
func FastRand() uint32

//func (sl *SkipList) Close() error {
//	return nil
//}
//
//// Search 查找
//func (sl *SkipList) Search(key []byte) *codec.Entry {
//	sl.lock.Lock()
//	defer sl.lock.Unlock()
//	// 为什么需要计算？
//	// 原因如下：
//	// 1、统一的比较标准，可以将键换成各种类型，比如字符串、数字、map等
//	// 2、有时候需要根据自定义规则进行排序
//	// 3、减少比较次数，如果提供有效的比较机制，使用整数而非字符串，那么会提高性能
//	// 4、复用代码，比较的地方有很多，直接提出来方便使用
//	keyScore := sl.calcScore(key)
//	header, maxLevel := sl.header, sl.maxLevel
//	prev := header
//
//	// 和插入过程是一样的
//	for i := maxLevel; i >= 0; i-- {
//		for cur := prev.levels[i]; cur != nil; cur = prev.levels[i] {
//			if comp := sl.compare(keyScore, key, cur); comp <= 0 {
//				if comp == 0 {
//					return cur.entry
//				} else {
//					prev = cur
//				}
//			} else {
//				break
//			}
//		}
//	}
//
//	return nil
//}
//
//func (sl *SkipList) PrintElement() {
//	sl.lock.Lock()
//	defer sl.lock.Unlock()
//
//	header, maxLevel := sl.header, sl.maxLevel
//	prev := header
//
//	for i := maxLevel; i >= 0; i-- {
//		// 当前层的第一个
//		curLevel := prev.levels[i]
//		for cur := curLevel; cur != nil; cur = curLevel.levels[i] {
//			fmt.Print(cur.entry.Key, "--")
//			curLevel = cur.levels[i]
//
//		}
//		fmt.Println()
//	}
//}
//
//// Add 插入
//func (sl *SkipList) Add(entry *codec.Entry) error {
//	sl.lock.Lock()
//	defer sl.lock.Unlock()
//
//	prevs := make([]*Element, defaultMaxHeight+1)
//	keyScore := sl.calcScore(entry.Key)
//	header, maxLevel := sl.header, sl.maxLevel
//	prev := header
//	// 寻找过程
//	// 1、首先从最高的level开始
//	// 2、其次从每一层level的第一个元素开始
//	for i := maxLevel; i >= 0; i-- {
//		for cur := prev.levels[i]; cur != nil; cur = prev.levels[i] {
//			// 升序排列
//			if comp := sl.compare(keyScore, entry.Key, cur); comp <= 0 {
//				if comp == 0 {
//					// 说明插入元素的key存在列表当中，直接进行更新
//					cur.entry = entry
//					return nil
//					// 要插入的元素比当前元素大，继续往后查找
//				} else {
//					prev = cur
//				}
//				// 要插入的元素比当前元素小，进入下一行
//			} else {
//				break
//			}
//		}
//		// 存这一行中指向自己的元素
//		prevs[i] = prev
//	}
//
//	// 计算要插入几层level
//	randLevel := sl.randLevel()
//	newe := newElement(keyScore, entry, randLevel)
//	for i := randLevel; i >= 0; i-- {
//		nexte := prevs[i].levels[i]
//		prevs[i].levels[i] = newe
//		newe.levels[i] = nexte
//	}
//
//	return nil
//}
//
//func NewSkipList() *SkipList {
//	header := &Element{
//		levels: make([]*Element, defaultMaxHeight),
//	}
//
//	return &SkipList{
//		header:   header,
//		maxLevel: defaultMaxHeight - 1,
//		rand:     r,
//	}
//}
//
//func newElement(score float64, entry *codec.Entry, level int) *Element {
//	return &Element{
//		levels: make([]*Element, level+1),
//		entry:  entry,
//		score:  score,
//	}
//}
//
//func (elem *Element) Entry() *codec.Entry {
//	return elem.entry
//}
//
//func (list *SkipList) calcScore(key []byte) float64 {
//	var hash uint64
//	l := len(key)
//
//	if l > 8 {
//		l = 8
//	}
//
//	for i := 0; i < l; i++ {
//		shift := uint64(64 - 8 - i*8)
//		hash |= uint64(key[i]) << shift
//	}
//
//	return float64(hash)
//}
//
//func (sl *SkipList) compare(score float64, key []byte, cur *Element) int {
//	if score == cur.score {
//		return bytes.Compare(key, cur.entry.Key)
//	}
//
//	if score < cur.score {
//		return -1
//	} else {
//		return 1
//	}
//}
//
//func (sl *SkipList) randLevel() int {
//	for i := 0; i < sl.maxLevel; i++ {
//		if sl.rand.Intn(2) == 0 {
//			return i
//		}
//	}
//
//	return sl.maxLevel
//}
