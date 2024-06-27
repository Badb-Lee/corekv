package utils

import (
	"bytes"
	"corekv/utils/codec"
	"fmt"
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
	// 将offset和length合并，优点如下：
	// 1、节省内存空间
	// 2、方便原始操作
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

// 给定一个值，找到跳表中最接近该值的节点
// 大体就是，根据key来找到key左右两个值（less主要是利用了迭代器思想）
// 如果less是true，那就找key左边的值
// 如果less是false，那就找key右边的值
// 第一个返回值表示返回的节点，第二个返回值表示返回节点的key是否和要查找的key相等
// 跳表的查找需要用
func (s *SkipList) findNear(key []byte, less bool, allowEqual bool) (*node, bool) {
	x := s.getHead()
	level := int(s.getHeight() - 1)
	for {
		next := s.getNext(x, level)

		if next == nil {
			// 当前层链表已经到达尾部
			if level > 0 {
				level--
				continue
			}

			// 如果已经是level0了，切less为false
			if !less {
				return nil, false
			}

			// 如果是头节点，说明该跳表中没有任何元素
			if x == s.getHead() {
				return nil, false
			}
			return x, false
		}

		nextKey := next.key(s.arena)
		cmp := CompareKeys(key, nextKey)

		if cmp > 0 {
			x = next
			continue
		}

		if cmp == 0 {
			// 如果能够返回相等
			if allowEqual {
				return next, true
			}

			if !less {
				// height = 0 的原因是保证是最小大于key的
				return s.getNext(next, 0), false
			}

			// 想要找小的，level还>0，这个时候下面包有小的
			if level > 0 {
				level--
				continue
			}

			// 想找小的，但是level=0，而且找到头了，这个时候包没有小的
			if x == s.getHead() {
				return nil, false
			}

			return x, false
		}

		// 此时cmp < 0
		if level > 0 {
			level--
			continue
		}
		// cmp<0 而且最后一层了，返回的还是大的，直接返回这个
		if !less {
			return next, false
		}

		// 如果是链表是空，返回nil
		if x == s.getHead() {
			return nil, false
		}

		return x, false
	}
}

func (s *SkipList) getHeight() int32 {
	// 原子操作
	return atomic.LoadInt32(&s.height)
}

func CompareKeys(key1, key2 []byte) int {
	CondPanic((len(key1) <= 8 || len(key2) <= 8), fmt.Errorf("%s,%s < 8", string(key1), string(key2)))
	if cmp := bytes.Compare(key1[:len(key1)-8], key2[:len(key2)-8]); cmp != 0 {
		return cmp
	}
	return bytes.Compare(key1[len(key1)-8:], key2[len(key2)-8:])
}

// 找到要插入的位置，参数是要比较的key，前一个节点返回值是地址
// 如果有相等的key，直接原地替换
// 否则找到这样一个位置 before.key < key < after.key
// 跳表的插入需要用
func (s *SkipList) findSpliceForLevel(key []byte, before uint32, level int) (uint32, uint32) {
	// 1、原地替换
	// 2、接在末尾
	// 3、接在链表中间
	for {
		beforeNode := s.arena.getNode(before)
		nextOffset := beforeNode.getNexOffset(level)
		nextNode := s.arena.getNode(nextOffset)

		//	 说明是在链表末尾
		if nextNode == nil {
			return before, nextOffset
		}

		nextKey := nextNode.key(s.arena)
		cmp := CompareKeys(key, nextKey)

		// 如果存在想等的key
		if cmp == 0 {
			return nextOffset, nextOffset
		}

		// 如果key小于nextkey，完美插入位置
		if cmp < 0 {
			return before, nextOffset
		}

		// 如果大于，继续向后找
		before = nextOffset

	}
}

// 向跳表中新增节点
func (s *SkipList) Add(e *Entry) {
	key, v := e.Key, ValueStruct{
		Meta:      e.Meta,
		Value:     e.Value,
		ExpiresAt: e.ExpiresAt,
		Version:   e.Version,
	}

	// 当前要插入的高度
	listHeight := s.getHeight()
	// 要插入节点的每一层的前一个节点
	var prev [maxHeight + 1]uint32
	// 要插入节点的每一层的下一个节点
	var next [maxHeight + 1]uint32

	prev[listHeight] = s.headOffset

	for i := int(listHeight) - 1; i >= 0; i-- {
		// 传的是上一层，好处就是不用从头节点开始了
		prev[i], next[i] = s.findSpliceForLevel(key, prev[i+1], i)
		// 如果存在key，进行替换
		if prev[i] == next[i] {
			//先存到内存中，返回地址
			vo := s.arena.putVal(v)
			// 传入offset和size进行编码
			encValue := encodeValue(vo, v.EncodedSize())
			prevNode := s.arena.getNode(prev[i])
			// 进行该节点value的更新
			prevNode.setValue(encValue)
			return
		}
	}

	// 在获取每一层前后节点之后，就可以在每一层当中进行插入了
	// 需要插入的高度
	height := s.randomHeight()
	// 初始化要插入的节点
	x := newNode(s.arena, key, v, height)
	// 如果随机高度大于当前的高度，将当前高度替换为随机高度
	for height > int(listHeight) {
		if atomic.CompareAndSwapInt32(&s.height, listHeight, int32(height)) {
			break
		}
		listHeight = s.getHeight()
	}

	// 从第0层开始插入
	for i := 0; i < height; i++ {
		for {
			// 有这种可能吗？
			if s.arena.getNode(prev[i]) == nil {
				// 如果链表最开始为空，那么这时候pre起码是头节点
				AssertTrue(i > 1)
				prev[i], next[i] = s.findSpliceForLevel(key, s.headOffset, i)
				AssertTrue(prev[i] != next[i])
			}
			// 注意这里不需要用cas，因为这里是一个新的节点，不会有并发操作
			// cas是跟之前的值进行比较，如果这个值等于旧值才进行替换，这个是新的节点，没有旧值，不能也没办法使用cas
			x.tower[i] = next[i]
			// 但是获取prevnode的时候可能会涉及到并发操作，这里使用cas
			pnode := s.arena.getNode(prev[i])
			if pnode.casNextOffset(i, next[i], s.arena.getNodeOffset(x)) {
				break
			}

			// 对于cas失败的情况，也就是说更新的时候发现有人已经更新了
			prev[i], next[i] = s.findSpliceForLevel(key, prev[i], i)
			if prev[i] == next[i] {
				// 对于i == 0 的理解，如果两个协程一起进行插入
				// 插入的位置都是从第0层开始的，最开始发现的时候只会从第0层发现
				// 例如：A，B两个协程同时更新，A发现B已经插入过了，A在此更新值之后就跳出了，剩下的层由B继续进行更新
				// 同理，如果B发现A已经更新过了，那这个时候B在此更新这个值之后跳出，剩下的层又A进行更新
				AssertTrue(i == 0)
				vo := s.arena.putVal(v)
				encValue := encodeValue(vo, v.EncodedSize())
				prevNode := s.arena.getNode(prev[i])
				prevNode.setValue(encValue)
				return
			}

		}
	}

}

func (s *SkipList) Empty() bool {
	return s.findLast() == nil
}

func (s *SkipList) findLast() *node {
	// 核心思想就是找到最后一层的最后一个
	n := s.getHead()
	level := int(s.getHeight() - 1)
	for {
		next := s.getNext(n, level)
		if next != nil {
			n = next
			continue
		}

		if level == 0 {
			if n == s.getHead() {
				return nil
			}

			return n
		}

		level--
	}
}

func (s *SkipList) Search(key []byte) ValueStruct {
	// findNear返回的是一个node和一个布尔变量值
	n, _ := s.findNear(key, false, true)

	if n == nil {
		return ValueStruct{}
	}

	nextKey := s.arena.getKey(n.keyOffset, n.keySize)
	if !SameKey(key, nextKey) {
		return ValueStruct{}
	}

	valOffset, valSize := n.getValueOffset()
	vs := s.arena.getVal(valOffset, valSize)
	vs.ExpiresAt = ParseTs(nextKey)
	return vs

}

func (s *SkipList) NewSkipListIterator() Iterator {
	s.IncrRef()
	return &SkipListIterator{
		list: s,
	}
}

type SkipListIterator struct {
	list *SkipList
	n    *node
}

func (s SkipListIterator) Next() {
	AssertTrue(s.Valid())
	s.n = s.list.getNext(s.n, 0)
}

func (s SkipListIterator) Valid() bool {
	return s.n != nil
}

func (s SkipListIterator) Rewind() {
	s.SeekToFirst()
}

func (s SkipListIterator) Item() Item {
	return &Entry{
		Key:       s.Key(),
		Value:     s.Value().Value,
		ExpiresAt: s.Value().ExpiresAt,
		Meta:      s.Value().Meta,
		Version:   s.Value().Version,
	}
}

func (s SkipListIterator) Close() error {
	// 减少引用，如果引用为0，进行资源回收
	s.list.DecrRef()
	return nil
}

func (s SkipListIterator) Seek(key []byte) {
	panic("implement me")
}

// 定位到链表的第一个节点
func (s *SkipListIterator) SeekToFirst() {
	//implement me here
}

// Key returns the key at the current position.
func (s *SkipListIterator) Key() []byte {
	//implement me here
	return nil
}

// Value returns value.
func (s *SkipListIterator) Value() ValueStruct {
	//implement me here
	return ValueStruct{}
}

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
