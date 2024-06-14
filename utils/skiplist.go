package utils

import (
	"corekv/utils/codec"
	"math"
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
	keySize   uint32
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
	// 引用计数
	ref int32
	// 内存池
	arena   *Arena
	OnClose func()
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
