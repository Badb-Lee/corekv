package cache

import "container/list"

type segmentedLRU struct {
	// map用于存储
	data map[uint64]*list.Element
	// 两个链表的容量限制
	// probation（20%） 和 protected（80%）两个区域
	stageOneCap, stageTwoCap int
	// 两个链表
	stageOne, stageTwo *list.List
}

const (
	STAGE_ONE = iota
	STAGE_TWO
)

func newSLRU(data map[uint64]*list.Element, stageOneCap, stageTwoCap int) *segmentedLRU {
	return &segmentedLRU{
		data:        data,
		stageOneCap: stageOneCap,
		stageTwoCap: stageTwoCap,
		stageOne:    list.New(),
		stageTwo:    list.New(),
	}
}

func (slru *segmentedLRU) add(newitem storeItem) {
	// 新数据都是probation
	newitem.stage = 1

	if slru.stageOne.Len() < slru.stageOneCap || slru.Len() < slru.stageOneCap+slru.stageTwoCap {
		slru.data[newitem.key] = slru.stageOne.PushFront(&newitem)
		return
	}

	// 在链表probation区域进行淘汰
	e := slru.stageOne.Back()
	item := e.Value.(*storeItem)

	delete(slru.data, item.key)

	*item = newitem

	slru.data[item.key] = e
	slru.stageOne.MoveToFront(e)
}

func (slru *segmentedLRU) get(v *list.Element) {
	item := v.Value.(*storeItem)

	// 如果在protected区，直接放到链表的最前面
	if item.stage == STAGE_TWO {
		slru.stageTwo.MoveToFront(v)
		return
	}

	// 如果链表1当中，而且现在链表2没有满
	if slru.stageTwo.Len() < slru.stageTwoCap {
		// 从链表1中移除
		slru.stageOne.Remove(v)
		// 改为链表2
		item.stage = STAGE_TWO
		slru.data[item.key] = slru.stageTwo.PushFront(item)
		return
	}

	// 如果在链表1中，而且链表2已经满了，在链表2中进行淘汰
	// 将链表2的元素和链表1的元素进行交换
	back := slru.stageTwo.Back()
	bitem := back.Value.(*storeItem)

	*bitem, *item = *item, *bitem

	bitem.stage = STAGE_TWO
	item.stage = STAGE_ONE

	slru.data[item.key] = v
	slru.data[bitem.key] = back

	slru.stageOne.MoveToFront(v)
	slru.stageTwo.MoveToFront(back)
}

func (slru *segmentedLRU) Len() int {
	return slru.stageTwo.Len() + slru.stageOne.Len()
}

func (slru *segmentedLRU) victim() *storeItem {
	if slru.Len() < slru.stageOneCap+slru.stageTwoCap {
		return nil
	}

	v := slru.stageOne.Back()
	return v.Value.(*storeItem)
}
