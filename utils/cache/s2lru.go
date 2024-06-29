package cache

import (
	"container/list"
)

type segmentedLRU struct {
	// map用于存储
	data map[uint64]*list.Element
	// 两个链表的容量限制
	// probation(20%) 和 protected(80%)
	StageOneCap, StageTwoCap int
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
		StageOneCap: stageOneCap,
		StageTwoCap: stageTwoCap,
		stageOne:    list.New(),
		stageTwo:    list.New(),
	}
}

func (slru *segmentedLRU) add(newitem storeItem) {
	// 新数据都是probation
	newitem.stage = 1
	// 如果stageOne没满
	if slru.stageOne.Len() < slru.StageOneCap || slru.Len() < slru.StageTwoCap+slru.StageOneCap {
		slru.data[newitem.key] = slru.stageOne.PushBack(&newitem)
		return
	}

	//如果stageOne满了
	e := slru.stageOne.Back()
	item := e.Value.(*storeItem)

	delete(slru.data, item.key)
	*item = newitem

	slru.data[item.key] = e
	slru.stageOne.MoveToFront(e)
}

// movetofront是只改变节点在链表中的位置
// pushfront是将数据包装成新节点冰插入链表
func (slru *segmentedLRU) get(v *list.Element) {
	item := v.Value.(*storeItem)
	// 如果在protected区，直接放到链表的最前面
	if item.stage == STAGE_TWO {
		slru.stageTwo.MoveToFront(v)
		return
	}

	// 如果在probation区，而且protected区没有满
	if slru.stageTwo.Len() < slru.StageTwoCap {
		// 从probation移除
		slru.stageOne.Remove(v)
		// 加入protected
		item.stage = STAGE_TWO
		slru.data[item.key] = slru.stageTwo.PushFront(item)
		return
	}

	// 如果在probation中，而且protected已经满了
	// 这时候和进行元素交换
	back := slru.stageTwo.Back()
	bitem := back.Value.(*storeItem)

	// 交换
	*bitem, *item = *item, *bitem
	bitem.stage = STAGE_TWO
	item.stage = STAGE_ONE

	// 更新映射，item指向v，bitem指向back
	slru.data[item.key] = v
	slru.data[bitem.key] = back

	// 因为两个都用到了，所以两个链表中都需要把这个元素提到最前
	slru.stageOne.MoveToFront(v)
	slru.stageTwo.MoveToFront(back)
}
func (slru *segmentedLRU) Len() int {
	return slru.stageTwo.Len() + slru.stageOne.Len()
}

func (slru *segmentedLRU) victim() *storeItem {
	if slru.Len() < slru.StageOneCap+slru.StageTwoCap {
		return nil
	}

	v := slru.stageOne.Back()
	return v.Value.(*storeItem)
}
