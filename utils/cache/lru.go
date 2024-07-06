package cache

import "container/list"

type windowLRU struct {
	// 值类型是*list.Element，键类型是uint64
	data map[uint64]*list.Element
	// 容量大小
	cap int
	// 链表
	list *list.List
}

type storeItem struct {
	// 标记第几个阶段
	stage int
	// key
	key uint64
	// 当key出现冲突的时候进行辅助判断
	conflict uint64
	// value
	value interface{}
}

func newWindowLRU(size int, data map[uint64]*list.Element) *windowLRU {
	return &windowLRU{
		data: data,
		cap:  size,
		list: list.New(),
	}
}

func (lru *windowLRU) add(newitem storeItem) (eitem storeItem, evicted bool) {
	if lru.list.Len() < lru.cap {
		lru.data[newitem.key] = lru.list.PushFront(&newitem)
		return storeItem{}, false
	}

	// 返回该链表的最后一个元素
	evictItem := lru.list.Back()
	// 转换为对应数据结构
	item := evictItem.Value.(*storeItem)

	// 在map中删除key
	delete(lru.data, item.key)

	eitem, *item = *item, newitem

	// 新元素加入链表
	lru.data[item.key] = evictItem
	// 新元素移到头部
	lru.list.MoveToFront(evictItem)
	return eitem, true
}

func (lru *windowLRU) get(v *list.Element) {
	// 直接移到头部
	lru.list.MoveToFront(v)
}
