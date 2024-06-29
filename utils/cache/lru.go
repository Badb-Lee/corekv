package cache

import "container/list"

type windowLRU struct {
	data map[uint64]*list.Element
	// 容量大小
	cap int
	// 链表
	list *list.List
}

type storeItem struct {
	key uint64
	val interface{}
	// 用于标记第几个阶段
	stage int
	// 用于hash冲突的辅助判断
	conflict uint64
}

func newWindowLRU(size int, data map[uint64]*list.Element) *windowLRU {
	return &windowLRU{
		data: data,
		cap:  size,
		list: list.New(),
	}
}

func (lru *windowLRU) add(newitem storeItem) (eitem storeItem, evitced bool) {
	if lru.list.Len() < lru.cap {
		lru.data[newitem.key] = lru.list.PushFront(&newitem)
		return storeItem{}, false
	}

	// 如果满了
	// 返回链表的最后一个元素
	evictItem := lru.list.Back()
	// 转换为对应的数据结构
	item := evictItem.Value.(*storeItem)
	// 在map中删除key
	delete(lru.data, item.key)

	// 将最后一个元素赋值给eitem
	// 将newitem赋值给*item
	// 这个函数需要返回被淘汰的元素，所以需要将item赋值给eitem
	eitem, *item = *item, newitem

	//新元素加入链表
	//item是evictItem.Value的指针
	lru.data[item.key] = evictItem
	// 移到头部
	lru.list.MoveToFront(evictItem)
	return eitem, true

}

func (lru *windowLRU) get(v *list.Element) {
	// 直接移到头部
	lru.list.MoveToFront(v)
}
