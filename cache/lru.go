/*
 * @Author: dejavudwh
 * @Date: 2022-07-07 11:18:25
 * @LastEditTime: 2022-07-07 11:57:24
 */
package cache

import (
	"container/list"
	"fmt"
)

/*
	As the gate to the entire cache, the execution is performed according to the LRU policy
	and the eliminated elements are handed over to the next operation
*/
type windowLRU struct {
	data map[uint64]*list.Element
	cap  int
	list *list.List
}

type storeItem struct {
	stage    int
	key      uint64
	conflict uint64
	value    interface{}
}

func newWindowLRU(size int, data map[uint64]*list.Element) *windowLRU {
	return &windowLRU{
		data: data,
		cap:  size,
		list: list.New(),
	}
}

/*
	Adds an element to the LRU, returning an empty storeItem and false if the addition is successful
	and the original element is not eliminated
*/
func (lru *windowLRU) add(newitem storeItem) (eitem storeItem, evicted bool) {
	// If the window cap is not full, insert it directly into the
	if lru.list.Len() < lru.cap {
		lru.data[newitem.key] = lru.list.PushFront(&newitem)
		return storeItem{}, false
	}

	// If the widow cap is full, follow the lru rule to eliminate it from the tail
	evictItem := lru.list.Back()
	item := evictItem.Value.(*storeItem)
	delete(lru.data, item.key)

	// Assign values to evictItem and *item directly to avoid reapplying space to the runtime
	// item = evictItem = netitem
	eitem, *item = *item, newitem
	lru.data[item.key] = evictItem
	lru.list.MoveToFront(evictItem)
	return eitem, true
}

func (lru *windowLRU) get(v *list.Element) {
	lru.list.MoveToFront(v)
}

/* for debug */
func (lru *windowLRU) String() string {
	var s string
	for e := lru.list.Front(); e != nil; e = e.Next() {
		s += fmt.Sprintf("%v,", e.Value.(*storeItem).value)
	}
	return s
}
