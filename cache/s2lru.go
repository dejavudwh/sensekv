/*
 * @Author: dejavudwh
 * @Date: 2022-07-07 12:00:25
 * @LastEditTime: 2022-07-07 14:20:02
 */
package cache

import (
	"container/list"
	"fmt"
)

const (
	STAGEONE = iota + 1
	STAGETWO
)

/*
	Segment-lru divides the cache into two parts, one occupies 20% called Probation (probation),
	and the other occupies 80% (Protected).
	When the data needs to enter the Segment-LRU, it will enter the Probation area first,
	and when the data is accessed again in the Probation area, it will enter the Protected area.
	If the data in the Protected area is full, the last data will be retired.
*/
type segmentedLRU struct {
	data                     map[uint64]*list.Element
	stageOneCap, stageTwoCap int
	stageOne, stageTwo       *list.List
}

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
	newitem.stage = STAGEONE
	if slru.stageOne.Len() < slru.stageOneCap || slru.Len() < slru.stageOneCap+slru.stageTwoCap {
		slru.data[newitem.key] = slru.stageOne.PushFront(&newitem)
		return
	}

	// This means that StageOne is full, or the whole LFU is full, so it is time to eliminate data from StageOne
	e := slru.stageOne.Back()
	item := e.Value.(*storeItem)
	delete(slru.data, item.key)

	// item = e = newitem
	*item = newitem
	slru.data[item.key] = e
	slru.stageOne.MoveToFront(e)
}

func (slru *segmentedLRU) get(val *list.Element) {
	item := val.Value.(*storeItem)

	// If you are accessing cached data that is already in StageTwo, simply advance it according to the LRU rules
	if item.stage == STAGETWO {
		slru.stageOne.MoveToFront(val)
		return
	}
	// If the accessed data is still in StageOne, it will need to be elevated to StageTwo if it is accessed again
	if slru.stageTwo.Len() < slru.stageTwoCap {
		slru.stageOne.Remove(val)
		item.stage = STAGETWO
		slru.data[item.key] = slru.stageTwo.PushFront(item)
		return
	}

	// New data added to StageTwo, need to eliminate the old data,
	// StageTwo in the eliminated data will not disappear,
	// will enter the StageOne, StageOne, less frequently accessed data, may be eliminated
	back := slru.stageTwo.Back()
	bitem := back.Value.(*storeItem)

	*bitem, *item = *item, *bitem

	bitem.stage = STAGETWO
	// into StageOne
	item.stage = STAGEONE

	slru.data[item.key] = val
	slru.data[bitem.key] = back

	slru.stageOne.MoveToFront(val)
	slru.stageTwo.MoveToFront(back)
}

/* Returns the item that may be eliminated in the segmentedLRU in the next step */
func (slru *segmentedLRU) victim() *storeItem {
	if slru.Len() < slru.stageOneCap+slru.stageTwoCap {
		return nil
	}

	// If it's already full, you need to eliminate data from the 20% area,
	// here just take the last element directly from the tail
	v := slru.stageOne.Back()
	return v.Value.(*storeItem)
}

func (slru *segmentedLRU) Len() int {
	return slru.stageTwo.Len() + slru.stageOne.Len()
}

func (slru *segmentedLRU) String() string {
	var s string
	for e := slru.stageTwo.Front(); e != nil; e = e.Next() {
		s += fmt.Sprintf("%v,", e.Value.(*storeItem).value)
	}
	s += fmt.Sprintf(" | ")
	for e := slru.stageOne.Front(); e != nil; e = e.Next() {
		s += fmt.Sprintf("%v,", e.Value.(*storeItem).value)
	}
	return s
}
