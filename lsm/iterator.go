package lsm

import (
	"sensekv/db"
)

type Iterator struct {
	it    Item
	iters []db.Iterator
}
type Item struct {
	e *db.Entry
}

func (it *Item) Entry() *db.Entry {
	return it.e
}
