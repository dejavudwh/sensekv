/*
 * @Author: dejavudwh
 * @Date: 2022-07-15 11:52:42
 * @LastEditTime: 2022-07-15 11:54:14
 */
package sensekv

import (
	"sensekv/db"
	"sensekv/lsm"
	"sensekv/utils"
)

type DBIterator struct {
	iitr db.Iterator
	vlog *valueLog
}
type Item struct {
	e *db.Entry
}

func (it *Item) Entry() *db.Entry {
	return it.e
}
func (database *DB) NewIterator(opt *db.Options) db.Iterator {
	iters := make([]db.Iterator, 0)
	iters = append(iters, database.lsm.NewIterators(opt)...)

	res := &DBIterator{
		vlog: database.vlog,
		iitr: lsm.NewMergeIterator(iters, opt.IsAsc),
	}
	return res
}

func (iter *DBIterator) Next() {
	iter.iitr.Next()
	for ; iter.Valid() && iter.Item() == nil; iter.iitr.Next() {
	}
}
func (iter *DBIterator) Valid() bool {
	return iter.iitr.Valid()
}
func (iter *DBIterator) Rewind() {
	iter.iitr.Rewind()
	for ; iter.Valid() && iter.Item() == nil; iter.iitr.Next() {
	}
}
func (iter *DBIterator) Item() db.Item {
	// 检查从lsm拿到的value是否是value ptr,是则从vlog中拿值
	e := iter.iitr.Item().Entry()
	var value []byte

	if e != nil && db.IsValuePtr(e) {
		var vp db.ValuePtr
		vp.Decode(e.Value)
		result, cb, err := iter.vlog.read(&vp)
		defer db.RunCallback(cb)
		if err != nil {
			return nil
		}
		value = utils.SafeCopy(nil, result)
	}

	if e.IsDeletedOrExpired() || value == nil {
		return nil
	}

	res := &db.Entry{
		Key:          e.Key,
		Value:        value,
		ExpiresAt:    e.ExpiresAt,
		Meta:         e.Meta,
		Version:      e.Version,
		Offset:       e.Offset,
		Hlen:         e.Hlen,
		ValThreshold: e.ValThreshold,
	}
	return res
}
func (iter *DBIterator) Close() error {
	return iter.iitr.Close()
}
func (iter *DBIterator) Seek(key []byte) {
}
