/*
 * @Author: dejavudwh
 * @Date: 2022-07-07 04:28:01
 * @LastEditTime: 2022-07-11 04:22:35
 */
package db

type Iterator interface {
	Next()
	Valid() bool
	Rewind()
	Item() Item
	Close() error
	Seek(key []byte)
}

type Item interface {
	Entry() *Entry
}

type Options struct {
	Prefix []byte
	IsAsc  bool
}
