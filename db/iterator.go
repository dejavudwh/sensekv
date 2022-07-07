/*
 * @Author: dejavudwh
 * @Date: 2022-07-07 04:28:01
 * @LastEditTime: 2022-07-07 07:34:04
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
