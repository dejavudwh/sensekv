/*
 * @Author: dejavudwh
 * @Date: 2022-07-10 07:48:24
 * @LastEditTime: 2022-07-10 07:48:43
 */
package file

import "io"

type Options struct {
	FID      uint64
	FileName string
	Dir      string
	Path     string
	Flag     int
	MaxSz    int
}

type SenseFile interface {
	Close() error
	Truncature(n int64) error
	ReName(name string) error
	NewReader(offset int) io.Reader
	Bytes(off, sz int) ([]byte, error)
	AllocateSlice(sz, offset int) ([]byte, int, error)
	Sync() error
	Delete() error
	Slice(offset int) []byte
}
