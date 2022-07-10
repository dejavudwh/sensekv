/*
 * @Author: dejavudwh
 * @Date: 2022-07-10 05:36:36
 * @LastEditTime: 2022-07-10 06:25:44
 */
package mmap

import "os"

func MMap(fd *os.File, writable bool, size int64) ([]byte, error) {
	return mmap(fd, writable, size)
}

func Munmap(b []byte) error {
	return munmap(b)
}

func Mremap(data []byte, size int) ([]byte, error) {
	return mremap(data, size)
}

func Madvise(b []byte, readahead bool) error {
	return madvise(b, readahead)
}

func Msync(b []byte) error {
	return msync(b)
}
