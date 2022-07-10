/*
 * @Author: dejavudwh
 * @Date: 2022-07-10 09:24:47
 * @LastEditTime: 2022-07-10 12:02:57
 */
package utils

import (
	"encoding/binary"
	"reflect"
	"unsafe"
)

/* BytesToU32 converts the given byte slice to uint32 */
func BytesToU32(b []byte) uint32 {
	return binary.BigEndian.Uint32(b)
}

/* BytesToU64 converts the given byte slice to uint64 */
func BytesToU64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

/* U32SliceToBytes converts the given Uint32 slice to byte slice */
func U32SliceToBytes(u32s []uint32) []byte {
	if len(u32s) == 0 {
		return nil
	}
	var b []byte
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	hdr.Len = len(u32s) * 4
	hdr.Cap = hdr.Len
	hdr.Data = uintptr(unsafe.Pointer(&u32s[0]))
	return b
}

/* U32ToBytes converts the given Uint32 to bytes */
func U32ToBytes(v uint32) []byte {
	var uBuf [4]byte
	binary.BigEndian.PutUint32(uBuf[:], v)
	return uBuf[:]
}

/* U64ToBytes converts the given Uint64 to bytes */
func U64ToBytes(v uint64) []byte {
	var uBuf [8]byte
	binary.BigEndian.PutUint64(uBuf[:], v)
	return uBuf[:]
}
