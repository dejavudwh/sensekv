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

/* BytesToU32Slice converts the given byte slice to uint32 slice */
func BytesToU32Slice(b []byte) []uint32 {
	if len(b) == 0 {
		return nil
	}
	var u32s []uint32
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&u32s))
	hdr.Len = len(b) / 4
	hdr.Cap = hdr.Len
	hdr.Data = uintptr(unsafe.Pointer(&b[0]))
	return u32s
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
