/*
 * @Author: dejavudwh
 * @Date: 2022-07-10 09:24:47
 * @LastEditTime: 2022-07-10 09:30:58
 */
package utils

import "encoding/binary"

func BytesToU32(b []byte) uint32 {
	return binary.BigEndian.Uint32(b)
}

func BytesToU64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}
