/*
 * @Author: dejavudwh
 * @Date: 2022-07-07 07:35:12
 * @LastEditTime: 2022-07-13 12:38:20
 */
package utils

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
)

func CompareKeys(key1, key2 []byte) int {
	CondPanic((len(key1) <= 8 || len(key2) <= 8), fmt.Errorf("%s,%s < 8", string(key1), string(key2)))
	// len(key) >= 8
	if cmp := bytes.Compare(key1[:len(key1)-8], key2[:len(key2)-8]); cmp != 0 {
		return cmp
	}
	return bytes.Compare(key1[len(key1)-8:], key2[len(key2)-8:])
}

/* ParseTs parses the timestamp from the key bytes. */
func ParseTs(key []byte) uint64 {
	if len(key) <= 8 {
		return 0
	}
	return math.MaxUint64 - binary.BigEndian.Uint64(key[len(key)-8:])
}

/*
	SameKey checks for key equality ignoring the version timestamp suffix.
	The real key and the timestamp are encoded together
*/
func SameKey(src, dst []byte) bool {
	if len(src) != len(dst) {
		return false
	}
	return bytes.Equal(ParseKey(src), ParseKey(dst))
}

func ParseKey(key []byte) []byte {
	if len(key) < 8 {
		return key
	}

	return key[:len(key)-8]
}

// KeyWithTs generates a new key by appending ts to key.
func KeyWithTs(key []byte, ts uint64) []byte {
	out := make([]byte, len(key)+8)
	copy(out, key)
	binary.BigEndian.PutUint64(out[len(key):], math.MaxUint64-ts)
	return out
}

// SafeCopy does append(a[:0], src...).
func SafeCopy(a, src []byte) []byte {
	return append(a[:0], src...)
}
