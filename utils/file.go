/*
 * @Author: dejavudwh
 * @Date: 2022-07-10 09:30:23
 * @LastEditTime: 2022-07-10 09:30:34
 */
package utils

import (
	"hash/crc32"

	"github.com/pkg/errors"
)

func VerifyChecksum(data []byte, expected []byte) error {
	actual := uint64(crc32.Checksum(data, CastagnoliCrcTable))
	expectedU64 := BytesToU64(expected)
	if actual != expectedU64 {
		return errors.Wrapf(ErrChecksumMismatch, "actual: %d, expected: %d", actual, expectedU64)
	}

	return nil
}
