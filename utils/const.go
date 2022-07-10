/*
 * @Author: dejavudwh
 * @Date: 2022-07-10 09:31:22
 * @LastEditTime: 2022-07-10 09:33:31
 */
package utils

import "hash/crc32"

/* for codec */
var (
	MagicText    = [4]byte{'D', 'V', 'D', '7'}
	MagicVersion = uint32(1)
	// CastagnoliCrcTable is a CRC32 polynomial table
	CastagnoliCrcTable = crc32.MakeTable(crc32.Castagnoli)
)
