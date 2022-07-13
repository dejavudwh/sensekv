/*
 * @Author: dejavudwh
 * @Date: 2022-07-10 09:31:22
 * @LastEditTime: 2022-07-13 12:35:29
 */
package utils

import (
	"hash/crc32"
	"os"
)

/* for codec */
var (
	MagicText    = [4]byte{'D', 'V', 'D', '7'}
	MagicVersion = uint32(1)
	// CastagnoliCrcTable is a CRC32 polynomial table
	CastagnoliCrcTable = crc32.MakeTable(crc32.Castagnoli)
)

// meta
const (
	BitDelete       byte = 1 << 0 // Set if the key has been deleted.
	BitValuePointer byte = 1 << 1 // Set if the value is NOT stored directly next to key.
)

/* file */
const (
	ManifestFilename                  = "MANIFEST"
	ManifestRewriteFilename           = "REWRITEMANIFEST"
	ManifestDeletionsRewriteThreshold = 10000
	ManifestDeletionsRatio            = 10
	DefaultFileFlag                   = os.O_RDWR | os.O_CREATE | os.O_APPEND
	DefaultFileMode                   = 0666
)
