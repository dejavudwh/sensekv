/*
 * @Author: dejavudwh
 * @Date: 2022-07-10 09:31:22
 * @LastEditTime: 2022-07-10 14:18:33
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

/* file */
const (
	ManifestFilename                  = "MANIFEST"
	ManifestRewriteFilename           = "REWRITEMANIFEST"
	ManifestDeletionsRewriteThreshold = 10000
	ManifestDeletionsRatio            = 10
	DefaultFileFlag                   = os.O_RDWR | os.O_CREATE | os.O_APPEND
	DefaultFileMode                   = 0666
)
