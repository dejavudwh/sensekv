/*
 * @Author: dejavudwh
 * @Date: 2022-07-10 08:02:36
 * @LastEditTime: 2022-07-10 11:08:12
 */
package file

import (
	"os"
	"sync"
)

/*
	ManifestFile is the file used to maintain the meta information of the sst file
	Cannot use mmap, need to guarantee real-time writing
*/
type ManifestFile struct {
	opt                       *Options
	f                         *os.File
	lock                      sync.Mutex
	deletionsRewriteThreshold int
	manifest                  *Manifest
}

/* Metadata status maintenance */
type Manifest struct {
	Levels    []levelManifest
	Tables    map[uint64]TableManifest
	Creations int
	Deletions int
}

type TableManifest struct {
	Level    uint8
	Checksum []byte
}

type levelManifest struct {
	Tables map[uint64]struct{} // Set of table id's
}

/* Some meta information about sst */
type TableMeta struct {
	ID       uint64
	Checksum []byte
}
