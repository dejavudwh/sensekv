/*
 * @Author: dejavudwh
 * @Date: 2022-07-10 11:19:36
 * @LastEditTime: 2022-07-10 11:19:39
 */
package lsm

import (
	"bytes"
	"sensekv/db"
)

type memTable struct {
	lsm        *LSM
	sl         *db.Skiplist
	buf        *bytes.Buffer
	maxVersion uint64
}
