/*
 * @Author: dejavudwh
 * @Date: 2022-07-15 07:01:20
 * @LastEditTime: 2022-07-15 07:05:23
 */
package sensekv

import "sensekv/utils"

type Options struct {
	ValueThreshold      int64
	WorkDir             string
	MemTableSize        int64
	SSTableMaxSz        int64
	MaxBatchCount       int64
	MaxBatchSize        int64 // max batch size in bytes
	ValueLogFileSize    int
	VerifyValueChecksum bool
	ValueLogMaxEntries  uint32
	LogRotatesToFlush   int32
	MaxTableSize        int64
}

// NewDefaultOptions 返回默认的options
func NewDefaultOptions() *Options {
	opt := &Options{
		WorkDir:      "./worktest",
		MemTableSize: 1024,
		SSTableMaxSz: 1 << 30,
	}
	opt.ValueThreshold = utils.DefaultValueThreshold
	return opt
}
