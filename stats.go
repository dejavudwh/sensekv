/*
 * @Author: dejavudwh
 * @Date: 2022-07-15 07:26:24
 * @LastEditTime: 2022-07-15 07:26:30
 */
package sensekv

import "sensekv/utils"

type Stats struct {
	closer   *utils.Closer
	EntryNum int64 // 存储多少个kv数据
}

// Close
func (s *Stats) close() error {
	return nil
}

// StartStats
func (s *Stats) StartStats() {
	defer s.closer.Done()
	for {
		select {
		case <-s.closer.CloseSignal:
			return
		}
		// stats logic...
	}
}

// NewStats
func newStats(opt *Options) *Stats {
	s := &Stats{}
	s.closer = utils.NewCloser()
	s.EntryNum = 1 // 这里直接写
	return s
}
