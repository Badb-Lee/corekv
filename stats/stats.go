package stats

import (
	"corekv/utils"
)

/**
该文件是用来进行信息统计的，比如使用了多少内存，占用了多少kv
*/

type Stats struct {
	closer   *utils.Closer // 用于资源回收的信号控制
	EntryNum int64         //统计一共有多少kv数据
}

func (s *Stats) close() error {
	return nil
}

func (s *Stats) startStats() {
	defer s.closer.Done()

	for {
		select {
		case <-s.closer.Wait():

		}
	}
}

func newStats(opt *Options) *Stats {
	s := &Stats{}
	s.closer = utils.NewCloser(1)
	s.EntryNum = 1
	return s
}
