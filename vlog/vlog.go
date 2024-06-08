package vlog

import (
	"corekv/utils"
	"corekv/utils/codec"
)

type Options struct {
}

/*
* vlog是用来实现kv分离
 */
type VLog struct {
	closer *utils.Closer
}

// NewVLog 新建VLog
func NewVLog() *VLog {
	v := &VLog{}
	v.closer = utils.NewCloser(1)
	return v
}

func (v *VLog) StartGC() {
	defer v.closer.Done()
	for {
		select {
		case <-v.closer.Wait():

		}
	}
}

// Set 进行kv分离
func (v *VLog) Set(entry *codec.Entry) error {
	return nil
}

func (v *VLog) Get(entry *codec.Entry) (*codec.Entry, error) {
	return nil, nil
}
