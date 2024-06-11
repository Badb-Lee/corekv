package corekv

import "github.com/hardcore-os/corekv/utils"

type Options struct {
	ValueThreshold int64
}

// NewDefaultOptions 返回默认的options
// 初始化这个结构体
func NewDefaultOptions() *Options {
	opt := &Options{}
	opt.ValueThreshold = utils.DefaultValueThreshold
	return opt
}
