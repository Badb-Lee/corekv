package stats

import "corekv/utils"

type Options struct {
	ValueThreshold int64
}

func NewDefaultOptions() *Options {
	opt := &Options{ValueThreshold: utils.DefaultValueThreshold}
	return opt
}
