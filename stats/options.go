package stats

import "corekv/utils"

type Options struct {
	ValueThreshold int64
}

func newDefaultOptions() *Options {
	opt := &Options{ValueThreshold: utils.DefaultValueThreshold}
	return opt
}
