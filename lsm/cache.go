package lsm

/*
该文件用来存储热点数据
*/
type Cache struct {
}

func (c *Cache) close() error {
	return nil
}

func newCache(opt *Options) *Cache {
	return &Cache{}
}
