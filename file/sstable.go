package file

/*
*
sstable对象
*/
type SSTable struct {
	f *LogFile
}

func OpenSSTable(opt *Options) *SSTable {
	return &SSTable{f: openLogFile(opt)}
}
