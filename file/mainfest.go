package file

/*
*
该文件用于记录sstable在哪一层
*/
type Mainfest struct {
	f *LogFile

	Levels    []levelManifest
	Tables    map[uint64]TableManifest
	Creations int
	Deletions int
}

type levelManifest struct {
	Tables map[uint64]struct{} // Set of table id's
}

// TableManifest 包含sst的基本信息
type TableManifest struct {
	Level    uint8
	Checksum []byte // 方便今后扩展
}

func (mf *Mainfest) Close() error {
	if err := mf.f.close(); err != nil {
		return err
	}
	return nil
}

func OpenMainfest(opt *Options) *Mainfest {

	return &Mainfest{f: openLogFile(opt)}

}
