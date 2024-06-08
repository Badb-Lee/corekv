package file

/*
*
该文件用于记录sstable在哪一层
*/
type Mainfest struct {
	f *LogFile
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
