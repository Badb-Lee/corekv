package file

import "os"

/**
文件的基本操作
*/

type LogFile struct {
	f *os.File
}

func (lf *LogFile) close() error {
	if err := lf.f.Close(); err != nil {
		return err
	}
	return nil
}

func (ls *LogFile) write(bytes []byte) error {
	//if _, err := ls.f.Write(append(bytes, '\n')); err != nil {
	//	return err
	//}
	return nil
}

type Options struct {
	name     string
	FID      uint64
	FileName string
	Dir      string
	Path     string
	Flag     int
	MaxSz    int
}

func openLogFile(opt *Options) *LogFile {
	lf := &LogFile{}
	lf.f, _ = os.Create(opt.name)
	return lf
}
