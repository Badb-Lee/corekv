package file

import (
	"corekv/utils/codec"
)

/**
是wal文件，用来执行wal策略的
*/

type WALFile struct {
	f *LogFile
}

func (wf *WALFile) Close() error {
	if err := wf.f.close(); err != nil {
		return err
	}
	return nil

}

func OpenWalFile(opt *Options) *WALFile {
	return &WALFile{f: openLogFile(opt)}
}

// 将操作写入aof文件中
func (wf *WALFile) Write(entry *codec.Entry) error {
	// 写入的过程是简单的同步写
	// 序列化为磁盘结构
	walData := codec.WALCodec(entry)
	return wf.f.write(walData)
}
