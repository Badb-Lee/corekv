package codec

import "time"

type Entry struct {
	Key       []byte
	Value     []byte
	ExpiresAt uint64
}

type ValueStruct struct {
	Meta      byte
	Value     []byte
	ExpiresAt uint64

	Version uint64 // This field is not serialized. Only for internal usage.
}

func NewEntry(key, value []byte) *Entry {

	return &Entry{
		Key:   key,
		Value: value,
	}
}

func (e *Entry) WithTTL(dur time.Duration) *Entry {
	e.ExpiresAt = uint64(time.Now().Add(dur).Unix())
	return e
}
