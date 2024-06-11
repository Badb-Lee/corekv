package codec

type ValuePtr struct {
}

func NewValuePtr(entry *Entry) *ValuePtr {
	return &ValuePtr{}
}

func IsValuePtr(entry *Entry) bool {
	return true
}

func ValuePtrDecode(data []byte) *ValuePtr {
	return nil
}
