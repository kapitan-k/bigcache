package buffer

type Buffer interface {
	Bytes() (bytes []byte)
	Enlarge(nextSize int) (nextBuffer, oldBuffer []byte, err error)
	Close() (err error)
}

type BufferCreator interface {
	NewBuffer(initialSize int) (buffer Buffer, err error)
}
