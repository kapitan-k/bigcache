package buffer

type ByteSliceBuffer struct {
	data []byte
}

func (b *ByteSliceBuffer) Bytes() (bytes []byte) {
	return b.data
}

func (b *ByteSliceBuffer) Enlarge(nextSize int) (nextBuffer, oldBuffer []byte, err error) {
	oldBuffer = b.data
	b.data = make([]byte, nextSize)
	return b.data, oldBuffer, nil
}

func (b *ByteSliceBuffer) Close() (err error) {
	b.data = nil
	return
}

type ByteSliceBufferCreator struct{}

func (self ByteSliceBufferCreator) NewBuffer(initialSize int) (buffer Buffer, err error) {
	return &ByteSliceBuffer{
		data: make([]byte, initialSize),
	}, nil
}
