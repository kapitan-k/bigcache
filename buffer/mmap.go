package buffer

import (
	"github.com/kapitan-k/bigcache/mmap"
	"strconv"
	"sync"
)

type MmapFileBuffer struct {
	mmf *mmap.MmapFile
}

func NewMmapFileBuffer(mmf *mmap.MmapFile) (b *MmapFileBuffer) {
	b = &MmapFileBuffer{
		mmf,
	}
	return
}

func NewMmapFileBufferOpen(name string, allocSize int) (b *MmapFileBuffer, err error) {
	var mmf *mmap.MmapFile
	mmf, err = mmap.NewMmapFileOpen(name, allocSize)
	if err != nil {
		return
	}

	return NewMmapFileBuffer(mmf), nil
}

func (b *MmapFileBuffer) Bytes() (bytes []byte) {
	return b.mmf.MapData()
}

func (b *MmapFileBuffer) Enlarge(nextSize int) (nextBuffer, oldBuffer []byte, err error) {
	var isRemapped bool
	oldBuffer = b.Bytes()
	nextBuffer, isRemapped, err = b.mmf.EnsureFileSize(nextSize)
	if isRemapped {
		oldBuffer = nextBuffer[:len(oldBuffer)]
	}

	return
}

func (b *MmapFileBuffer) Close() (err error) {
	return b.mmf.Close()
}

type SeqentialMmapFileBufferCreator struct {
	lock sync.Mutex
	path string
	seq  uint64
}

func NewSeqentialMmapFileBufferCreator(path string) (c *SeqentialMmapFileBufferCreator) {
	if path[len(path)-1] != '/' {
		path += "/"
	}

	return &SeqentialMmapFileBufferCreator{
		path: path,
		seq:  1,
	}
}

// NewBuffer returns a newly created and opened MmapFileBuffer on success, or an error.
// The method is thread safe (goroutine safe).
func (c *SeqentialMmapFileBufferCreator) NewBuffer(initialSize int) (buffer Buffer, err error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	var b *MmapFileBuffer
	b, err = NewMmapFileBufferOpen(c.path+strconv.FormatUint(c.seq, 10), initialSize)
	if err != nil {
		return
	}

	c.seq++

	return b, nil
}
