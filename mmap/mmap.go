package mmap

import (
	"errors"
	"os"
	"reflect"
	"syscall"
	"unsafe"
)

var (
	ErrMmapTooSmall = errors.New("Mmap area too small, you must allocate a larger one in advance")
	ErrFileTooSmall = errors.New("File too small, you must allocate a larger one in advance")
)

type MmapFile struct {
	file        *os.File
	mapData     []byte
	mapSize     int
	usedMapSize int
}

func NewMmapFileOpen(name string, allocSize int) (mmf *MmapFile, err error) {
	var file *os.File
	var mapData []byte

	file, err = os.OpenFile(name, os.O_CREATE|os.O_RDWR, 0777)
	if err != nil {
		return
	}

	var stats os.FileInfo
	stats, err = file.Stat()
	if err != nil {
		return
	}

	err = syscall.Fallocate(int(file.Fd()), 0, stats.Size(), int64(allocSize))
	if err != nil {
		return
	}

	mapData, err = syscall.Mmap(int(file.Fd()), 0, allocSize, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED|syscall.MAP_NORESERVE)
	if err != nil {
		return
	}

	mmf = &MmapFile{
		file:    file,
		mapData: mapData,
		mapSize: allocSize,
	}

	return
}

func (mmf *MmapFile) File() *os.File {
	return mmf.file
}

func (mmf *MmapFile) Close() (err error) {
	if mmf.mapData != nil {
		err = syscall.Munmap(mmf.mapData)
		if err != nil {
			return
		}
	}
	return mmf.file.Close()
}

func (mmf *MmapFile) Madvise(advice int) (err error) {
	return syscall.Madvise(mmf.mapData, advice)
}

func (mmf *MmapFile) CreateBuffer(size int) (buf []byte, err error) {
	availableSize := mmf.mapSize - mmf.usedMapSize
	if size > availableSize {
		return nil, ErrMmapTooSmall
	}

	mmf.usedMapSize += size

	return
}

func (mmf *MmapFile) EnsureFileSize(size int) (buf []byte, isRemapped bool, err error) {

	file := mmf.File()
	var stats os.FileInfo
	stats, err = file.Stat()
	if err != nil {
		return
	}

	fileSize := stats.Size()
	if int64(size) > fileSize {
		err = syscall.Fallocate(int(file.Fd()), 0, fileSize, int64(size)-fileSize)
		if err != nil {
			return
		}
	}

	if size > mmf.mapSize {
		var addrNew uintptr
		addr := uintptr(unsafe.Pointer(&mmf.mapData[0]))
		addrNew, err = Mremap(addr, uintptr(mmf.mapSize), uintptr(size))
		if err != nil {
			return
		}

		mmf.mapData = *(*[]byte)(unsafe.Pointer(&reflect.SliceHeader{
			Data: addrNew,
			Len:  size,
			Cap:  size,
		}))

		mmf.mapSize = size
		isRemapped = true
	}

	return mmf.mapData, isRemapped, nil
}

func (mmf *MmapFile) MapData() []byte {
	return mmf.mapData
}

func Mmap(addr, l, prot, flags, fd, offset uintptr) (uintptr, error) {
	xaddr, _, err := syscall.Syscall6(syscall.SYS_MMAP, addr, l, prot, flags, fd, offset)
	if err != 0 {
		return 0, err
	}
	return xaddr, nil
}

const mREMAP_MAYMOVE = 1

func Mremap(addr, len, lenNew uintptr) (uintptr, error) {
	xaddr, _, err := syscall.Syscall6(syscall.SYS_MREMAP, addr, len, lenNew, mREMAP_MAYMOVE, 0, 0)
	if err != 0 {
		return 0, err
	}
	return xaddr, nil
}

func Munmap(addr, len uintptr) error {
	_, _, err := syscall.Syscall(syscall.SYS_MUNMAP, addr, len, 0)
	if err != 0 {
		return err
	}
	return nil
}
