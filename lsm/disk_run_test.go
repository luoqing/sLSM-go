package lsm

import (
	"fmt"
	"io"
	"os"
	"syscall"
	"testing"
	"unsafe"

	"golang.org/x/exp/mmap"
)

// https://pkg.go.dev/golang.org/x/exp/mmap
// https://tip.golang.org/src/cmd/go/internal/mmap/mmap.go
// https://tip.golang.org/src/cmd/go/internal/mmap/mmap_unix.go
// https://www.cnblogs.com/oxspirt/p/14633182.html
func TestMmapWrite(t *testing.T) {
	filename := fmt.Sprintf("output.txt")
	words := "i am a pretty girl"
	data := []byte(words)
	filesize := uintptr(len(data))
	fmt.Printf("len:%d\n", filesize)
	fd, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0600)
	if err != nil {
		panic(err)
	}
	// strench file
	_, err = fd.Write(make([]byte, filesize))
	if err != nil {
		err = fmt.Errorf("Error writing last byte of the file: %v", err)
		panic(err)
	}
	b, err := syscall.Mmap(int(fd.Fd()), 0, int(filesize), syscall.PROT_WRITE|syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		err = fmt.Errorf("mmap failed: %v", err)
		panic(err)
	}
	err = fd.Close()
	if err != nil {
		panic(err)
	}
	for i, v := range data {
		b[i] = v
	}
	err = syscall.Munmap(b)
	if err != nil {
		panic(err)
	}
}

func TestMmapRead(t *testing.T) {
	filename := fmt.Sprintf("output.txt")
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		panic(err)
	}
	stat, err := f.Stat()
	if err != nil {
		panic(err)
	}
	size := int(stat.Size())
	fmt.Println(size)
	b, err := syscall.Mmap(int(f.Fd()), 0, size, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s\n", b)
	err = f.Close()
	if err != nil {
		panic(err)
	}
	err = syscall.Munmap(b)
	if err != nil {
		panic(err)
	}
}

// https://geektutu.com/post/quick-go-mmap.html
// https://tip.golang.org/src/cmd/go/internal/mmap/mmap_unix.go
// https://tip.golang.org/src/cmd/go/internal/mmap/mmap.go
func TestMmapRead2(t *testing.T) {
	filename := fmt.Sprintf("output.txt")
	at, err := mmap.Open(filename)
	if err != nil {
		panic(err)
	}
	size := at.Len()
	fmt.Printf("file size:%d\n", size)
	buff := make([]byte, size)
	_, err = at.ReadAt(buff, 0)
	if err != nil && err != io.EOF {
		fmt.Printf("read at error:%v\n", err)
		panic(err)
	}
	err = at.Close()
	if err != nil {
		fmt.Printf("close error:%v\n", err)
		panic(err)
	}
	fmt.Println(string(buff))
}

func TestMmapReadStruct(t *testing.T) {
	filename := fmt.Sprintf("C_0_0.txt")
	at, err := mmap.Open(filename)
	if err != nil {
		panic(err)
	}
	size := at.Len()
	fmt.Printf("file size:%d\n", size)
	buff := make([]byte, size)
	var capacity int = size / int(unsafe.Sizeof(KVPair{}))
	_, err = at.ReadAt(buff, 0)
	if err != nil && err != io.EOF {
		fmt.Printf("read at error:%v\n", err)
		panic(err)
	}
	err = at.Close()
	if err != nil {
		fmt.Printf("close error:%v\n", err)
		panic(err)
	}
	mslice := &SliceMock{
		array: uintptr(unsafe.Pointer(&buff[0])),
		len:   capacity,
		cap:   capacity,
	}
	fmt.Printf("size:%d capacity:%d\n", size, capacity)
	Map := *(*[]KVPair)(unsafe.Pointer(mslice))
	fmt.Println(Map) // 为什么key写进去了
}

func TestNewDiskRun(t *testing.T) {
	r := NewDiskRun(10, 2, 0, 0, 0.01)
	a1 := KVPair{
		Key: K{
			Data: 1,
		},
		Value: V{
			Data: 2,
		},
	}
	a2 := KVPair{
		Key: K{
			Data: 12,
		},
		Value: V{
			Data: 23,
		},
	}

	a3 := KVPair{
		Key: K{
			Data: 43,
		},
		Value: V{
			Data: 23,
		},
	}
	run := []KVPair{a1, a2, a3}
	r.WriteData(run, 0, len(run))
	r.ConstructIndex()
	key := K{
		Data: 2,
	}
	found, value := r.LookUp(key)
	fmt.Println(found, value)
	
	err := r.Close()
	if err != nil {
		panic(err)
	}
}


