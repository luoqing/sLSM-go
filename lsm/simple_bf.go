package lsm

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"hash"
)

// https://github.com/bits-and-blooms/bloom
// filter := bloom.NewWithEstimates(1000000, 0.01)
//  filter.Add([]byte("Love"))
// filter.Test([]byte("Love"))

// 以下简单的方法只是使用了一个hash函数。这样对于hash函数的散列保证不会那么好
// https://medium.com/@meeusdylan/creating-a-bloom-filter-with-go-7d4e8d944cfa
type filter struct {
	bitfield [100]bool
}

func createHash(h hash.Hash, input []byte) int {
	h.Write(input)
	bits := h.Sum(nil)
	buf := bytes.NewBuffer(bits)
	result, _ := binary.ReadVarint(buf)
	return int(result) // cast the int64
}

func (f *filter) hashPosition(s []byte) int {
	var hasher = sha1.New() // hasher不可复用，如果复用需要重置
	hs := createHash(hasher, s)
	if hs < 0 {
		hs = -hs // ensure a positive index
	}
	return hs % len(f.bitfield)
}

func (f *filter) Set(s []byte) {
	pos := f.hashPosition(s)
	f.bitfield[pos] = true
}

func (f *filter) Get(s []byte) bool {
	pos := f.hashPosition(s)
	return f.bitfield[pos]
}
