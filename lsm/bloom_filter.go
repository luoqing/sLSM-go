package lsm

import (
	"math"
	"unsafe"

	"github.com/spaolacci/murmur3"
)

type BloomFilter struct {
	bits      []bool
	numHashes int
}

func NewBloomFilter(n uint64, fp float64) (bf *BloomFilter) {
	denom := 0.480453013918201 // (ln(2))^2
	size := -1 * float64(n) * (math.Log2(fp) / denom)
	bf = &BloomFilter{}
	bf.bits = make([]bool, int(size))
	ln2 := 0.693147180559945
	bf.numHashes = int(math.Ceil((size / float64(n)) * ln2))
	return
}

// https://cloud.tencent.com/developer/article/1468933

func baseHash(data []byte) []uint64 {
	a1 := []byte{1} // to grab another bit of data
	hasher := murmur3.New128()
	hasher.Write(data) // #nosec
	v1, v2 := hasher.Sum128()
	hasher.Write(a1) // #nosec
	v3, v4 := hasher.Sum128()
	return []uint64{
		v1, v2, v3, v4,
	}
}

type SliceMock struct {
	addr uintptr
	len  int
	cap  int
}

// http://www.codebaoku.com/it-go/it-go-144265.html
func ByteToStruct(data []byte) (key *K) {
	key = *(**K)(unsafe.Pointer(&data))
	// fmt.Printf("struct data is : %v\n", key)
	return
}

func StructToByte(key *K) (data []byte) {
	len := unsafe.Sizeof(*key)
	testBytes := &SliceMock{
		addr: uintptr(unsafe.Pointer(key)),
		cap:  int(len),
		len:  int(len),
	}
	data = *(*[]byte)(unsafe.Pointer(testBytes))
	// fmt.Printf("[]byte is : %v\n", data)
	return
}

// 写入
func (bf *BloomFilter) Add(key K) {
	data := StructToByte(&key)
	hashValues := baseHash(data)
	n := 0
	for n < bf.numHashes {
		pos := bf.nthHash(uint32(n), hashValues[0], hashValues[1], uint64(len(bf.bits)))
		bf.bits[pos] = true
		n += 1
	}
}

func (bf *BloomFilter) nthHash(n uint32, hashA, hashB, filterSize uint64) uint64 {
	return (hashA + uint64(n)*hashB) % filterSize
}

// 判断是否存在
// 如果hash与bitmap有一bit不能相同，就是不存在
func (bf *BloomFilter) MayContain(key K) bool {
	data := StructToByte(&key)
	hashValues := baseHash(data)
	n := 0
	for n < bf.numHashes {
		size := len(bf.bits)
		pos := bf.nthHash(uint32(n), hashValues[0], hashValues[1], uint64(size))
		has := bf.bits[pos]
		if !has {
			return false
		}
		n += 1
	}
	return true
}
