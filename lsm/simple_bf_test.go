package lsm

import (
	"fmt"
	"testing"
	"unsafe"

	"github.com/bits-and-blooms/bloom/v3"
)

func TestBloom(t *testing.T) {
	key1 := K{
		// Data: "I am a pretty and lovely girl",
		Data: 0,
	}
	key2 := K{
		// Data: "1988",
		Data: 1,
	}
	keys := []K{key1, key2}
	filter := bloom.NewWithEstimates(1000000, 0.01)
	for _, key := range keys {
		data := StructToByte(&key)
		filter.Add(data)
		isContain := filter.Test(data)
		fmt.Printf("key:%v iscontain:%v\n", key, isContain)
	}

	key := K{
		// Data: "not existed",
		Data: 2,
	}
	data := StructToByte(&key)
	isContain := filter.Test(data)
	fmt.Printf("key:%v iscontain:%v\n", key, isContain)
}

func TestSimpleBloom(t *testing.T) {
	key1 := K{
		// Data: "I am a pretty and lovely girl",
		Data: 0,
	}
	key2 := K{
		// Data: "1988",
		Data: 1,
	}
	keys := []K{key1, key2}
	f := filter{}
	for _, key := range keys {
		data := StructToByte(&key)
		f.Set(data)
		isContain := f.Get(data)
		fmt.Printf("key:%v iscontain:%v\n", key, isContain)
	}

	key := K{
		// Data: "not existed",
		Data: 2,
	}
	data := StructToByte(&key)
	isContain := f.Get(data)
	fmt.Printf("key:%v iscontain:%v\n", key, isContain)
}

func TestSizeOf(t *testing.T) {
	s := []int32{1, 2, 3, 4, 5}
	size := unsafe.Sizeof(s)
	fmt.Println(size)
}
