package lsm

import (
	"fmt"
	"testing"
)

func TestByteToStruct(t *testing.T) {
	key := K{
		// Data: "I am a pretty and lovely girl",
		Data: 1,
	}
	data := StructToByte(&key)
	key2 := ByteToStruct(data)
	fmt.Println(key2)
}

func TestBloomFilter(t *testing.T) {
	// fasePostive的概率越低越好
	bf := NewBloomFilter(100, 0.01)
	fmt.Println(bf)
	key1 := K{
		// Data: "I am a pretty and lovely girl",
		Data: 1,
	}
	key2 := K{
		// Data: "1988",
		Data: 1,
	}
	keys := []K{key1, key2}
	for _, key := range keys {
		bf.Add(key)
		isContain := bf.MayContain(key)
		fmt.Printf("key:%v iscontain:%v\n", key, isContain)
	}
	key := K{
		// Data: "not existed",
		Data: 2,
	}
	isContain := bf.MayContain(key)
	fmt.Printf("key:%v iscontain:%v\n", key, isContain)
}
