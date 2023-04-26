package lsm

import (
	"log"
)

// 内存的run
// 可以是skiplist， 也可以是array
type ArrayRun struct {
	data []KVPair
	min  K
	max  K
}

// 定义key的范围，是什么作用？
// 是skiplist需要，感觉是ckiplist需要
func NewArrayRun(minKey K, maxKey K) *ArrayRun {
	r := &ArrayRun{
		min: minKey,
		max: maxKey,
	}
	return r
}

func (r *ArrayRun) GetMin() (key K) {
	return r.min
}

func (r *ArrayRun) GetMax() (key K) {
	return r.max
}

func (r *ArrayRun) InsertKey(key K, value V) {
	found, index := r.binarySearch(key)
	log.Printf("search before insert key:%v found:%v index:%d\n", key, found, index)
	kv := KVPair{
		Key:   key,
		Value: value,
	}
	if found {
		r.data[index] = kv
	} else {
		r.data = append(r.data, kv) // 需要扩容一个位置
		// 元素后移动
		// 在下标index前进行插入
		for i := len(r.data) - 2; i >= index; i-- {
			r.data[i+1] = r.data[i]
		}
		r.data[index] = kv
	}

	if moreThan(key, r.max) {
		r.max = key
	}
	if lessThan(key, r.min) {
		r.min = key
	}
	return
}

func (r *ArrayRun) DeleteKey(key K) {
	found, index := r.binarySearch(key)
	if found {
		for i := index; i < len(r.data)-1; i++ {
			r.data[i] = r.data[i+1]
		}
	}
	return
}

func (r *ArrayRun) binarySearch(key K) (found bool, index int) {
	found = false
	left := 0
	right := len(r.data) - 1

	for left <= right {
		middle := (left + right) >> 1
		if moreThan(key, r.data[middle].Key) {
			left = middle + 1
		} else if key == r.data[middle].Key {
			found = true
			index = middle
			return
		} else {
			right = middle - 1
		}
	}
	index = left
	return
}

func (r *ArrayRun) LookUp(key K) (found bool, value V) {
	found, index := r.binarySearch(key)
	if found {
		value = r.data[index].Value
	}
	return
}

func (r *ArrayRun) NumElements() (num int) {
	return len(r.data)
}

func (r *ArrayRun) GetAll() []KVPair {
	return r.data
}

func (r *ArrayRun) Range(key1 K, key2 K) (data []KVPair) {
	if moreThan(key1, key2) {
		tmp := key2
		key2 = key1
		key1 = tmp
	}
	i1, i2 := 0, 0
	if moreThan(key1, r.max) || lessThan(key2, r.min) {
		return
	}
	if !lessThan(key1, r.min) {
		_, i1 = r.binarySearch(key1)
	} else {
		i1 = 0
	}

	if moreThan(key2, r.max) {
		key2 = r.max
		i2 = len(r.data)
	} else {
		_, i2 = r.binarySearch(key2)
	}
	return r.data[i1:i2]
}
