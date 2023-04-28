package lsm

import (
	"fmt"
	"math/rand"
)

type SkipListNode struct {
	key     K
	value   V
	forward []*SkipListNode
}

type SkipList struct {
	header   *SkipListNode
	maxLevel int
	level    int
}

func NewSkipListNode(key K, value V, level int) *SkipListNode {
	node := new(SkipListNode)
	node.key = key
	node.value = value
	node.forward = make([]*SkipListNode, level)
	return node
}

func NewSkipList() *SkipList {
	list := new(SkipList)
	list.header = NewSkipListNode(K{}, V{}, 32)
	list.maxLevel = 32
	list.level = 1
	return list
}

func (list *SkipList) randomLevel() int {
	level := 1
	for rand.Intn(2) == 1 && level < list.maxLevel {
		level++
	}
	return level
}

func (list *SkipList) InsertKey(key K, value V) {
	update := make([]*SkipListNode, list.maxLevel)
	x := list.header
	for i := list.level - 1; i >= 0; i-- {
		for x.forward[i] != nil && x.forward[i].key.Data < key.Data {
			x = x.forward[i]
		}
		update[i] = x
	}
	x = x.forward[0]
	if x != nil && x.key == key {
		x.value = value
		return
	}
	level := list.randomLevel()
	if level > list.level {
		for i := list.level; i < level; i++ {
			update[i] = list.header
		}
		list.level = level
	}
	x = NewSkipListNode(key, value, level)
	for i := 0; i < level; i++ {
		x.forward[i] = update[i].forward[i]
		update[i].forward[i] = x
	}
}

func (list *SkipList) LookUp(key K) (found bool, value V) {
	x := list.header
	for i := list.level - 1; i >= 0; i-- {
		for x.forward[i] != nil && x.forward[i].key.Data < key.Data {
			x = x.forward[i]
		}
	}
	if x.forward[0] != nil && x.forward[0].key.Data == key.Data {
		return true, x.forward[0].value
	} else {
		return false, V{}
	}
}

func (list *SkipList) DeleteKey(key K) {
	update := make([]*SkipListNode, list.maxLevel)
	x := list.header
	for i := list.level - 1; i >= 0; i-- {
		for x.forward[i] != nil && x.forward[i].key.Data < key.Data {
			x = x.forward[i]
		}
		update[i] = x
	}
	if x.forward[0] != nil && x.forward[0].key.Data == key.Data {
		for i := 0; i < list.level; i++ {
			if update[i].forward[i] != nil && update[i].forward[i].key.Data == key.Data {
				update[i].forward[i] = update[i].forward[i].forward[i]
			}
		}
	}
}

func (list *SkipList) NumElements() int {
	count := 0
	x := list.header.forward[0]
	for x != nil {
		count++
		x = x.forward[0]
	}
	return count
}

func (list *SkipList) GetAll() []KVPair {
	pairs := make([]KVPair, 0)
	x := list.header.forward[0]
	for x != nil {
		pairs = append(pairs, KVPair{x.key, x.value})
		x = x.forward[0]
	}
	fmt.Printf("pairs:%d\n", len(pairs))
	return pairs
}

func (list *SkipList) Range(key1 K, key2 K) []KVPair {
	pairs := make([]KVPair, 0)
	x := list.header
	for i := list.level - 1; i >= 0; i-- {
		for x.forward[i] != nil && x.forward[i].key.Data < key1.Data {
			x = x.forward[i]
		}
	}
	x = x.forward[0]
	for x != nil && x.key.Data < key2.Data {
		pairs = append(pairs, KVPair{x.key, x.value})
		x = x.forward[0]
	}
	return pairs
}

type SkipListRun struct {
	list *SkipList
	min  K
	max  K
}

func NewSkipListRun(minKey K, maxKey K) *SkipListRun {
	r := &SkipListRun{
		min:  minKey,
		max:  maxKey,
		list: NewSkipList(),
	}
	return r
}

func (r *SkipListRun) GetMin() K {
	return r.min
	// return r.list.header.forward[0].key
}

func (r *SkipListRun) GetMax() K {
	return r.max
	// x := r.list.header
	// for i := r.list.level - 1; i >= 0; i-- {
	// 	for x.forward[i] != nil {
	// 		x = x.forward[i]
	// 	}
	// }
	// return x.key
}

func (r *SkipListRun) InsertKey(key K, value V) {
	r.list.InsertKey(key, value)
	if moreThan(key, r.max) {
		r.max = key
	}
	if lessThan(key, r.min) {
		r.min = key
	}
}

func (r *SkipListRun) DeleteKey(key K) {
	r.list.DeleteKey(key)
}

func (r *SkipListRun) LookUp(key K) (found bool, value V) {
	return r.list.LookUp(key)
}

func (r *SkipListRun) NumElements() int {
	return r.list.NumElements()
}

func (r *SkipListRun) GetAll() []KVPair {
	return r.list.GetAll()
}

func (r *SkipListRun) Range(key1 K, key2 K) []KVPair {
	return r.list.Range(key1, key2)
}
