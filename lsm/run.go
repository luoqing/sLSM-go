package lsm

// 代码不是单纯的将c++翻译成go，而是将你自己的理解进行实现，拿出你的热情
// 提升编码能力
// 提升对LSM的理解

// 先定义好api——没语法错误
// 再写测试用例
// 最后写实现

func lessThan(key1 K, key2 K) bool {
	return key1.Data < key2.Data
	// if value1, ok1 := key1.Data.(int); ok1 {
	// 	if value2, ok2 := key2.Data.(int); ok2 {
	// 		return value1 < value2
	// 	}
	// }
	// if value1, ok1 := key1.Data.(string); ok1 {
	// 	if value2, ok2 := key2.Data.(string); ok2 {
	// 		return value1 < value2
	// 	}
	// }
	panic("not support key type")
}

func moreThan(key1 K, key2 K) bool {
	return key1.Data > key2.Data
	// if value1, ok1 := key1.Data.(int); ok1 {
	// 	if value2, ok2 := key2.Data.(int); ok2 {
	// 		return value1 > value2
	// 	}
	// }
	// if value1, ok1 := key1.Data.(string); ok1 {
	// 	if value2, ok2 := key2.Data.(string); ok2 {
	// 		return value1 > value2
	// 	}
	// }
	panic("not support key type")
}

type K struct {
	// Data interface{}  // 如果是interface，文件存储的是指针，数据并不是一直都保留的
	Data int // 使用int也是可以的
}

type V struct {
	// Data interface{}
	Data int
}

type KVPair struct {
	Key   K
	Value V
}

type Run interface {
	InsertKey(key K, value V)
	// DeleteKey(key K) // 基于日志的都不用实现
	LookUp(key K) (found bool, value V)
	// GetMin() K // 实现了没用到
	// GetMax() K // 实现了没用到
	NumElements() int
	GetAll() []KVPair
	// Range(key1 K, key2 K) []KVPair // 实现了没用到
}

type Item struct {
	KV       KVPair
	priority int
	index    int // 必须大小，否则无法暴露在外进行复制
}

type PriorityQueue []*Item

var V_TOMBSTONE V = V{
	Data: -1,
}

var INT32_MIN K = K{
	Data: -2147483648,
}

var INT32_MAX K = K{
	Data: 2147483647,
}

func (pq PriorityQueue) Len() int { return len(pq) }
func (pq PriorityQueue) Less(i, j int) bool {
	// We want Pop to give us the highest, not lowest, priority so we use greater than here.
	if pq[i].KV.Key == pq[j].KV.Key {
		return pq[i].priority > pq[j].priority
	} else {
		return lessThan(pq[i].KV.Key, pq[j].KV.Key)
	}
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*Item)
	item.index = n
	*pq = append(*pq, item)

}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}
