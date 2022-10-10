package lsm

// 代码不是单纯的将c++翻译成go，而是将你自己的理解进行实现，拿出你的热情
// 提升编码能力
// 提升对LSM的理解

// 先定义好api——没语法错误
// 再写测试用例
// 最后写实现

type K struct {
	// Data interface{}  // 如果是interface，文件存储的是指针，数据并不是一直都保留的
	Data int // 使用int也是可以的
}

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

type V struct {
	// Data interface{}
	Data int
}

type KVPair struct {
	Key   K
	Value V
}

type Run interface {
	GetMin() K
	GetMax() K
	InsertKey(key K, value V)
	DeleteKey(key K)
	LookUp(key K) (found bool, value V)
	NumElements() int
	// get_all
	// get_all_in_range
}
