package lsm

// 代码不是单纯的将c++翻译成go，而是将你自己的理解进行实现，拿出你的热情
// 提升编码能力
// 提升对LSM的理解

// 先定义好api——没语法错误
// 再写测试用例
// 最后写实现

type int K
type int V 

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

type KVPair struct {
	Key   K
	Value V
}





