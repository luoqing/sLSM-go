package lsm

import (
	"fmt"
	"os"
)

// 磁盘的run
type DiskRun struct {
	Map           []KVPair // mmap， 将磁盘和虚拟内存映射，避免了用户态和内核态的多次拷贝
	Fd            *os.File
	Bf            BloomFilter
	pageSize      int
	fencePointers []K // 存储多个key，按一定步长pageSize提取key，这样即使不存储pos，也知道key对应在哪个范围
	capacity      int
	filename      string
	level         int
	runID         int
	bf_fp         float64
	minKey        K
	maxKey        K
}

// 【todo】capacity这个字段有啥用--- map的大小, 用来初始化map的大小，方便mmap
func NewDiskRun(capacity, pageSize, level int, runID int, bfFp float64) *DiskRun {
	// 创建文件 C_level_runID.txt
	// mmap --- 主要为了创建map
	return nil
}

// ？？释放就是doUnMap在对象进行销毁的时候
// 将长度为len的run数组写入到map的offset开始的位置
func (r *DiskRun) WriteData(run []KVPair, offset int, len int) {
	copy(r.Map[offset:(offset+len)], run[0:])
	// capacity = offset + len 吗 ，而不是capacity = offset
}

// 根据map中的数据构建索引——bloomfilter + fencepointer + minkey, maxkey
func (r *DiskRun) ConstructIndex() {
	// map的大小是否是固定的呢？ 在创建run的时候应该是固定了为capacity
	var i int = 0
	for i < r.capacity {
		elem := r.Map[i]
		// 由于map是有序数组，只需要取第一个和最后一个即是最小值和最大值
		// if lessThan(elem.Key, r.minKey) {
		// 	r.minKey = elem.Key
		// }
		// if moreThan(elem.Key, r.maxKey) {
		// 	r.maxKey = elem.Key
		// }
		r.Bf.Add(elem.Key)
		if i%r.pageSize == 0 {
			r.fencePointers = append(r.fencePointers, elem.Key)
		}
		i += 1
	}
	r.minKey = r.Map[0].Key
	r.maxKey = r.Map[r.capacity-1].Key
}

// 【todo】根据fencepointer定位key的数据范围 start和end
func (r *DiskRun) getFlankingFP(key K) (start int, end int) {
	// 二分查找定位出start 和 end
	left := 0
	right := len(r.fencePointers)

	return
}

// 【todo】然后zai [start, end) 中查找key的位置
func (r *DiskRun) binarySearch(start int, n int, key K) (found bool, pos int) {
	return
}

func (r *DiskRun) getIndex(key K) (found bool, pos int) {
	start, end := r.getFlankingFP(key)
	found, pos = r.binarySearch(start, end-start, key)
	return
}

func (r *DiskRun) Range(key1 K, key2 K) (i1 int, i2 int) {
	i1, i2 = 0, 0

	if moreThan(key1, r.maxKey) || lessThan(key2, r.minKey) {
		return
	}

	if !lessThan(key1, r.minKey) {
		_, i1 = r.getIndex(key1)
	} else {
		i1 = 0
	}

	if moreThan(key2, r.maxKey) {
		key2 = r.maxKey
		i2 = r.capacity
	} else {
		_, i2 = r.getIndex(key2)
	}
	return
}

func (r *DiskRun) PrintElts() {
	for _, element := range r.Map {
		// fmt.Printf(" (%v, %v)", element.Key.Data, element.Value.Data)
		fmt.Printf("%v,", element.Key.Data)
	}
	return
}

func (r *DiskRun) LookUp(key K) (found bool, value V) {
	found, idx := r.getIndex(key)
	if found {
		value = r.Map[idx].Value
	}
	return
}

func (r *DiskRun) GetMin() K {
	return r.minKey
}

func (r *DiskRun) GetMax() K {
	return r.maxKey
}

// func (r *DiskRun) InsertKey(key K, value V) {
// 	return
// }

// func (r *DiskRun) DeleteKey(key K) {
// 	return
// }

func (r *DiskRun) NumElements() (num int) {
	num = r.capacity
	return
}
