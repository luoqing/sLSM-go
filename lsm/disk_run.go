package lsm

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

// 磁盘的run
type DiskRun struct {
	Map           []KVPair // mmap， 将磁盘和虚拟内存映射，避免了用户态和内核态的多次拷贝
	Fd            *os.File
	Bf            *BloomFilter
	pageSize      int
	fencePointers []K // 存储多个key，按一定步长pageSize提取key，这样即使不存储pos，也知道key对应在哪个范围
	capacity      int
	filename      string
	level         int
	runID         int
	bf_fp         float64
	minKey        K
	maxKey        K
	mmap_ptr      unsafe.Pointer // 用于释放空间
}

// 【todo】capacity这个字段有啥用--- map的大小, 用来初始化map的大小，方便mmap
func NewDiskRun(capacity, pageSize, level int, runID int, bfFp float64) (r *DiskRun) {
	// 创建文件 C_level_runID.txt
	// mmap --- 主要为了创建map
	filename := fmt.Sprintf("C_%d_%d.txt", level, runID)
	fd, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0600)
	if err != nil {
		panic(err)
	}

	r = &DiskRun{
		// Map:           make([]KVPair, capacity),
		Bf:            NewBloomFilter(uint64(capacity), bfFp),
		Fd:            fd,
		pageSize:      pageSize,
		capacity:      0,
		fencePointers: make([]K, 0),
		filename:      filename,
		level:         level,
		runID:         runID,
		bf_fp:         bfFp,
	}
	// 24是slice的struct的大小， 是有三个记录了 len 和 cap， slice大小是24
	// filesize := int64(unsafe.Sizeof(KVPair{})) * int64(capacity) + 24
	filesize := int64(unsafe.Sizeof(KVPair{})) * int64(capacity)

	fmt.Println(filesize)
	// err = syscall.Ftruncate(int(file.Fd()), int64(size))
	// if err != nil {
	//     panic(err)
	// }

	_, err = fd.Write(make([]byte, filesize))
	if err != nil {
		err = fmt.Errorf("Error writing last byte of the file: %v", err)
		panic(err)
	}
	b, err := syscall.Mmap(int(fd.Fd()), 0, int(filesize), syscall.PROT_WRITE|syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		err = fmt.Errorf("mmap failed: %v", err)
		panic(err)
	}
	fmt.Println(len(b))
	// m := *(**[]KVPair)(unsafe.Pointer(&b))
	mslice := &SliceMock{
		array: uintptr(unsafe.Pointer(&b[0])),
		len:   capacity,
		cap:   capacity,
	}
	r.Map = *(*[]KVPair)(unsafe.Pointer(mslice))
	fmt.Printf("m:%d\n", len(r.Map))
	r.capacity = 0
	r.mmap_ptr = unsafe.Pointer(&b)
	return r
}

func (r *DiskRun) Close() (err error) {
	err = r.Fd.Close()
	if err != nil {
		return
	}
	b := *(*[]byte)(r.mmap_ptr)
	err = syscall.Munmap(b)
	if err != nil {
		return
	}
	return
}

// ？？释放就是doUnMap在对象进行销毁的时候
// 将长度为len的run数组写入到map的offset开始的位置
func (r *DiskRun) WriteData(run []KVPair, offset int, len int) {
	copy(r.Map[offset:(offset+len)], run[0:])
	r.capacity = offset + len // 而不是capacity = offset
}

// 根据map中的数据构建索引——bloomfilter + fencepointer + minkey, maxkey
func (r *DiskRun) ConstructIndex() {
	// map的大小是否是固定的呢？ 在创建run的时候应该是固定了为capacity
	var i int = 0
	for i < r.capacity {
		elem := r.Map[i]
		r.Bf.Add(elem.Key)
		if i%r.pageSize == 0 {
			r.fencePointers = append(r.fencePointers, elem.Key)
		}
		i += 1
	}
	// 由于map是有序数组，只需要取第一个和最后一个即是最小值和最大值
	r.minKey = r.Map[0].Key
	r.maxKey = r.Map[r.capacity-1].Key
}

// 根据fencepointer定位key的数据范围 start和end
// 前闭后闭 a >= start && b <= end
// 边界如何考虑
func (r *DiskRun) getFlankingFP(key K) (start int, end int) {
	// invalid
	if lessThan(key, r.minKey) {
		start = -2
		end = -1
		return
	}
	// invalid
	if moreThan(key, r.maxKey) {
		start = r.capacity + 1
		end = start + 1
		return
	}
	// invalid
	if len(r.fencePointers) == 0 {
		start = -2
		end = -1
		return
	}

	if len(r.fencePointers) == 1 {
		start = 0
		end = r.capacity
		return
	}

	if lessThan(key, r.fencePointers[1]) {
		start = 0
		end = r.pageSize
		return
	}

	if !lessThan(key, r.fencePointers[len(r.fencePointers)-1]) {
		start = (len(r.fencePointers) - 1) * r.pageSize
		end = r.capacity
		return
	}

	// 数据一定在栅栏的范围内
	left := 0
	right := len(r.fencePointers) - 1
	for left <= right {
		middle := (left + right) >> 1
		// key > r.Map[middle].Key
		if moreThan(key, r.fencePointers[middle]) {
			if lessThan(key, r.fencePointers[middle+1]) {
				start = middle * r.pageSize
				end = (middle + 1) * r.pageSize
				return
			}
			left = middle + 1
		} else if lessThan(key, r.fencePointers[middle]) {
			if moreThan(key, r.fencePointers[middle-1]) {
				start = (middle - 1) * r.pageSize
				end = middle * r.pageSize
				return
			}
			right = middle - 1
		} else {
			start = middle * r.pageSize
			end = start
			return
		}
	}
	return
}

// 然后zai [start, end) 中查找key的位置
func (r *DiskRun) binarySearch(start int, n int, key K) (found bool, pos int) {
	left := start
	right := start + n - 1
	middle := (left + right) >> 1
	for left <= right {
		// key > r.Map[middle].Key
		if moreThan(key, r.Map[middle].Key) {
			left = middle + 1
		} else if key == r.Map[middle].Key {
			found = true
			pos = middle
			return
		} else {
			right = middle - 1
		}
		middle = (left + right) >> 1
	}
	pos = left // 返回要插入的位置, left是要插入的位置
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
