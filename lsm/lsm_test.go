package lsm

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

var lsm *LSM

func init() {
	eltsPerRun := 5
	numRuns := 5
	mergedFrac := 1.0
	bfFp := 0.9
	pageSize := 128
	diskRunsPerLevel := 2
	lsm = NewLSM(eltsPerRun, numRuns, mergedFrac, bfFp, pageSize, diskRunsPerLevel)

}

// 这个要测试能写入多个数据。
// 这个测试在merge的时候绝对有问题，需要一步步调试，看看问题在哪里，主要是activerun的数据不对
func TestInsertKey(t *testing.T) {
	numInserts := 1000
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < numInserts; i++ {
		d := rand.Intn(1000)
		key := K{
			Data: d,
		}
		value := V{
			Data: d,
		}
		if d > rand.Intn(500) {
			lsm.InsertKey(key, value)
		} else {
			lsm.InsertKey(key, V_TOMBSTONE)
		}
	}
	lsm.PrintElts()
}

func BenchmarkInsertKey(b *testing.B) {
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < b.N; i++ {
		d := rand.Intn(1000)
		key := K{
			Data: d,
		}
		value := V{
			Data: d,
		}
		if d > rand.Intn(500) {
			lsm.InsertKey(key, value)
		} else {
			lsm.InsertKey(key, V_TOMBSTONE)
		}
	}
}

// https://geektutu.com/post/hpg-benchmark.html
// 查询可以并发读取
func BenchmarkLookup(b *testing.B) {
	numInserts := 200
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < numInserts; i++ {
		d := rand.Intn(1000)
		key := K{
			Data: d,
		}
		value := V{
			Data: d,
		}
		lsm.InsertKey(key, value)

	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := rand.Intn(1000)
			key := K{
				Data: i,
			}
			// lsm.Lookup(key)
			found, value := lsm.Lookup(key)
			if found && value.Data != key.Data {
				b.Errorf("value error -key:%v -value:%v", key, value)
			}
			// fmt.Printf("find key:%d found:%v\n", key, found)
		}
	})
}

func TestDeleteKey(t *testing.T) {
	key := K{
		Data: 1,
	}
	lsm.DeleteKey(key)
	lsm.PrintElts()
	found, value := lsm.Lookup(key)
	fmt.Println(found, value)

}
