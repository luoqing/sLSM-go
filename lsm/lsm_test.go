package lsm

import (
	"fmt"
	"math/rand"
	"testing"
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
	rand.Seed(5)
	for i := 0; i < numInserts; i++ {
		d := rand.Intn(1000)
		key := K{
			Data: d,
		}
		value := V{
			Data: d,
		}
		if i == 0 {
			lsm.InsertKey(key, V_TOMBSTONE)
		}
		if d > rand.Intn(500) {
			lsm.InsertKey(key, value)
		} else {
			lsm.InsertKey(key, V_TOMBSTONE)
		}
	}
	lsm.PrintElts()
}

func TestLookup(t *testing.T) {
	key := K{
		Data: 1,
	}
	value := V{
		Data: 1,
	}
	lsm.InsertKey(key, value)
	found, value := lsm.Lookup(key)
	fmt.Println(found, value)
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
