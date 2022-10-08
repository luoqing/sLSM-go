package lsm

// 磁盘的run
type DiskRun struct {
	Map []KVPair
}

type DiskLevel struct {
}

type DiskPart struct {
	DiskLevels       []*DiskLevel // 磁盘有多层，需要做merge
	numDiskLevels    int
	diskRunsPerLevel int
	pageSize         int
}
