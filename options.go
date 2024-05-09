package rosedb

import "os"

type Options struct {
	DirPath           string
	SegmentSize       int64
	BlockCache        uint32
	Sync              bool // true表示commit时候需要fsync
	BytesPerSync      uint32
	WatchQueueSize    uint64
	AutoMergeCronExpr string
}

type BatchOptions struct {
	Sync     bool // // true表示commit时候需要fsync
	ReadOnly bool
}

const (
	B  = 1
	KB = 1024 * B
	MB = 1024 * KB
	GB = 1024 * MB
)

var DefaultOptions = Options{
	DirPath:           tempDBDir(),
	SegmentSize:       1 * GB,
	BlockCache:        0,
	Sync:              false,
	BytesPerSync:      0,
	WatchQueueSize:    0,
	AutoMergeCronExpr: "",
}

var DefaultBatchOptions = BatchOptions{
	Sync:     true,
	ReadOnly: false,
}

func tempDBDir() string {
	dir, _ := os.MkdirTemp("", "rosedb-temp")
	return dir
}
