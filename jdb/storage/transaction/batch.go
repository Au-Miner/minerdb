package transaction

import (
	"fmt"
	"github.com/bwmarrin/snowflake"
	"github.com/valyala/bytebufferpool"
	"jdb/jdb/common/constrants"
	"sync"
)

type BatchState = int

const (
	COMMITTED BatchState = iota
	ABORTED
)

// Batch 指db中一批操作的集合，被jdb视作事务
// Batch保证了原子性和隔离性（要么在内存中发生崩溃全部失败，要么存储到磁盘中）
// 通过lock_manager保证了事务的隔离性，目前支持可重复读（默认batch结束时自动释放batch期间获取的所有锁）
// 持久性需要设置Sync选项为true
type Batch struct {
	PendingWrites    []*LogRecord     // 存record
	PendingWritesMap map[uint64][]int // key -> record所在pendingWrites中的下标
	Options          constrants.BatchOptions
	Mu               sync.RWMutex
	Buffers          []*bytebufferpool.ByteBuffer
	Committed        bool            // 是否batch已经被committed
	Rollbacked       bool            // 是否batch已经被rollbacked
	BatchId          *snowflake.Node // 于生成分布式唯一 ID
	State            BatchState      // 用于lock_manager
}

func NewBatch(options constrants.BatchOptions) *Batch {
	batch := &Batch{
		Options:    options,
		Committed:  false,
		Rollbacked: false,
	}
	if !options.ReadOnly {
		node, err := snowflake.NewNode(1)
		if err != nil {
			panic(fmt.Sprintf("snowflake.NewNode(1) failed: %v", err))
		}
		batch.BatchId = node
	}
	return batch
}
