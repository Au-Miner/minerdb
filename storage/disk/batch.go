package disk

import (
	"fmt"
	"github.com/bwmarrin/snowflake"
	"github.com/rosedblabs/rosedb/v2/common/constrants"
	"github.com/valyala/bytebufferpool"
	"sync"
)

type BatchState = int

const (
	COMMITTED BatchState = iota
	ABORTED
)

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
