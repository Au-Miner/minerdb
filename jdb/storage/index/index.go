package index

import (
	"jdb/jdb/storage/wal"
)

// Indexer 是用于索引键和位置的接口
// 它用于存储数据在 WAL 中的键和位置
// 当数据库打开时，索引将被重新构建
// 可以通过实现此接口来实现自己的索引器
type Indexer interface {
	// Put 将键和位置放入索引中
	Put(key []byte, position *wal.ChunkPosition) *wal.ChunkPosition

	// Get 获取索引中键的位置
	Get(key []byte) *wal.ChunkPosition

	// Delete 删除键的索引
	Delete(key []byte) (*wal.ChunkPosition, bool)

	// Size 表示索引中的键数
	Size() int

	// Ascend 按升序遍历项，并为每个项调用处理程序函数
	// 如果处理程序函数返回 false，则停止迭代
	Ascend(handleFn func(key []byte, position *wal.ChunkPosition) (bool, error))

	// AscendRange 在 [startKey, endKey] 范围内按升序迭代，调用 handleFn
	// 如果 handleFn 返回 false，则停止
	AscendRange(startKey, endKey []byte, handleFn func(key []byte, position *wal.ChunkPosition) (bool, error))

	// AscendGreaterOrEqual 按升序迭代，从键 >= 给定键开始，调用 handleFn
	// 如果 handleFn 返回 false，则停止
	AscendGreaterOrEqual(key []byte, handleFn func(key []byte, position *wal.ChunkPosition) (bool, error))

	// Descend 按降序遍历项，并为每个项调用处理程序函数
	// 如果处理程序函数返回 false，则停止迭代
	Descend(handleFn func(key []byte, pos *wal.ChunkPosition) (bool, error))

	// DescendRange 在 [startKey, endKey] 范围内按降序迭代，调用 handleFn
	// 如果 handleFn 返回 false，则停止
	DescendRange(startKey, endKey []byte, handleFn func(key []byte, position *wal.ChunkPosition) (bool, error))

	// DescendLessOrEqual 按降序迭代，从键 <= 给定键开始，调用 handleFn
	// 如果 handleFn 返回 false，则停止
	DescendLessOrEqual(key []byte, handleFn func(key []byte, position *wal.ChunkPosition) (bool, error))
}

type IndexerType = byte

const (
	BTree IndexerType = iota
)

var indexType = BTree

func NewIndexer() Indexer {
	switch indexType {
	case BTree:
		return newBTree()
	default:
		panic("unexpected index type")
	}
}
