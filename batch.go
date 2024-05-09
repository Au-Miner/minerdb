package rosedb

import (
	"bytes"
	"fmt"
	"github.com/rosedblabs/rosedb/v2/utils"
	"sync"
	"time"

	"github.com/bwmarrin/snowflake"
	"github.com/valyala/bytebufferpool"
)

// Batch 指db中一批操作的集合，被jdb视作事务
// 目前不能成为事务，不能保证隔离性
// 目前想要保证原子性、一致性、持久性，需要设置Sync选项为true
type Batch struct {
	db               *DB
	pendingWrites    []*LogRecord     // 存record
	pendingWritesMap map[uint64][]int // key -> record所在pendingWrites中的下标
	options          BatchOptions
	mu               sync.RWMutex
	committed        bool            // 是否batch已经被committed
	rollbacked       bool            // 是否batch已经被rollbacked
	batchId          *snowflake.Node // 于生成分布式唯一 ID
	buffers          []*bytebufferpool.ByteBuffer
}

// NewBatch 新建batch
func (db *DB) NewBatch(options BatchOptions) *Batch {
	batch := &Batch{
		db:         db,
		options:    options,
		committed:  false,
		rollbacked: false,
	}
	if !options.ReadOnly {
		node, err := snowflake.NewNode(1)
		if err != nil {
			panic(fmt.Sprintf("snowflake.NewNode(1) failed: %v", err))
		}
		batch.batchId = node
	}
	batch.lock()
	return batch
}

func newBatch() interface{} {
	node, err := snowflake.NewNode(1)
	if err != nil {
		panic(fmt.Sprintf("snowflake.NewNode(1) failed: %v", err))
	}
	return &Batch{
		options: DefaultBatchOptions,
		batchId: node,
	}
}

func newRecord() interface{} {
	return &LogRecord{}
}

func (b *Batch) init(rdonly, sync bool, db *DB) *Batch {
	b.options.ReadOnly = rdonly
	b.options.Sync = sync
	b.db = db
	// 对batch加锁
	b.lock()
	return b
}

func (b *Batch) reset() {
	b.db = nil
	b.pendingWrites = b.pendingWrites[:0]
	b.pendingWritesMap = nil
	b.committed = false
	b.rollbacked = false
	// buf是用来创建encRecord的，batch使用完要清空
	for _, buf := range b.buffers {
		bytebufferpool.Put(buf)
	}
	b.buffers = b.buffers[:0]
}

func (b *Batch) lock() {
	if b.options.ReadOnly {
		b.db.mu.RLock()
	} else {
		b.db.mu.Lock()
	}
}

func (b *Batch) unlock() {
	if b.options.ReadOnly {
		b.db.mu.RUnlock()
	} else {
		b.db.mu.Unlock()
	}
}

// Put 在batch中添加一个put操作
func (b *Batch) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	if b.db.closed {
		return ErrDBClosed
	}
	if b.options.ReadOnly {
		return ErrReadOnlyBatch
	}
	b.mu.Lock()
	// 写入batch的pendingWrites
	var record = b.lookupPendingWrites(key)
	if record == nil {
		// 如果key不存在于pendingWrites中，写入一个新的record
		// 当batch提交或回滚时，record会被放回到pool中
		record = b.db.recordPool.Get().(*LogRecord)
		b.appendPendingWrites(key, record)
	}
	record.Key, record.Value = key, value
	record.Type, record.Expire = LogRecordNormal, 0
	b.mu.Unlock()
	return nil
}

// PutWithTTL 相比于put只是多了一个ttl
func (b *Batch) PutWithTTL(key []byte, value []byte, ttl time.Duration) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	if b.db.closed {
		return ErrDBClosed
	}
	if b.options.ReadOnly {
		return ErrReadOnlyBatch
	}

	b.mu.Lock()
	var record = b.lookupPendingWrites(key)
	if record == nil {
		record = b.db.recordPool.Get().(*LogRecord)
		b.appendPendingWrites(key, record)
	}
	record.Key, record.Value = key, value
	record.Type, record.Expire = LogRecordNormal, time.Now().Add(ttl).UnixNano()
	b.mu.Unlock()
	return nil
}

// Get 从当前batch或者dataFiles中获取value
func (b *Batch) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}
	if b.db.closed {
		return nil, ErrDBClosed
	}
	now := time.Now().UnixNano()
	b.mu.RLock()
	var record = b.lookupPendingWrites(key)
	b.mu.RUnlock()
	// 如果当前batch的pendingWrites中存在key，直接返回value
	if record != nil {
		if record.Type == LogRecordDeleted || record.IsExpired(now) {
			return nil, ErrKeyNotFound
		}
		return record.Value, nil
	}
	// 从dataFiles获取value
	chunkPosition := b.db.index.Get(key)
	if chunkPosition == nil {
		return nil, ErrKeyNotFound
	}
	chunk, err := b.db.dataFiles.Read(chunkPosition)
	if err != nil {
		return nil, err
	}
	record = decodeLogRecord(chunk)
	// 检查logRecord是否被删除或过期
	if record.Type == LogRecordDeleted {
		panic("Deleted data cannot exist in the index")
	}
	if record.IsExpired(now) {
		b.db.index.Delete(record.Key)
		return nil, ErrKeyNotFound
	}
	return record.Value, nil
}

// Delete 如果batch中存在该key，则修改record，否则创建record并存入pendingWrites
func (b *Batch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	if b.db.closed {
		return ErrDBClosed
	}
	if b.options.ReadOnly {
		return ErrReadOnlyBatch
	}
	b.mu.Lock()
	var exist bool
	var record = b.lookupPendingWrites(key)
	if record != nil {
		record.Type = LogRecordDeleted
		record.Value = nil
		record.Expire = 0
		exist = true
	}
	if !exist {
		record = &LogRecord{
			Key:  key,
			Type: LogRecordDeleted,
		}
		b.appendPendingWrites(key, record)
	}
	b.mu.Unlock()
	return nil
}

// Exist 检查是否key存在于batch或dataFiles中
func (b *Batch) Exist(key []byte) (bool, error) {
	if len(key) == 0 {
		return false, ErrKeyIsEmpty
	}
	if b.db.closed {
		return false, ErrDBClosed
	}
	now := time.Now().UnixNano()
	// 检查是否存在batch中
	b.mu.RLock()
	var record = b.lookupPendingWrites(key)
	b.mu.RUnlock()
	if record != nil {
		return record.Type != LogRecordDeleted && !record.IsExpired(now), nil
	}
	// 检查是否存在dataFiles中
	position := b.db.index.Get(key)
	if position == nil {
		return false, nil
	}
	chunk, err := b.db.dataFiles.Read(position)
	if err != nil {
		return false, err
	}
	record = decodeLogRecord(chunk)
	if record.Type == LogRecordDeleted || record.IsExpired(now) {
		b.db.index.Delete(record.Key)
		return false, nil
	}
	return true, nil
}

// Expire 手动设置key对应的ttl
func (b *Batch) Expire(key []byte, ttl time.Duration) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	if b.db.closed {
		return ErrDBClosed
	}
	if b.options.ReadOnly {
		return ErrReadOnlyBatch
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	var record = b.lookupPendingWrites(key)

	// 如果pendingWrites中有，则直接更新
	if record != nil {
		if record.Type == LogRecordDeleted || record.IsExpired(time.Now().UnixNano()) {
			return ErrKeyNotFound
		}
		record.Expire = time.Now().Add(ttl).UnixNano()
		return nil
	}
	// 如果pendingWrites中没有，则从wal中找
	position := b.db.index.Get(key)
	if position == nil {
		return ErrKeyNotFound
	}
	chunk, err := b.db.dataFiles.Read(position)
	if err != nil {
		return err
	}

	now := time.Now()
	record = decodeLogRecord(chunk)
	// 如果record已经被删除或者过期，无法设置ttl
	if record.Type == LogRecordDeleted || record.IsExpired(now.UnixNano()) {
		b.db.index.Delete(key)
		return ErrKeyNotFound
	}
	// 可以更新
	record.Expire = now.Add(ttl).UnixNano()
	b.appendPendingWrites(key, record)
	return nil
}

// TTL 获取key的ttl
func (b *Batch) TTL(key []byte) (time.Duration, error) {
	if len(key) == 0 {
		return -1, ErrKeyIsEmpty
	}
	if b.db.closed {
		return -1, ErrDBClosed
	}

	now := time.Now()
	b.mu.Lock()
	defer b.mu.Unlock()

	// 先查找batch中的pendingWrites有没有对应的key
	var record = b.lookupPendingWrites(key)
	if record != nil {
		if record.Expire == 0 {
			return -1, nil
		}
		if record.Type == LogRecordDeleted || record.IsExpired(now.UnixNano()) {
			return -1, ErrKeyNotFound
		}
		return time.Duration(record.Expire - now.UnixNano()), nil
	}

	// 没有的话从wal中找，思路和get类似
	position := b.db.index.Get(key)
	if position == nil {
		return -1, ErrKeyNotFound
	}
	chunk, err := b.db.dataFiles.Read(position)
	if err != nil {
		return -1, err
	}

	record = decodeLogRecord(chunk)
	if record.Type == LogRecordDeleted {
		return -1, ErrKeyNotFound
	}
	if record.IsExpired(now.UnixNano()) {
		b.db.index.Delete(key)
		return -1, ErrKeyNotFound
	}

	if record.Expire > 0 {
		return time.Duration(record.Expire - now.UnixNano()), nil
	}
	return -1, nil
}

// Commit 提交一个batch，如果batch是只读的或者为空，直接返回
func (b *Batch) Commit() error {
	defer b.unlock()
	if b.db.closed {
		return ErrDBClosed
	}
	if b.options.ReadOnly || len(b.pendingWrites) == 0 {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.committed {
		return ErrBatchCommitted
	}
	if b.rollbacked {
		return ErrBatchRollbacked
	}

	batchId := b.batchId.Generate()
	now := time.Now().UnixNano()
	// 将record信息、头文件写入buf中，并将buf.Bytes()写入wal文件
	for _, record := range b.pendingWrites {
		buf := bytebufferpool.Get()
		b.buffers = append(b.buffers, buf)
		record.BatchId = uint64(batchId)
		encRecord := encodeLogRecord(record, b.db.encodeHeader, buf)
		b.db.dataFiles.PendingWrites(encRecord)
	}
	// 在PendingWrites中写一个record，表示batch结束
	buf := bytebufferpool.Get()
	b.buffers = append(b.buffers, buf)
	endRecord := encodeLogRecord(&LogRecord{
		Key:  batchId.Bytes(),
		Type: LogRecordBatchFinished,
	}, b.db.encodeHeader, buf)
	b.db.dataFiles.PendingWrites(endRecord)
	// 把wal.pendingWrites写入到WAL中，但不sync
	chunkPositions, err := b.db.dataFiles.WriteAll()
	if err != nil {
		b.db.dataFiles.ClearPendingWrites()
		return err
	}
	if len(chunkPositions) != len(b.pendingWrites)+1 {
		panic("chunk positions length is not equal to pending writes length")
	}
	// 如果batch的Sync选项为true，且db不需要手动sync
	if b.options.Sync && !b.db.options.Sync {
		if err := b.db.dataFiles.Sync(); err != nil {
			return err
		}
	}
	// 写入index
	for i, record := range b.pendingWrites {
		if record.Type == LogRecordDeleted || record.IsExpired(now) {
			b.db.index.Delete(record.Key)
		} else {
			b.db.index.Put(record.Key, chunkPositions[i])
		}
		if b.db.options.WatchQueueSize > 0 {
			e := &Event{Key: record.Key, Value: record.Value, BatchId: record.BatchId}
			if record.Type == LogRecordDeleted {
				e.Action = WatchActionDelete
			} else {
				e.Action = WatchActionPut
			}
			b.db.watcher.putEvent(e)
		}
		b.db.recordPool.Put(record)
	}
	b.committed = true
	return nil
}

// Rollback 丢弃一个未提交的batch，清除缓冲数据并释放锁
func (b *Batch) Rollback() error {
	defer b.unlock()
	if b.db.closed {
		return ErrDBClosed
	}
	if b.committed {
		return ErrBatchCommitted
	}
	if b.rollbacked {
		return ErrBatchRollbacked
	}
	for _, buf := range b.buffers {
		bytebufferpool.Put(buf)
	}
	if !b.options.ReadOnly {
		// 清空batch中的pendingWrites
		for _, record := range b.pendingWrites {
			b.db.recordPool.Put(record)
		}
		b.pendingWrites = b.pendingWrites[:0]
		for key := range b.pendingWritesMap {
			delete(b.pendingWritesMap, key)
		}
	}
	b.rollbacked = true
	return nil
}

// lookupPendingWrites 如果pendingWrites中存在key，返回record
func (b *Batch) lookupPendingWrites(key []byte) *LogRecord {
	if len(b.pendingWritesMap) == 0 {
		return nil
	}

	hashKey := utils.MemHash(key)
	// pendingWritesMap存的是key对应在pendingWrites中的下标
	for _, entry := range b.pendingWritesMap[hashKey] {
		if bytes.Compare(b.pendingWrites[entry].Key, key) == 0 {
			return b.pendingWrites[entry]
		}
	}
	return nil
}

// 增加新record到pendingWrites和pendingWritesMap.
func (b *Batch) appendPendingWrites(key []byte, record *LogRecord) {
	b.pendingWrites = append(b.pendingWrites, record)
	if b.pendingWritesMap == nil {
		b.pendingWritesMap = make(map[uint64][]int)
	}
	hashKey := utils.MemHash(key)
	b.pendingWritesMap[hashKey] = append(b.pendingWritesMap[hashKey], len(b.pendingWrites)-1)
}
