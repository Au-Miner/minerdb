package execution

import (
	"bytes"
	"minerdb/minerdb/common/exception"
	"minerdb/minerdb/common/utils"
	"minerdb/minerdb/concurrency"
	transaction2 "minerdb/minerdb/storage/transaction"
	"minerdb/minerdb/watch"
	"time"

	"github.com/valyala/bytebufferpool"
)

type CommonExecutor struct {
	db    *MinerDB
	batch transaction2.Batch
}

func (ce *CommonExecutor) init(rdonly, sync bool, db *MinerDB) *CommonExecutor {
	ce.batch.Options.ReadOnly = rdonly
	ce.batch.Options.Sync = sync
	ce.db = db
	// 对batch加锁
	ce.lock()
	return ce
}

func (ce *CommonExecutor) reset() {
	ce.db = nil
	ce.batch.PendingWrites = ce.batch.PendingWrites[:0]
	ce.batch.PendingWritesMap = nil
	ce.batch.Committed = false
	ce.batch.Rollbacked = false
	// buf是用来创建encRecord的，batch使用完要清空
	for _, buf := range ce.batch.Buffers {
		bytebufferpool.Put(buf)
	}
	ce.batch.Buffers = ce.batch.Buffers[:0]
}

func (ce *CommonExecutor) lock() {
	if ce.batch.Options.ReadOnly {
		ce.db.mu.RLock()
	} else {
		ce.db.mu.Lock()
	}
}

func (ce *CommonExecutor) unlock() {
	if ce.batch.Options.ReadOnly {
		ce.db.mu.RUnlock()
	} else {
		ce.db.mu.Unlock()
	}
}

// Put 在batch中添加一个put操作
func (ce *CommonExecutor) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return exception.ErrKeyIsEmpty
	}
	if ce.db.isClose {
		return exception.ErrDBClose
	}
	if ce.batch.Options.ReadOnly {
		return exception.ErrReadOnlyBatch
	}
	ce.batch.Mu.Lock()
	// 写入batch的pendingWrites
	var record = ce.lookupPendingWrites(key)
	if record == nil {
		// 如果key不存在于pendingWrites中，写入一个新的record
		// 当batch提交或回滚时，record会被放回到pool中
		record = ce.db.recordPool.Get().(*transaction2.LogRecord)
		ce.appendPendingWrites(key, record)
	}
	record.Key, record.Value = key, value
	record.Type, record.Expire = transaction2.LogRecordNormal, 0
	ce.batch.Mu.Unlock()
	return nil
}

func (ce *CommonExecutor) ConcurrentPut(key []byte, value []byte, lockManager *concurrency.LockManager) error {
	if len(key) == 0 {
		return exception.ErrKeyIsEmpty
	}
	if ce.db.isClose {
		return exception.ErrDBClose
	}
	if ce.batch.Options.ReadOnly {
		return exception.ErrReadOnlyBatch
	}
	ce.batch.Mu.Lock()
	err := lockManager.LockRow(&ce.batch, concurrency.EXCLUSIVE, key)
	if err != nil {
		return err
	}
	// 写入batch的pendingWrites
	var record = ce.lookupPendingWrites(key)
	if record == nil {
		// 如果key不存在于pendingWrites中，写入一个新的record
		// 当batch提交或回滚时，record会被放回到pool中
		record = ce.db.recordPool.Get().(*transaction2.LogRecord)
		ce.appendPendingWrites(key, record)
	}
	record.Key, record.Value = key, value
	record.Type, record.Expire = transaction2.LogRecordNormal, 0
	ce.batch.Mu.Unlock()
	return nil
}

// PutWithTTL 相比于put只是多了一个ttl
func (ce *CommonExecutor) PutWithTTL(key []byte, value []byte, ttl time.Duration) error {
	if len(key) == 0 {
		return exception.ErrKeyIsEmpty
	}
	if ce.db.isClose {
		return exception.ErrDBClose
	}
	if ce.batch.Options.ReadOnly {
		return exception.ErrReadOnlyBatch
	}
	ce.batch.Mu.Lock()
	var record = ce.lookupPendingWrites(key)
	if record == nil {
		record = ce.db.recordPool.Get().(*transaction2.LogRecord)
		ce.appendPendingWrites(key, record)
	}
	record.Key, record.Value = key, value
	record.Type, record.Expire = transaction2.LogRecordNormal, time.Now().Add(ttl).UnixNano()
	ce.batch.Mu.Unlock()
	return nil
}

// Get 从当前batch或者dataFiles中获取value
func (ce *CommonExecutor) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, exception.ErrKeyIsEmpty
	}
	if ce.db.isClose {
		return nil, exception.ErrDBClose
	}
	now := time.Now().UnixNano()
	ce.batch.Mu.RLock()
	var record = ce.lookupPendingWrites(key)
	ce.batch.Mu.RUnlock()
	// 如果当前batch的pendingWrites中存在key，直接返回value
	if record != nil {
		if record.Type == transaction2.LogRecordDeleted || record.IsExpired(now) {
			return nil, exception.ErrKeyNotFound
		}
		return record.Value, nil
	}
	// 从dataFiles获取value
	chunkPosition := ce.db.index.Get(key)
	if chunkPosition == nil {
		return nil, exception.ErrKeyNotFound
	}
	chunk, err := ce.db.dataFiles.Read(chunkPosition)
	if err != nil {
		return nil, err
	}
	record = transaction2.DecodeLogRecord(chunk)
	// 检查logRecord是否被删除或过期
	if record.Type == transaction2.LogRecordDeleted {
		panic("Deleted data cannot exist in the index")
	}
	if record.IsExpired(now) {
		ce.db.index.Delete(record.Key)
		return nil, exception.ErrKeyNotFound
	}
	return record.Value, nil
}

func (ce *CommonExecutor) ConcurrentGet(key []byte, lockManager *concurrency.LockManager) ([]byte, error) {
	if len(key) == 0 {
		return nil, exception.ErrKeyIsEmpty
	}
	if ce.db.isClose {
		return nil, exception.ErrDBClose
	}
	err := lockManager.LockRow(&ce.batch, concurrency.SHARED, key)
	if err != nil {
		return nil, err
	}
	now := time.Now().UnixNano()
	ce.batch.Mu.RLock()
	var record = ce.lookupPendingWrites(key)
	ce.batch.Mu.RUnlock()
	// 如果当前batch的pendingWrites中存在key，直接返回value
	if record != nil {
		if record.Type == transaction2.LogRecordDeleted || record.IsExpired(now) {
			return nil, exception.ErrKeyNotFound
		}
		return record.Value, nil
	}
	// 从dataFiles获取value
	chunkPosition := ce.db.index.Get(key)
	if chunkPosition == nil {
		return nil, exception.ErrKeyNotFound
	}
	chunk, err := ce.db.dataFiles.Read(chunkPosition)
	if err != nil {
		return nil, err
	}
	record = transaction2.DecodeLogRecord(chunk)
	// 检查logRecord是否被删除或过期
	if record.Type == transaction2.LogRecordDeleted {
		panic("Deleted data cannot exist in the index")
	}
	if record.IsExpired(now) {
		ce.db.index.Delete(record.Key)
		return nil, exception.ErrKeyNotFound
	}
	return record.Value, nil
}

// Delete 如果batch中存在该key，则修改record，否则创建record并存入pendingWrites
func (ce *CommonExecutor) Delete(key []byte) error {
	if len(key) == 0 {
		return exception.ErrKeyIsEmpty
	}
	if ce.db.isClose {
		return exception.ErrDBClose
	}
	if ce.batch.Options.ReadOnly {
		return exception.ErrReadOnlyBatch
	}
	ce.batch.Mu.Lock()
	var exist bool
	var record = ce.lookupPendingWrites(key)
	if record != nil {
		record.Type = transaction2.LogRecordDeleted
		record.Value = nil
		record.Expire = 0
		exist = true
	}
	if !exist {
		record = &transaction2.LogRecord{
			Key:  key,
			Type: transaction2.LogRecordDeleted,
		}
		ce.appendPendingWrites(key, record)
	}
	ce.batch.Mu.Unlock()
	return nil
}

func (ce *CommonExecutor) ConcurrentDelete(key []byte, lockManager *concurrency.LockManager) error {
	if len(key) == 0 {
		return exception.ErrKeyIsEmpty
	}
	if ce.db.isClose {
		return exception.ErrDBClose
	}
	if ce.batch.Options.ReadOnly {
		return exception.ErrReadOnlyBatch
	}
	ce.batch.Mu.Lock()
	err := lockManager.LockRow(&ce.batch, concurrency.EXCLUSIVE, key)
	if err != nil {
		return err
	}
	var exist bool
	var record = ce.lookupPendingWrites(key)
	if record != nil {
		record.Type = transaction2.LogRecordDeleted
		record.Value = nil
		record.Expire = 0
		exist = true
	}
	if !exist {
		record = &transaction2.LogRecord{
			Key:  key,
			Type: transaction2.LogRecordDeleted,
		}
		ce.appendPendingWrites(key, record)
	}
	ce.batch.Mu.Unlock()
	return nil
}

// Exist 检查是否key存在于batch或dataFiles中
func (ce *CommonExecutor) Exist(key []byte) (bool, error) {
	if len(key) == 0 {
		return false, exception.ErrKeyIsEmpty
	}
	if ce.db.isClose {
		return false, exception.ErrDBClose
	}
	now := time.Now().UnixNano()
	// 检查是否存在batch中
	ce.batch.Mu.RLock()
	var record = ce.lookupPendingWrites(key)
	ce.batch.Mu.RUnlock()
	if record != nil {
		return record.Type != transaction2.LogRecordDeleted && !record.IsExpired(now), nil
	}
	// 检查是否存在dataFiles中
	position := ce.db.index.Get(key)
	if position == nil {
		return false, nil
	}
	chunk, err := ce.db.dataFiles.Read(position)
	if err != nil {
		return false, err
	}
	record = transaction2.DecodeLogRecord(chunk)
	if record.Type == transaction2.LogRecordDeleted || record.IsExpired(now) {
		ce.db.index.Delete(record.Key)
		return false, nil
	}
	return true, nil
}

// Expire 手动设置key对应的ttl
func (ce *CommonExecutor) Expire(key []byte, ttl time.Duration) error {
	if len(key) == 0 {
		return exception.ErrKeyIsEmpty
	}
	if ce.db.isClose {
		return exception.ErrDBClose
	}
	if ce.batch.Options.ReadOnly {
		return exception.ErrReadOnlyBatch
	}

	ce.batch.Mu.Lock()
	defer ce.batch.Mu.Unlock()
	var record = ce.lookupPendingWrites(key)

	// 如果pendingWrites中有，则直接更新
	if record != nil {
		if record.Type == transaction2.LogRecordDeleted || record.IsExpired(time.Now().UnixNano()) {
			return exception.ErrKeyNotFound
		}
		record.Expire = time.Now().Add(ttl).UnixNano()
		return nil
	}
	// 如果pendingWrites中没有，则从wal中找
	position := ce.db.index.Get(key)
	if position == nil {
		return exception.ErrKeyNotFound
	}
	chunk, err := ce.db.dataFiles.Read(position)
	if err != nil {
		return err
	}

	now := time.Now()
	record = transaction2.DecodeLogRecord(chunk)
	// 如果record已经被删除或者过期，无法设置ttl
	if record.Type == transaction2.LogRecordDeleted || record.IsExpired(now.UnixNano()) {
		ce.db.index.Delete(key)
		return exception.ErrKeyNotFound
	}
	// 可以更新
	record.Expire = now.Add(ttl).UnixNano()
	ce.appendPendingWrites(key, record)
	return nil
}

// TTL 获取key的ttl
func (ce *CommonExecutor) TTL(key []byte) (time.Duration, error) {
	if len(key) == 0 {
		return -1, exception.ErrKeyIsEmpty
	}
	if ce.db.isClose {
		return -1, exception.ErrDBClose
	}

	now := time.Now()
	ce.batch.Mu.Lock()
	defer ce.batch.Mu.Unlock()

	// 先查找batch中的pendingWrites有没有对应的key
	var record = ce.lookupPendingWrites(key)
	if record != nil {
		if record.Expire == 0 {
			return -1, nil
		}
		if record.Type == transaction2.LogRecordDeleted || record.IsExpired(now.UnixNano()) {
			return -1, exception.ErrKeyNotFound
		}
		return time.Duration(record.Expire - now.UnixNano()), nil
	}

	// 没有的话从wal中找，思路和get类似
	position := ce.db.index.Get(key)
	if position == nil {
		return -1, exception.ErrKeyNotFound
	}
	chunk, err := ce.db.dataFiles.Read(position)
	if err != nil {
		return -1, err
	}

	record = transaction2.DecodeLogRecord(chunk)
	if record.Type == transaction2.LogRecordDeleted {
		return -1, exception.ErrKeyNotFound
	}
	if record.IsExpired(now.UnixNano()) {
		ce.db.index.Delete(key)
		return -1, exception.ErrKeyNotFound
	}

	if record.Expire > 0 {
		return time.Duration(record.Expire - now.UnixNano()), nil
	}
	return -1, nil
}

// Commit 提交一个batch，如果batch是只读的或者为空，直接返回
func (ce *CommonExecutor) Commit() error {
	defer ce.unlock()
	if ce.db.isClose {
		return exception.ErrDBClose
	}
	if ce.batch.Options.ReadOnly || len(ce.batch.PendingWrites) == 0 {
		return nil
	}
	ce.batch.Mu.Lock()
	defer ce.batch.Mu.Unlock()
	if ce.batch.Committed {
		return exception.ErrBatchCommitted
	}
	if ce.batch.Rollbacked {
		return exception.ErrBatchRollbacked
	}

	batchId := ce.batch.BatchId.Generate()
	now := time.Now().UnixNano()
	// 将record信息、头文件写入buf中，并将buf.Bytes()写入wal文件
	for _, record := range ce.batch.PendingWrites {
		buf := bytebufferpool.Get()
		ce.batch.Buffers = append(ce.batch.Buffers, buf)
		record.BatchId = uint64(batchId)
		encRecord := transaction2.EncodeLogRecord(record, ce.db.encodeHeader, buf)
		ce.db.dataFiles.PendingWrites(encRecord)
	}
	// 在PendingWrites中写一个record，表示batch结束
	buf := bytebufferpool.Get()
	ce.batch.Buffers = append(ce.batch.Buffers, buf)
	endRecord := transaction2.EncodeLogRecord(&transaction2.LogRecord{
		Key:  batchId.Bytes(),
		Type: transaction2.LogRecordBatchFinished,
	}, ce.db.encodeHeader, buf)
	ce.db.dataFiles.PendingWrites(endRecord)
	// 把wal.pendingWrites写入到WAL中，但不sync
	chunkPositions, err := ce.db.dataFiles.WriteAll()
	if err != nil {
		ce.db.dataFiles.ClearPendingWrites()
		return err
	}
	if len(chunkPositions) != len(ce.batch.PendingWrites)+1 {
		panic("chunk positions length is not equal to pending writes length")
	}
	// 如果batch的Sync选项为true，且db不需要手动sync
	if ce.batch.Options.Sync && !ce.db.options.Sync {
		if err := ce.db.dataFiles.Sync(); err != nil {
			return err
		}
	}
	// 写入index
	for i, record := range ce.batch.PendingWrites {
		if record.Type == transaction2.LogRecordDeleted || record.IsExpired(now) {
			ce.db.index.Delete(record.Key)
		} else {
			ce.db.index.Put(record.Key, chunkPositions[i])
		}
		if ce.db.options.WatchQueueSize > 0 {
			e := &watch.Event{Key: record.Key, Value: record.Value, BatchId: record.BatchId}
			if record.Type == transaction2.LogRecordDeleted {
				e.Action = watch.WatchActionDelete
			} else {
				e.Action = watch.WatchActionPut
			}
			ce.db.watcher.PutEvent(e)
		}
		ce.db.recordPool.Put(record)
	}
	ce.batch.Committed = true
	return nil
}

func (ce *CommonExecutor) ConcurrentCommit(key []byte, lockManager *concurrency.LockManager) error {
	defer ce.unlock()
	if ce.db.isClose {
		return exception.ErrDBClose
	}
	if ce.batch.Options.ReadOnly || len(ce.batch.PendingWrites) == 0 {
		return nil
	}
	ce.batch.Mu.Lock()
	defer ce.batch.Mu.Unlock()
	if ce.batch.Committed {
		return exception.ErrBatchCommitted
	}
	if ce.batch.Rollbacked {
		return exception.ErrBatchRollbacked
	}

	batchId := ce.batch.BatchId.Generate()
	now := time.Now().UnixNano()
	// 将record信息、头文件写入buf中，并将buf.Bytes()写入wal文件
	for _, record := range ce.batch.PendingWrites {
		buf := bytebufferpool.Get()
		ce.batch.Buffers = append(ce.batch.Buffers, buf)
		record.BatchId = uint64(batchId)
		encRecord := transaction2.EncodeLogRecord(record, ce.db.encodeHeader, buf)
		ce.db.dataFiles.PendingWrites(encRecord)
	}
	// 在PendingWrites中写一个record，表示batch结束
	buf := bytebufferpool.Get()
	ce.batch.Buffers = append(ce.batch.Buffers, buf)
	endRecord := transaction2.EncodeLogRecord(&transaction2.LogRecord{
		Key:  batchId.Bytes(),
		Type: transaction2.LogRecordBatchFinished,
	}, ce.db.encodeHeader, buf)
	ce.db.dataFiles.PendingWrites(endRecord)
	// 把wal.pendingWrites写入到WAL中，但不sync
	chunkPositions, err := ce.db.dataFiles.WriteAll()
	if err != nil {
		ce.db.dataFiles.ClearPendingWrites()
		return err
	}
	if len(chunkPositions) != len(ce.batch.PendingWrites)+1 {
		panic("chunk positions length is not equal to pending writes length")
	}
	// 如果batch的Sync选项为true，且db不需要手动sync
	if ce.batch.Options.Sync && !ce.db.options.Sync {
		if err := ce.db.dataFiles.Sync(); err != nil {
			return err
		}
	}
	// 写入index
	for i, record := range ce.batch.PendingWrites {
		if record.Type == transaction2.LogRecordDeleted || record.IsExpired(now) {
			ce.db.index.Delete(record.Key)
		} else {
			ce.db.index.Put(record.Key, chunkPositions[i])
		}
		if ce.db.options.WatchQueueSize > 0 {
			e := &watch.Event{Key: record.Key, Value: record.Value, BatchId: record.BatchId}
			if record.Type == transaction2.LogRecordDeleted {
				e.Action = watch.WatchActionDelete
			} else {
				e.Action = watch.WatchActionPut
			}
			ce.db.watcher.PutEvent(e)
		}
		ce.db.recordPool.Put(record)
	}
	ce.batch.Committed = true
	lockManager.UnlockRow(&ce.batch, key)
	return nil
}

// Rollback 丢弃一个未提交的batch，清除缓冲数据并释放锁
func (ce *CommonExecutor) Rollback() error {
	defer ce.unlock()
	if ce.db.isClose {
		return exception.ErrDBClose
	}
	if ce.batch.Committed {
		return exception.ErrBatchCommitted
	}
	if ce.batch.Rollbacked {
		return exception.ErrBatchRollbacked
	}
	for _, buf := range ce.batch.Buffers {
		bytebufferpool.Put(buf)
	}
	if !ce.batch.Options.ReadOnly {
		// 清空batch中的pendingWrites
		for _, record := range ce.batch.PendingWrites {
			ce.db.recordPool.Put(record)
		}
		ce.batch.PendingWrites = ce.batch.PendingWrites[:0]
		for key := range ce.batch.PendingWritesMap {
			delete(ce.batch.PendingWritesMap, key)
		}
	}
	ce.batch.Rollbacked = true
	return nil
}

func (ce *CommonExecutor) ConcurrentRollback(key []byte, lockManager *concurrency.LockManager) interface{} {
	defer ce.unlock()
	if ce.db.isClose {
		return exception.ErrDBClose
	}
	if ce.batch.Committed {
		return exception.ErrBatchCommitted
	}
	if ce.batch.Rollbacked {
		return exception.ErrBatchRollbacked
	}
	for _, buf := range ce.batch.Buffers {
		bytebufferpool.Put(buf)
	}
	if !ce.batch.Options.ReadOnly {
		// 清空batch中的pendingWrites
		for _, record := range ce.batch.PendingWrites {
			ce.db.recordPool.Put(record)
		}
		ce.batch.PendingWrites = ce.batch.PendingWrites[:0]
		for key := range ce.batch.PendingWritesMap {
			delete(ce.batch.PendingWritesMap, key)
		}
	}
	ce.batch.Rollbacked = true
	lockManager.UnlockRow(&ce.batch, key)
	return nil
}

// lookupPendingWrites 如果pendingWrites中存在key，返回record
func (ce *CommonExecutor) lookupPendingWrites(key []byte) *transaction2.LogRecord {
	if len(ce.batch.PendingWritesMap) == 0 {
		return nil
	}

	hashKey := utils.MemHash(key)
	// pendingWritesMap存的是key对应在pendingWrites中的下标
	for _, entry := range ce.batch.PendingWritesMap[hashKey] {
		if bytes.Compare(ce.batch.PendingWrites[entry].Key, key) == 0 {
			return ce.batch.PendingWrites[entry]
		}
	}
	return nil
}

// 增加新record到pendingWrites和pendingWritesMap.
func (ce *CommonExecutor) appendPendingWrites(key []byte, record *transaction2.LogRecord) {
	ce.batch.PendingWrites = append(ce.batch.PendingWrites, record)
	if ce.batch.PendingWritesMap == nil {
		ce.batch.PendingWritesMap = make(map[uint64][]int)
	}
	hashKey := utils.MemHash(key)
	ce.batch.PendingWritesMap[hashKey] = append(ce.batch.PendingWritesMap[hashKey], len(ce.batch.PendingWrites)-1)
}
