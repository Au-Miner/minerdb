package execution

import (
	"fmt"
	"io"
	"minerdb/minerdb/common/constrants"
	"minerdb/minerdb/common/exception"
	"minerdb/minerdb/common/utils"
	"minerdb/minerdb/concurrency"
	"minerdb/minerdb/storage/index"
	transaction2 "minerdb/minerdb/storage/transaction"
	wal2 "minerdb/minerdb/storage/wal"
	"minerdb/minerdb/watch"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"

	"github.com/bwmarrin/snowflake"
	"github.com/gofrs/flock"
	"github.com/robfig/cron/v3"
)

const (
	fileLockName       = "FLOCK"
	dataFileNameSuffix = ".SEG"
	hintFileNameSuffix = ".HINT"
	mergeFinNameSuffix = ".MERGEFIN"
)

type MinerDB struct {
	dataFiles     *wal2.WAL     // 所有bitcask日志文件
	hintFile      *wal2.WAL     // merge期间所有older data file统计的hint，用于快速恢复index
	index         index.Indexer // key->wal.ChunkPosition，pos是wal写入后返回的信息
	options       constrants.Options
	fileLock      *flock.Flock
	mu            sync.RWMutex
	isClose       bool
	mergeRunning  uint32
	executorPool  sync.Pool
	recordPool    sync.Pool
	encodeHeader  []byte
	watchCh       chan *watch.Event
	watcher       *watch.Watcher
	cronScheduler *cron.Cron               // 自动merge
	LockManager   *concurrency.LockManager // 锁管理
}

// Stat 数据库统计信息
type Stat struct {
	KeysNum  int
	DiskSize int64
}

// NewExecutor 通过db.NewExecutor来新建executor
func (db *MinerDB) NewExecutor(options constrants.BatchOptions) *CommonExecutor {
	commonExecutor := &CommonExecutor{
		db:    db,
		batch: *transaction2.NewBatch(options),
	}
	commonExecutor.lock()
	return commonExecutor
}

func NewExecutor() interface{} {
	return &CommonExecutor{
		batch: *transaction2.NewBatch(constrants.DefaultBatchOptions),
	}
}

func NewRecord() interface{} {
	return &transaction2.LogRecord{}
}

// 根据options来创建db
func Open(options constrants.Options) (*MinerDB, error) {
	// 创建文件夹
	if _, err := os.Stat(options.DirPath); err != nil {
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}
	// 创建文件锁，阻止多进程访问相同同一数据库文件
	fileLock := flock.New(filepath.Join(options.DirPath, fileLockName))
	hold, err := fileLock.TryLock()
	if err != nil {
		return nil, err
	}
	if !hold {
		return nil, exception.ErrDatabaseIsUsing
	}
	// 读取merge后的文件(所有dirPath-merge文件（doMerge之后的mergeDB中的数据）会被移动到dirPath中)
	if err = loadMergeFiles(options.DirPath); err != nil {
		return nil, err
	}
	// 初始化DB实例
	db := &MinerDB{
		index:        index.NewIndexer(),
		options:      options,
		fileLock:     fileLock,
		executorPool: sync.Pool{New: NewExecutor},
		recordPool:   sync.Pool{New: NewRecord},
		encodeHeader: make([]byte, transaction2.MaxLogRecordHeaderSize),
		LockManager:  concurrency.NewLockManager(),
	}
	// 读取wal文件
	if db.dataFiles, err = db.openWalFiles(); err != nil {
		return nil, err
	}
	// 通过wal和hint来加载索引
	if err = db.loadIndex(); err != nil {
		return nil, err
	}
	// 开启watch
	if options.WatchQueueSize > 0 {
		db.watchCh = make(chan *watch.Event, 100)
		db.watcher = watch.NewWatcher(options.WatchQueueSize)
		// 开启goroutine以同步事件信息
		go db.watcher.SendEvent(db.watchCh)
	}
	// 开启自动merge task
	if len(options.AutoMergeCronExpr) > 0 {
		db.cronScheduler = cron.New(
			cron.WithParser(
				cron.NewParser(cron.SecondOptional | cron.Minute | cron.Hour |
					cron.Dom | cron.Month | cron.Dow | cron.Descriptor),
			),
		)
		_, err = db.cronScheduler.AddFunc(options.AutoMergeCronExpr, func() {
			// 自动执行merge
			_ = db.Merge(true)
		})
		if err != nil {
			return nil, err
		}
		db.cronScheduler.Start()
	}
	return db, nil
}

func (db *MinerDB) openWalFiles() (*wal2.WAL, error) {
	// 从WAL中读取数据
	walFiles, err := wal2.Open(wal2.Options{
		DirPath:        db.options.DirPath,
		SegmentSize:    db.options.SegmentSize,
		SegmentFileExt: dataFileNameSuffix,
		BlockCache:     db.options.BlockCache,
		Sync:           db.options.Sync,
		BytesPerSync:   db.options.BytesPerSync,
	})
	if err != nil {
		return nil, err
	}
	return walFiles, nil
}

func (db *MinerDB) loadIndex() error {
	// 从hint file创建index
	if err := db.loadIndexFromHintFile(); err != nil {
		return err
	}
	// 从dataFiles中创建index
	if err := db.loadIndexFromWAL(); err != nil {
		return err
	}
	return nil
}

// Close 关闭db，db后续不能用
func (db *MinerDB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := db.closeFiles(); err != nil {
		return err
	}
	// 释放文件锁，允许其他进程访问该数据库
	if err := db.fileLock.Unlock(); err != nil {
		return err
	}
	// 关闭watch channel，并通知watcher
	if db.options.WatchQueueSize > 0 {
		close(db.watchCh)
		time.Sleep(100 * time.Millisecond)
	}
	// 关闭自动merge任务
	if db.cronScheduler != nil {
		db.cronScheduler.Stop()
	}
	db.isClose = true
	return nil
}

// closeFiles close所有data files和hint file
func (db *MinerDB) closeFiles() error {
	if err := db.dataFiles.Close(); err != nil {
		return err
	}
	if db.hintFile != nil {
		if err := db.hintFile.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Sync sync activeSegment到磁盘
func (db *MinerDB) Sync() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	return db.dataFiles.Sync()
}

// Stat 返回database统计信息
func (db *MinerDB) Stat() *Stat {
	db.mu.Lock()
	defer db.mu.Unlock()

	diskSize, err := utils.DirSize(db.options.DirPath)
	if err != nil {
		panic(fmt.Sprintf("rosedb: get database directory size error: %v", err))
	}

	return &Stat{
		KeysNum:  db.index.Size(),
		DiskSize: diskSize,
	}
}

// Put 调用db.Put操作，只会将一个Put操作放入commonExecutorh中，并commit掉
func (db *MinerDB) Put(key []byte, value []byte) error {
	commonExecutor := db.executorPool.Get().(*CommonExecutor)
	defer func() {
		// 最后要reset
		commonExecutor.reset()
		db.executorPool.Put(commonExecutor)
	}()
	// 设置sync为false，因为数据会写入WAL，WAL文件会根据DB选项同步到磁盘
	commonExecutor.init(false, false, db)
	if err := commonExecutor.Put(key, value); err != nil {
		_ = commonExecutor.Rollback()
		return err
	}
	return commonExecutor.Commit()
}

func (db *MinerDB) ConcurrentPut(key []byte, value []byte) error {
	commonExecutor := db.executorPool.Get().(*CommonExecutor)
	defer func() {
		// 最后要reset
		commonExecutor.reset()
		db.executorPool.Put(commonExecutor)
	}()
	// 设置sync为false，因为数据会写入WAL，WAL文件会根据DB选项同步到磁盘
	commonExecutor.init(false, false, db)
	if err := commonExecutor.ConcurrentPut(key, value, db.LockManager); err != nil {
		_ = commonExecutor.ConcurrentRollback(key, db.LockManager)
		return err
	}
	return commonExecutor.ConcurrentCommit(key, db.LockManager)
}

// PutWithTTL kv对携带ttl
func (db *MinerDB) PutWithTTL(key []byte, value []byte, ttl time.Duration) error {
	commonExecutor := db.executorPool.Get().(*CommonExecutor)
	defer func() {
		commonExecutor.reset()
		db.executorPool.Put(commonExecutor)
	}()
	commonExecutor.init(false, false, db)
	if err := commonExecutor.PutWithTTL(key, value, ttl); err != nil {
		commonExecutor.Rollback()
		return err
	}
	return commonExecutor.Commit()
}

// Get get value
func (db *MinerDB) Get(key []byte) ([]byte, error) {
	commonExecutor := db.executorPool.Get().(*CommonExecutor)
	commonExecutor.init(true, false, db)
	defer func() {
		_ = commonExecutor.Commit()
		commonExecutor.reset()
		db.executorPool.Put(commonExecutor)
	}()
	return commonExecutor.Get(key)
}

func (db *MinerDB) ConcurrentGet(key []byte) ([]byte, error) {
	commonExecutor := db.executorPool.Get().(*CommonExecutor)
	commonExecutor.init(true, false, db)
	defer func() {
		commonExecutor.reset()
		db.executorPool.Put(commonExecutor)
	}()
	if val, err := commonExecutor.ConcurrentGet(key, db.LockManager); err != nil {
		commonExecutor.ConcurrentRollback(key, db.LockManager)
		return nil, err
	} else {
		_ = commonExecutor.ConcurrentCommit(key, db.LockManager)
		return val, nil
	}
}

// Delete 删除特定的key
func (db *MinerDB) Delete(key []byte) error {
	commonExecutor := db.executorPool.Get().(*CommonExecutor)
	defer func() {
		commonExecutor.reset()
		db.executorPool.Put(commonExecutor)
	}()
	// 同put，设置sync为false
	commonExecutor.init(false, false, db)
	if err := commonExecutor.Delete(key); err != nil {
		_ = commonExecutor.Rollback()
		return err
	}
	return commonExecutor.Commit()
}

func (db *MinerDB) ConcurrentDelete(key []byte) error {
	commonExecutor := db.executorPool.Get().(*CommonExecutor)
	defer func() {
		commonExecutor.reset()
		db.executorPool.Put(commonExecutor)
	}()
	// 同put，设置sync为false
	commonExecutor.init(false, false, db)
	if err := commonExecutor.ConcurrentDelete(key, db.LockManager); err != nil {
		_ = commonExecutor.ConcurrentRollback(key, db.LockManager)
		return err
	}
	return commonExecutor.ConcurrentCommit(key, db.LockManager)
}

// Exist 检查是否存在key
func (db *MinerDB) Exist(key []byte) (bool, error) {
	commonExecutor := db.executorPool.Get().(*CommonExecutor)
	commonExecutor.init(true, false, db)
	defer func() {
		_ = commonExecutor.Commit()
		commonExecutor.reset()
		db.executorPool.Put(commonExecutor)
	}()
	return commonExecutor.Exist(key)
}

// Expire 手动设置key对应的ttl，和put、PutWithTTL类似
func (db *MinerDB) Expire(key []byte, ttl time.Duration) error {
	commonExecutor := db.executorPool.Get().(*CommonExecutor)
	defer func() {
		commonExecutor.reset()
		db.executorPool.Put(commonExecutor)
	}()
	commonExecutor.init(false, false, db)
	if err := commonExecutor.Expire(key, ttl); err != nil {
		_ = commonExecutor.Rollback()
		return err
	}
	return commonExecutor.Commit()
}

// TTL 获取key的ttl
func (db *MinerDB) TTL(key []byte) (time.Duration, error) {
	commonExecutor := db.executorPool.Get().(*CommonExecutor)
	commonExecutor.init(true, false, db)
	defer func() {
		_ = commonExecutor.Commit()
		commonExecutor.reset()
		db.executorPool.Put(commonExecutor)
	}()
	return commonExecutor.TTL(key)
}

func (db *MinerDB) Watch() (<-chan *watch.Event, error) {
	if db.options.WatchQueueSize <= 0 {
		return nil, exception.ErrWatchDisabled
	}
	return db.watchCh, nil
}

// Ascend 按照key升序枚举，每个kv对调用handleFn
func (db *MinerDB) Ascend(handleFn func(k []byte, v []byte) (bool, error)) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	db.index.Ascend(func(key []byte, pos *wal2.ChunkPosition) (bool, error) {
		chunk, err := db.dataFiles.Read(pos)
		if err != nil {
			return false, err
		}
		if value := db.checkValue(chunk); value != nil {
			return handleFn(key, value)
		}
		return true, nil
	})
}

// AscendRange 处在[startKey, endKey]范围内的ascend
func (db *MinerDB) AscendRange(startKey, endKey []byte, handleFn func(k []byte, v []byte) (bool, error)) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	db.index.AscendRange(startKey, endKey, func(key []byte, pos *wal2.ChunkPosition) (bool, error) {
		chunk, err := db.dataFiles.Read(pos)
		if err != nil {
			return false, nil
		}
		if value := db.checkValue(chunk); value != nil {
			return handleFn(key, value)
		}
		return true, nil
	})
}

// AscendGreaterOrEqual >=key的执行handleFn
func (db *MinerDB) AscendGreaterOrEqual(key []byte, handleFn func(k []byte, v []byte) (bool, error)) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	db.index.AscendGreaterOrEqual(key, func(key []byte, pos *wal2.ChunkPosition) (bool, error) {
		chunk, err := db.dataFiles.Read(pos)
		if err != nil {
			return false, nil
		}
		if value := db.checkValue(chunk); value != nil {
			return handleFn(key, value)
		}
		return true, nil
	})
}

// AscendKeys 根据key升序排列，调用handleFn，通过pattern筛选
// 设置filterExpired为false时，不需要查找value；为true时，会过滤过期的key但会影响性能
func (db *MinerDB) AscendKeys(pattern []byte, filterExpired bool, handleFn func(k []byte) (bool, error)) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	var reg *regexp.Regexp
	if len(pattern) > 0 {
		reg = regexp.MustCompile(string(pattern))
	}
	db.index.Ascend(func(key []byte, pos *wal2.ChunkPosition) (bool, error) {
		if reg == nil || reg.Match(key) {
			var invalid bool
			if filterExpired {
				chunk, err := db.dataFiles.Read(pos)
				if err != nil {
					return false, err
				}
				if value := db.checkValue(chunk); value == nil {
					invalid = true
				}
			}
			if invalid {
				return true, nil
			}
			return handleFn(key)
		}
		return true, nil
	})
}

// Descend 按照key降序枚举，每个kv对调用handleFn
func (db *MinerDB) Descend(handleFn func(k []byte, v []byte) (bool, error)) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	db.index.Descend(func(key []byte, pos *wal2.ChunkPosition) (bool, error) {
		chunk, err := db.dataFiles.Read(pos)
		if err != nil {
			return false, nil
		}
		if value := db.checkValue(chunk); value != nil {
			return handleFn(key, value)
		}
		return true, nil
	})
}

// DescendRange 处在[startKey, endKey]范围内的descend
func (db *MinerDB) DescendRange(startKey, endKey []byte, handleFn func(k []byte, v []byte) (bool, error)) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	db.index.DescendRange(startKey, endKey, func(key []byte, pos *wal2.ChunkPosition) (bool, error) {
		chunk, err := db.dataFiles.Read(pos)
		if err != nil {
			return false, nil
		}
		if value := db.checkValue(chunk); value != nil {
			return handleFn(key, value)
		}
		return true, nil
	})
}

// DescendLessOrEqual <=key的执行handleFn
func (db *MinerDB) DescendLessOrEqual(key []byte, handleFn func(k []byte, v []byte) (bool, error)) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	db.index.DescendLessOrEqual(key, func(key []byte, pos *wal2.ChunkPosition) (bool, error) {
		chunk, err := db.dataFiles.Read(pos)
		if err != nil {
			return false, nil
		}
		if value := db.checkValue(chunk); value != nil {
			return handleFn(key, value)
		}
		return true, nil
	})
}

// DescendKeys 根据key降序排列，调用handleFn，通过pattern筛选
// 设置filterExpired为false时，不需要查找value；为true时，会过滤过期的key但会影响性能
func (db *MinerDB) DescendKeys(pattern []byte, filterExpired bool, handleFn func(k []byte) (bool, error)) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	var reg *regexp.Regexp
	if len(pattern) > 0 {
		reg = regexp.MustCompile(string(pattern))
	}
	db.index.Descend(func(key []byte, pos *wal2.ChunkPosition) (bool, error) {
		if reg == nil || reg.Match(key) {
			var invalid bool
			if filterExpired {
				chunk, err := db.dataFiles.Read(pos)
				if err != nil {
					return false, err
				}
				if value := db.checkValue(chunk); value == nil {
					invalid = true
				}
			}
			if invalid {
				return true, nil
			}
			return handleFn(key)
		}
		return true, nil
	})
}

func (db *MinerDB) checkValue(chunk []byte) []byte {
	record := transaction2.DecodeLogRecord(chunk)
	now := time.Now().UnixNano()
	if record.Type != transaction2.LogRecordDeleted && !record.IsExpired(now) {
		return record.Value
	}
	return nil
}

// loadIndexFromWAL 从wal中来加载索引
// 它会加载所有WAL files来重建index
func (db *MinerDB) loadIndexFromWAL() error {
	mergeFinSegmentId, err := getMergeFinSegmentId(db.options.DirPath)
	if err != nil {
		return err
	}
	indexRecords := make(map[uint64][]*transaction2.IndexRecord)
	now := time.Now().UnixNano()
	// 创建reader
	reader := db.dataFiles.NewReader()
	for {
		// 只处理>mergeFinSegmentId的segment，因为old segment的index已经在hint file中了
		if reader.CurrentSegmentId() <= mergeFinSegmentId {
			reader.SkipCurrentSegment()
			continue
		}
		chunk, position, err := reader.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		record := transaction2.DecodeLogRecord(chunk)

		// 对于activeSegments中只有遇到LogRecordBatchFinished才会开始更新index
		if record.Type == transaction2.LogRecordBatchFinished {
			batchId, err := snowflake.ParseBytes(record.Key)
			if err != nil {
				return err
			}
			for _, idxRecord := range indexRecords[uint64(batchId)] {
				if idxRecord.RecordType == transaction2.LogRecordNormal {
					db.index.Put(idxRecord.Key, idxRecord.Position)
				}
				if idxRecord.RecordType == transaction2.LogRecordDeleted {
					db.index.Delete(idxRecord.Key)
				}
			}
			delete(indexRecords, uint64(batchId))
		} else if record.Type == transaction2.LogRecordNormal && record.BatchId == mergeFinishedBatchID {
			// 如果是LogRecordNormal且batchId为mergeFinishedBatchID，说明是merge后的数据，直接放入即可
			// 因为只处理>mergeFinSegmentId的segment，一般不会走该if
			fmt.Println("真的走了！！！！！")
			db.index.Put(record.Key, position)
		} else {
			if record.IsExpired(now) {
				db.index.Delete(record.Key)
				continue
			}
			// 先放到indexRecords中
			indexRecords[record.BatchId] = append(indexRecords[record.BatchId],
				&transaction2.IndexRecord{
					Key:        record.Key,
					RecordType: record.Type,
					Position:   position,
				})
		}
	}
	return nil
}
