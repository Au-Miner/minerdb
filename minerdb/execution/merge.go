package execution

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"minerdb/minerdb/common/constrants"
	"minerdb/minerdb/common/exception"
	"minerdb/minerdb/storage/index"
	"minerdb/minerdb/storage/transaction"
	wal2 "minerdb/minerdb/storage/wal"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/valyala/bytebufferpool"
)

const (
	mergeDirSuffixName   = "-merge"
	mergeFinishedBatchID = 0
)

// Merge merges所有的data files到merge date file中
//
// Merge操作很耗时，建议当db空闲时执行
// 当reopenAfterDone为真时，原始文件将被合并文件替换，并在合并完成后重建db的索引
// 当reopenAfterDone为假时，合并完成后不会重建索引
func (db *MinerDB) Merge(reopenAfterDone bool) error {
	// 核心：doMerge()
	if err := db.doMerge(); err != nil {
		return err
	}
	if !reopenAfterDone {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	_ = db.closeFiles()
	// 替代原始文件(将-merge替换为原始文件)
	err := loadMergeFiles(db.options.DirPath)
	if err != nil {
		return err
	}
	// 获取data files
	if db.dataFiles, err = db.openWalFiles(); err != nil {
		return err
	}
	db.index = index.NewIndexer()
	// 重建index
	if err = db.loadIndex(); err != nil {
		return err
	}
	return nil
}

// merge阶段会将所有older data file合并为merged data file，并创建全部数据的索引的hint file(唯一)
func (db *MinerDB) doMerge() error {
	db.mu.Lock()
	// 检查是否database已关、data files为空、merge操作正在进行
	if db.isClose {
		db.mu.Unlock()
		return exception.ErrDBClose
	}
	if db.dataFiles.IsEmpty() {
		db.mu.Unlock()
		return nil
	}
	if atomic.LoadUint32(&db.mergeRunning) == 1 {
		db.mu.Unlock()
		return exception.ErrMergeRunning
	}
	// 原子性，设置mergeRunning为1
	atomic.StoreUint32(&db.mergeRunning, 1)
	defer atomic.StoreUint32(&db.mergeRunning, 0)

	prevActiveSegId := db.dataFiles.ActiveSegmentID()
	// 将active segment存入older segments中，并创建一个新的active segment来作为merge segment
	if err := db.dataFiles.OpenNewActiveSegment(); err != nil {
		db.mu.Unlock()
		return err
	}
	// 这里就可以关锁了，因为后续写操作都在新的active segment中进行，merge操作只会读取older segment files
	db.mu.Unlock()
	// 创建一个新的merge db来存储merge后的数据
	mergeDB, err := db.openMergeDB()
	if err != nil {
		return err
	}
	defer func() {
		_ = mergeDB.Close()
	}()

	buf := bytebufferpool.Get()
	now := time.Now().UnixNano()
	defer bytebufferpool.Put(buf)

	// 从prevActiveSegId往前遍历
	reader := db.dataFiles.NewReaderWithMax(prevActiveSegId)
	for {
		buf.Reset()
		chunk, position, err := reader.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		record := transaction.DecodeLogRecord(chunk)
		// 只处理LogRecordNormal, 忽略LogRecordDeleted和LogRecordBatchFinished
		if record.Type == transaction.LogRecordNormal && (record.Expire == 0 || record.Expire > now) {
			db.mu.RLock()
			indexPos := db.index.Get(record.Key)
			db.mu.RUnlock()
			if indexPos != nil && positionEquals(indexPos, position) {
				// 清空record的batchId为0
				record.BatchId = mergeFinishedBatchID
				// mergeDB只用来存储merge后的数据，不会用来读写操作，所以不需要更新index
				newPosition, err := mergeDB.dataFiles.Write(transaction.EncodeLogRecord(record, mergeDB.encodeHeader, buf))
				if err != nil {
					return err
				}
				_, err = mergeDB.hintFile.Write(transaction.EncodeHintRecord(record.Key, newPosition))
				if err != nil {
					return err
				}
			}
		}
	}
	// 在重写所有数据之后，我们应该添加一个文件来指示合并操作已完成
	// 当重启db，如果文件存在，我们可以知道合并已完成，否则，我们将删除合并目录并重新执行合并操作
	mergeFinFile, err := mergeDB.openMergeFinishedFile()
	if err != nil {
		return err
	}
	_, err = mergeFinFile.Write(transaction.EncodeMergeFinRecord(prevActiveSegId))
	if err != nil {
		return err
	}
	if err := mergeFinFile.Close(); err != nil {
		return err
	}
	return nil
}

// 创建一个新的merge db
func (db *MinerDB) openMergeDB() (*MinerDB, error) {
	// dirPath加上-merge后缀
	mergePath := mergeDirPath(db.options.DirPath)
	if err := os.RemoveAll(mergePath); err != nil {
		return nil, err
	}
	options := db.options
	// 不使用原本同步策略，在merge结束后手动同步数据文件
	options.Sync, options.BytesPerSync = false, 0
	options.DirPath = mergePath
	mergeDB, err := Open(options)
	if err != nil {
		return nil, err
	}
	// 创建一个hint file
	hintFile, err := wal2.Open(wal2.Options{
		DirPath:        options.DirPath,
		SegmentSize:    math.MaxInt64,
		SegmentFileExt: hintFileNameSuffix,
		Sync:           false,
		BytesPerSync:   0,
		BlockCache:     0,
	})
	if err != nil {
		return nil, err
	}
	mergeDB.hintFile = hintFile
	return mergeDB, nil
}

func mergeDirPath(dirPath string) string {
	dir := filepath.Dir(filepath.Clean(dirPath))
	base := filepath.Base(dirPath)
	return filepath.Join(dir, base+mergeDirSuffixName)
}

func (db *MinerDB) openMergeFinishedFile() (*wal2.WAL, error) {
	return wal2.Open(wal2.Options{
		DirPath:        db.options.DirPath,
		SegmentSize:    constrants.GB,
		SegmentFileExt: mergeFinNameSuffix,
		Sync:           false,
		BytesPerSync:   0,
		BlockCache:     0,
	})
}

func positionEquals(a, b *wal2.ChunkPosition) bool {
	return a.SegmentId == b.SegmentId &&
		a.BlockNumber == b.BlockNumber &&
		a.ChunkOffset == b.ChunkOffset
}

// loadMergeFiles 读取所有merge文件，并将文件放到目录中
func loadMergeFiles(dirPath string) error {
	// 检查是否有dirPath-merge文件
	mergeDirPath := mergeDirPath(dirPath)
	if _, err := os.Stat(mergeDirPath); err != nil {
		// 不存在直接返回
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	// 最后删掉dirPath-merge文件
	defer func() {
		_ = os.RemoveAll(mergeDirPath)
	}()

	// 创建一个函数，用于将merge中的fileId(segmentId)文件移动到dirPath中
	copyFile := func(suffix string, fileId uint32, force bool) {
		// 根据suffix和fileId(segmentId)获取文件名
		srcFile := wal2.SegmentFileName(mergeDirPath, suffix, fileId)
		stat, err := os.Stat(srcFile)
		if os.IsNotExist(err) {
			return
		}
		if err != nil {
			panic(fmt.Sprintf("loadMergeFiles: failed to get src file stat %v", err))
		}
		if !force && stat.Size() == 0 {
			return
		}
		destFile := wal2.SegmentFileName(dirPath, suffix, fileId)
		_ = os.Rename(srcFile, destFile)
	}

	// 获取merge文件的最后segmentId(之前已经merge后的merge data files)
	mergeFinSegmentId, err := getMergeFinSegmentId(mergeDirPath)
	if err != nil {
		return err
	}
	// 将所有小于等于mergeFinSegmentId的segment文件移动到原始数据目录，并删除原始数据目录中的相应文件
	for fileId := uint32(1); fileId <= mergeFinSegmentId; fileId++ {
		destFile := wal2.SegmentFileName(dirPath, dataFileNameSuffix, fileId)
		if _, err = os.Stat(destFile); err == nil {
			if err = os.Remove(destFile); err != nil {
				return err
			}
		}
		copyFile(dataFileNameSuffix, fileId, false)
	}

	// 将mergeFinNameSuffix和hintFileNameSuffix文件复制到原始数据目录
	// 只有一个合并完成的文件，所以文件id总是1
	copyFile(mergeFinNameSuffix, 1, true)
	copyFile(hintFileNameSuffix, 1, true)

	return nil
}

// 获取merge文件的最后segmentId
func getMergeFinSegmentId(mergePath string) (wal2.SegmentID, error) {
	// 查找mergeFinNameSuffix标志的文件
	mergeFinFile, err := os.Open(wal2.SegmentFileName(mergePath, mergeFinNameSuffix, 1))
	if err != nil {
		// if the merge finished file does not exist, it means that the merge operation is not completed.
		// so we should remove the merge directory and return nil.
		return 0, nil
	}
	defer func() {
		_ = mergeFinFile.Close()
	}()

	// 单个record结构为 CRC (4B) | Length (2B) | Type (1B) |  Payload，所以从第7个字节开始读取
	mergeFinBuf := make([]byte, 4)
	if _, err := mergeFinFile.ReadAt(mergeFinBuf, 7); err != nil {
		return 0, err
	}
	mergeFinSegmentId := binary.LittleEndian.Uint32(mergeFinBuf)
	return mergeFinSegmentId, nil
}

func (db *MinerDB) loadIndexFromHintFile() error {
	hintFile, err := wal2.Open(wal2.Options{
		DirPath:        db.options.DirPath,
		SegmentSize:    math.MaxInt64,
		SegmentFileExt: hintFileNameSuffix,
		BlockCache:     32 * constrants.KB * 10,
	})
	if err != nil {
		return err
	}
	defer func() {
		_ = hintFile.Close()
	}()

	// 读取所有hint文件中的数据，将数据放入index中
	reader := hintFile.NewReader()
	for {
		chunk, _, err := reader.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		key, position := transaction.DecodeHintRecord(chunk)
		db.index.Put(key, position)
	}
	return nil
}
