package transaction

import (
	"encoding/binary"
	"github.com/valyala/bytebufferpool"
	"jdb/jdb/storage/wal"
)

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
	LogRecordBatchFinished
)

// type batchId keySize valueSize expire
//
//	1  +  10  +   5   +   5   +    10  = 31
const MaxLogRecordHeaderSize = binary.MaxVarintLen32*2 + binary.MaxVarintLen64*2 + 1

// LogRecord 用来存储在batch的pendingWrites中
type LogRecord struct {
	Key     []byte
	Value   []byte
	Type    LogRecordType
	BatchId uint64
	Expire  int64
}

// IsExpired 检查记录是否已过期
func (lr *LogRecord) IsExpired(now int64) bool {
	return lr.Expire > 0 && lr.Expire <= now
}

// IndexRecord key的index记录
// 只使用来重建index
type IndexRecord struct {
	Key        []byte
	RecordType LogRecordType
	Position   *wal.ChunkPosition
}

// +-------------+-------------+-------------+--------------+---------------+---------+--------------+
// |    type     |  batch id   |   key size  |   value size |     expire    |  key    |      value   |
// +-------------+-------------+-------------+--------------+---------------+--------+--------------+
//
//	1 byte	      varint(max 10) varint(max 5)  varint(max 5) varint(max 10)  varint      varint
//
// index的值表示了当前已经在header切片中编码了多少个字节。每当编码整数时编码后的字节会追加到header切片的末尾
// 最后写入buf并返回buf.Bytes()
func EncodeLogRecord(logRecord *LogRecord, header []byte, buf *bytebufferpool.ByteBuffer) []byte {
	header[0] = logRecord.Type
	var index = 1
	// batch id
	index += binary.PutUvarint(header[index:], logRecord.BatchId)
	// key size
	index += binary.PutVarint(header[index:], int64(len(logRecord.Key)))
	// value size
	index += binary.PutVarint(header[index:], int64(len(logRecord.Value)))
	// expire
	index += binary.PutVarint(header[index:], logRecord.Expire)
	// copy header
	_, _ = buf.Write(header[:index])
	// copy key
	_, _ = buf.Write(logRecord.Key)
	// copy value
	_, _ = buf.Write(logRecord.Value)
	return buf.Bytes()
}

// decodeLogRecord 从给定的buf解码LogRecord
func DecodeLogRecord(buf []byte) *LogRecord {
	recordType := buf[0]
	var index uint32 = 1
	// batch id
	batchId, n := binary.Uvarint(buf[index:])
	index += uint32(n)
	// key size
	keySize, n := binary.Varint(buf[index:])
	index += uint32(n)
	// value size
	valueSize, n := binary.Varint(buf[index:])
	index += uint32(n)
	// expire
	expire, n := binary.Varint(buf[index:])
	index += uint32(n)
	// copy key
	key := make([]byte, keySize)
	copy(key[:], buf[index:index+uint32(keySize)])
	index += uint32(keySize)
	// copy value
	value := make([]byte, valueSize)
	copy(value[:], buf[index:index+uint32(valueSize)])
	return &LogRecord{Key: key, Value: value, Expire: expire,
		BatchId: batchId, Type: recordType}
}

func EncodeHintRecord(key []byte, pos *wal.ChunkPosition) []byte {
	// SegmentId BlockNumber ChunkOffset ChunkSize
	//    5          5           10          5      =    25
	// see binary.MaxVarintLen64 and binary.MaxVarintLen32
	buf := make([]byte, 25)
	var idx = 0

	// SegmentId
	idx += binary.PutUvarint(buf[idx:], uint64(pos.SegmentId))
	// BlockNumber
	idx += binary.PutUvarint(buf[idx:], uint64(pos.BlockNumber))
	// ChunkOffset
	idx += binary.PutUvarint(buf[idx:], uint64(pos.ChunkOffset))
	// ChunkSize
	idx += binary.PutUvarint(buf[idx:], uint64(pos.ChunkSize))

	// key
	result := make([]byte, idx+len(key))
	copy(result, buf[:idx])
	copy(result[idx:], key)
	return result
}

func DecodeHintRecord(buf []byte) ([]byte, *wal.ChunkPosition) {
	var idx = 0
	// SegmentId
	segmentId, n := binary.Uvarint(buf[idx:])
	idx += n
	// BlockNumber
	blockNumber, n := binary.Uvarint(buf[idx:])
	idx += n
	// ChunkOffset
	chunkOffset, n := binary.Uvarint(buf[idx:])
	idx += n
	// ChunkSize
	chunkSize, n := binary.Uvarint(buf[idx:])
	idx += n
	// Key
	key := buf[idx:]

	return key, &wal.ChunkPosition{
		SegmentId:   wal.SegmentID(segmentId),
		BlockNumber: uint32(blockNumber),
		ChunkOffset: int64(chunkOffset),
		ChunkSize:   uint32(chunkSize),
	}
}

func EncodeMergeFinRecord(segmentId wal.SegmentID) []byte {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, segmentId)
	return buf
}
