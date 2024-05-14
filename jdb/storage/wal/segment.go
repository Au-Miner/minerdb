package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/valyala/bytebufferpool"
)

type ChunkType = byte
type SegmentID = uint32

const (
	ChunkTypeFull ChunkType = iota
	ChunkTypeFirst
	ChunkTypeMiddle
	ChunkTypeLast
)

var (
	ErrClosed     = errors.New("the segment file is closed")
	ErrInvalidCRC = errors.New("invalid crc, the data may be corrupted")
)

const (
	chunkHeaderSize = 7
	blockSize       = 32 * KB
	fileModePerm    = 0644
	maxLen          = binary.MaxVarintLen32*3 + binary.MaxVarintLen64
)

type segment struct {
	id                 SegmentID
	fd                 *os.File
	currentBlockNumber uint32
	currentBlockSize   uint32
	closed             bool
	cache              *lru.Cache[uint64, []byte]
	header             []byte
	blockPool          sync.Pool
}

type segmentReader struct {
	segment     *segment
	blockNumber uint32
	chunkOffset int64
}

type blockAndHeader struct {
	block  []byte
	header []byte
}

type ChunkPosition struct {
	SegmentId   SegmentID
	BlockNumber uint32
	ChunkOffset int64
	ChunkSize   uint32
}

func openSegmentFile(dirPath, extName string, id uint32, cache *lru.Cache[uint64, []byte]) (*segment, error) {
	fd, err := os.OpenFile(
		SegmentFileName(dirPath, extName, id),
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		fileModePerm,
	)
	if err != nil {
		return nil, err
	}
	offset, err := fd.Seek(0, io.SeekEnd)
	if err != nil {
		panic(fmt.Errorf("seek to the end of segment file %d%s failed: %v", id, extName, err))
	}
	return &segment{
		id:                 id,
		fd:                 fd,
		cache:              cache,
		header:             make([]byte, chunkHeaderSize),
		blockPool:          sync.Pool{New: newBlockAndHeader},
		currentBlockNumber: uint32(offset / blockSize),
		currentBlockSize:   uint32(offset % blockSize),
	}, nil
}

func newBlockAndHeader() interface{} {
	return &blockAndHeader{
		block:  make([]byte, blockSize),
		header: make([]byte, chunkHeaderSize),
	}
}

func (seg *segment) NewReader() *segmentReader {
	return &segmentReader{
		segment:     seg,
		blockNumber: 0,
		chunkOffset: 0,
	}
}

func (seg *segment) Sync() error {
	if seg.closed {
		return nil
	}
	return seg.fd.Sync()
}

func (seg *segment) Remove() error {
	if !seg.closed {
		seg.closed = true
		_ = seg.fd.Close()
	}
	return os.Remove(seg.fd.Name())
}

func (seg *segment) Close() error {
	if seg.closed {
		return nil
	}

	seg.closed = true
	return seg.fd.Close()
}

func (seg *segment) Size() int64 {
	size := int64(seg.currentBlockNumber) * int64(blockSize)
	return size + int64(seg.currentBlockSize)
}

func (seg *segment) writeToBuffer(data []byte, chunkBuffer *bytebufferpool.ByteBuffer) (*ChunkPosition, error) {
	startBufferLen := chunkBuffer.Len()
	padding := uint32(0)
	if seg.closed {
		return nil, ErrClosed
	}
	if seg.currentBlockSize+chunkHeaderSize >= blockSize {
		if seg.currentBlockSize < blockSize {
			p := make([]byte, blockSize-seg.currentBlockSize)
			chunkBuffer.B = append(chunkBuffer.B, p...)
			padding += blockSize - seg.currentBlockSize
			seg.currentBlockNumber += 1
			seg.currentBlockSize = 0
		}
	}
	position := &ChunkPosition{
		SegmentId:   seg.id,
		BlockNumber: seg.currentBlockNumber,
		ChunkOffset: int64(seg.currentBlockSize),
	}
	dataSize := uint32(len(data))
	if seg.currentBlockSize+dataSize+chunkHeaderSize <= blockSize {
		seg.appendChunkBuffer(chunkBuffer, data, ChunkTypeFull)
		position.ChunkSize = dataSize + chunkHeaderSize
	} else {
		var (
			leftSize             = dataSize
			blockCount    uint32 = 0
			currBlockSize        = seg.currentBlockSize
		)
		for leftSize > 0 {
			chunkSize := blockSize - currBlockSize - chunkHeaderSize
			if chunkSize > leftSize {
				chunkSize = leftSize
			}
			var end = dataSize - leftSize + chunkSize
			if end > dataSize {
				end = dataSize
			}
			var chunkType ChunkType
			switch leftSize {
			case dataSize: // First chunk
				chunkType = ChunkTypeFirst
			case chunkSize: // Last chunk
				chunkType = ChunkTypeLast
			default: // Middle chunk
				chunkType = ChunkTypeMiddle
			}
			seg.appendChunkBuffer(chunkBuffer, data[dataSize-leftSize:end], chunkType)
			leftSize -= chunkSize
			blockCount += 1
			currBlockSize = (currBlockSize + chunkSize + chunkHeaderSize) % blockSize
		}
		position.ChunkSize = blockCount*chunkHeaderSize + dataSize
	}
	endBufferLen := chunkBuffer.Len()
	if position.ChunkSize+padding != uint32(endBufferLen-startBufferLen) {
		panic(fmt.Sprintf("wrong!!! the chunk size %d is not equal to the buffer len %d",
			position.ChunkSize+padding, endBufferLen-startBufferLen))
	}
	seg.currentBlockSize += position.ChunkSize
	if seg.currentBlockSize >= blockSize {
		seg.currentBlockNumber += seg.currentBlockSize / blockSize
		seg.currentBlockSize = seg.currentBlockSize % blockSize
	}
	return position, nil
}

func (seg *segment) writeAll(data [][]byte) (positions []*ChunkPosition, err error) {
	if seg.closed {
		return nil, ErrClosed
	}
	originBlockNumber := seg.currentBlockNumber
	originBlockSize := seg.currentBlockSize
	chunkBuffer := bytebufferpool.Get()
	chunkBuffer.Reset()
	defer func() {
		if err != nil {
			seg.currentBlockNumber = originBlockNumber
			seg.currentBlockSize = originBlockSize
		}
		bytebufferpool.Put(chunkBuffer)
	}()
	var pos *ChunkPosition
	positions = make([]*ChunkPosition, len(data))
	for i := 0; i < len(positions); i++ {
		pos, err = seg.writeToBuffer(data[i], chunkBuffer)
		if err != nil {
			return
		}
		positions[i] = pos
	}
	if err = seg.writeChunkBuffer(chunkBuffer); err != nil {
		return
	}
	return
}

func (seg *segment) Write(data []byte) (pos *ChunkPosition, err error) {
	if seg.closed {
		return nil, ErrClosed
	}
	originBlockNumber := seg.currentBlockNumber
	originBlockSize := seg.currentBlockSize
	chunkBuffer := bytebufferpool.Get()
	chunkBuffer.Reset()
	defer func() {
		if err != nil {
			seg.currentBlockNumber = originBlockNumber
			seg.currentBlockSize = originBlockSize
		}
		bytebufferpool.Put(chunkBuffer)
	}()
	pos, err = seg.writeToBuffer(data, chunkBuffer)
	if err != nil {
		return
	}
	if err = seg.writeChunkBuffer(chunkBuffer); err != nil {
		return
	}
	return
}

func (seg *segment) appendChunkBuffer(buf *bytebufferpool.ByteBuffer, data []byte, chunkType ChunkType) {
	binary.LittleEndian.PutUint16(seg.header[4:6], uint16(len(data)))
	seg.header[6] = chunkType
	sum := crc32.ChecksumIEEE(seg.header[4:])
	sum = crc32.Update(sum, crc32.IEEETable, data)
	binary.LittleEndian.PutUint32(seg.header[:4], sum)
	buf.B = append(buf.B, seg.header...)
	buf.B = append(buf.B, data...)
}

func (seg *segment) writeChunkBuffer(buf *bytebufferpool.ByteBuffer) error {
	if seg.currentBlockSize > blockSize {
		panic("wrong! can not exceed the block size")
	}
	if _, err := seg.fd.Write(buf.Bytes()); err != nil {
		return err
	}
	return nil
}

func (seg *segment) Read(blockNumber uint32, chunkOffset int64) ([]byte, error) {
	value, _, err := seg.readInternal(blockNumber, chunkOffset)
	return value, err
}

func (seg *segment) readInternal(blockNumber uint32, chunkOffset int64) ([]byte, *ChunkPosition, error) {
	if seg.closed {
		return nil, nil, ErrClosed
	}
	var (
		result    []byte
		bh        = seg.blockPool.Get().(*blockAndHeader)
		segSize   = seg.Size()
		nextChunk = &ChunkPosition{SegmentId: seg.id}
	)
	defer func() {
		seg.blockPool.Put(bh)
	}()
	for {
		size := int64(blockSize)
		offset := int64(blockNumber) * blockSize
		if size+offset > segSize {
			size = segSize - offset
		}
		if chunkOffset >= size {
			return nil, nil, io.EOF
		}
		var ok bool
		var cachedBlock []byte
		if seg.cache != nil {
			cachedBlock, ok = seg.cache.Get(seg.getCacheKey(blockNumber))
		}
		if ok {
			copy(bh.block, cachedBlock)
		} else {
			_, err := seg.fd.ReadAt(bh.block[0:size], offset)
			if err != nil {
				return nil, nil, err
			}
			if seg.cache != nil && size == blockSize && len(cachedBlock) == 0 {
				cacheBlock := make([]byte, blockSize)
				copy(cacheBlock, bh.block)
				seg.cache.Add(seg.getCacheKey(blockNumber), cacheBlock)
			}
		}
		copy(bh.header, bh.block[chunkOffset:chunkOffset+chunkHeaderSize])
		length := binary.LittleEndian.Uint16(bh.header[4:6])
		start := chunkOffset + chunkHeaderSize
		result = append(result, bh.block[start:start+int64(length)]...)
		checksumEnd := chunkOffset + chunkHeaderSize + int64(length)
		checksum := crc32.ChecksumIEEE(bh.block[chunkOffset+4 : checksumEnd])
		savedSum := binary.LittleEndian.Uint32(bh.header[:4])
		if savedSum != checksum {
			return nil, nil, ErrInvalidCRC
		}
		chunkType := bh.header[6]
		if chunkType == ChunkTypeFull || chunkType == ChunkTypeLast {
			nextChunk.BlockNumber = blockNumber
			nextChunk.ChunkOffset = checksumEnd
			if checksumEnd+chunkHeaderSize >= blockSize {
				nextChunk.BlockNumber += 1
				nextChunk.ChunkOffset = 0
			}
			break
		}
		blockNumber += 1
		chunkOffset = 0
	}
	return result, nextChunk, nil
}

func (seg *segment) getCacheKey(blockNumber uint32) uint64 {
	return uint64(seg.id)<<32 | uint64(blockNumber)
}

func (segReader *segmentReader) Next() ([]byte, *ChunkPosition, error) {
	if segReader.segment.closed {
		return nil, nil, ErrClosed
	}
	chunkPosition := &ChunkPosition{
		SegmentId:   segReader.segment.id,
		BlockNumber: segReader.blockNumber,
		ChunkOffset: segReader.chunkOffset,
	}
	value, nextChunk, err := segReader.segment.readInternal(
		segReader.blockNumber,
		segReader.chunkOffset,
	)
	if err != nil {
		return nil, nil, err
	}
	chunkPosition.ChunkSize =
		nextChunk.BlockNumber*blockSize + uint32(nextChunk.ChunkOffset) -
			(segReader.blockNumber*blockSize + uint32(segReader.chunkOffset))
	segReader.blockNumber = nextChunk.BlockNumber
	segReader.chunkOffset = nextChunk.ChunkOffset
	return value, chunkPosition, nil
}

func (cp *ChunkPosition) Encode() []byte {
	return cp.encode(true)
}

func (cp *ChunkPosition) EncodeFixedSize() []byte {
	return cp.encode(false)
}

func (cp *ChunkPosition) encode(shrink bool) []byte {
	buf := make([]byte, maxLen)
	var index = 0
	index += binary.PutUvarint(buf[index:], uint64(cp.SegmentId))
	index += binary.PutUvarint(buf[index:], uint64(cp.BlockNumber))
	index += binary.PutUvarint(buf[index:], uint64(cp.ChunkOffset))
	index += binary.PutUvarint(buf[index:], uint64(cp.ChunkSize))
	if shrink {
		return buf[:index]
	}
	return buf
}

func DecodeChunkPosition(buf []byte) *ChunkPosition {
	if len(buf) == 0 {
		return nil
	}
	var index = 0
	segmentId, n := binary.Uvarint(buf[index:])
	index += n
	blockNumber, n := binary.Uvarint(buf[index:])
	index += n
	chunkOffset, n := binary.Uvarint(buf[index:])
	index += n
	chunkSize, n := binary.Uvarint(buf[index:])
	index += n
	return &ChunkPosition{
		SegmentId:   uint32(segmentId),
		BlockNumber: uint32(blockNumber),
		ChunkOffset: int64(chunkOffset),
		ChunkSize:   uint32(chunkSize),
	}
}
