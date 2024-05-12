package concurrency

import (
	"github.com/bwmarrin/snowflake"
	"jdb/common/exception"
	"jdb/common/utils"
	"jdb/storage/transaction"
	"sync"
)

type LockMode = int

const (
	SHARED LockMode = iota
	EXCLUSIVE
)

type LockRequest struct {
	batchId  *snowflake.Node // txn_id
	lockMode LockMode
	key      uint64
	granted  bool
}

type LockRequestQueue struct {
	requestQueue []LockRequest
	cv           *sync.Cond
	mu           sync.Mutex
}

type LockManager struct {
	keyLockMap      map[uint64]*LockRequestQueue // key -> 所有的batchId
	keyLockMapLatch sync.Mutex

	waitsFor      map[*snowflake.Node][]*snowflake.Node
	waitsForLatch sync.Mutex
}

func NewLockRequestQueue() *LockRequestQueue {
	lrq := &LockRequestQueue{
		requestQueue: make([]LockRequest, 0),
		mu:           sync.Mutex{},
	}
	lrq.cv = sync.NewCond(&lrq.mu)
	return lrq
}

func NewLockManager() *LockManager {
	return &LockManager{
		keyLockMap:      make(map[uint64]*LockRequestQueue),
		keyLockMapLatch: sync.Mutex{},
		waitsFor:        make(map[*snowflake.Node][]*snowflake.Node),
		waitsForLatch:   sync.Mutex{},
	}
}

func (lm *LockManager) LockRow(batch *transaction.Batch, lockMode LockMode, key []byte) error {
	lm.keyLockMapLatch.Lock()
	hashKey := utils.MemHash(key)
	if _, ok := lm.keyLockMap[hashKey]; !ok {
		lm.keyLockMap[hashKey] = NewLockRequestQueue()
	}
	lockRequestQueue, _ := lm.keyLockMap[hashKey]
	lockRequestQueue.mu.Lock()
	defer lockRequestQueue.mu.Unlock()
	lm.keyLockMapLatch.Unlock()

	newRequest := LockRequest{
		batchId:  batch.BatchId,
		lockMode: lockMode,
		key:      hashKey,
		granted:  false,
	}
	for i, request := range lockRequestQueue.requestQueue {
		if request.batchId == batch.BatchId {
			if request.lockMode == lockMode {
				return nil
			}
			lockRequestQueue.requestQueue[i] = newRequest
			for !GrantLock(&newRequest, lockRequestQueue) {
				lockRequestQueue.cv.Wait()
				if batch.State == transaction.ABORTED {
					lockRequestQueue.requestQueue = removeFromQueue(&lockRequestQueue.requestQueue, &newRequest)
					lockRequestQueue.cv.Broadcast()
					return exception.ErrBatchAborted
				}
			}
			lockRequestQueue.requestQueue[i].granted = true
			lockRequestQueue.cv.Broadcast()
			return nil
		}
	}
	lockRequestQueue.requestQueue = append(lockRequestQueue.requestQueue, newRequest)
	for !GrantLock(&newRequest, lockRequestQueue) {
		lockRequestQueue.cv.Wait()
		if batch.State == transaction.ABORTED {
			lockRequestQueue.requestQueue = removeFromQueue(&lockRequestQueue.requestQueue, &newRequest)
			lockRequestQueue.cv.Broadcast()
			return exception.ErrBatchAborted
		}
	}
	lockRequestQueue.requestQueue[len(lockRequestQueue.requestQueue)-1].granted = true
	lockRequestQueue.cv.Broadcast()
	return nil
}

func (lm *LockManager) UnlockRow(batch *transaction.Batch, key []byte) bool {
	lm.keyLockMapLatch.Lock()
	hashKey := utils.MemHash(key)
	if _, ok := lm.keyLockMap[hashKey]; !ok {
		lm.keyLockMapLatch.Unlock()
		batch.State = transaction.ABORTED
		return false
	}
	lockRequestQueue, _ := lm.keyLockMap[hashKey]
	lockRequestQueue.mu.Lock()
	defer lockRequestQueue.mu.Unlock()
	lm.keyLockMapLatch.Unlock()

	for i, request := range lockRequestQueue.requestQueue {
		if request.batchId == batch.BatchId && request.granted {
			lockRequestQueue.requestQueue = append(lockRequestQueue.requestQueue[:i], lockRequestQueue.requestQueue[i+1:]...)
			lockRequestQueue.cv.Broadcast()
			return true
		}
	}
	batch.State = transaction.ABORTED
	return false
}

func removeFromQueue(queue *[]LockRequest, request *LockRequest) []LockRequest {
	for i, r := range *queue {
		if r == *request {
			return append((*queue)[:i], (*queue)[i+1:]...)
		}
	}
	return *queue
}

func GrantLock(lockRequest *LockRequest, lockRequestQueue *LockRequestQueue) bool {
	for _, request := range lockRequestQueue.requestQueue {
		if request.granted {
			switch lockRequest.lockMode {
			case SHARED:
				if request.lockMode == EXCLUSIVE {
					return false
				}
			case EXCLUSIVE:
				return false
			default:
				return false
			}
		}
	}
	return true
}
