package fsm

import (
	"encoding/json"
	"fmt"
	"github.com/hashicorp/raft"
	"github.com/narvikd/errorskit"
	"io"
	"minerdb/minerdb/common/constrants"
	"minerdb/minerdb/execution"
)

// DatabaseFSM 表示数据库的有限状态机实现
type DatabaseFSM struct {
	db *execution.MinerDB
}

// snapshot 仅作为raft.FSM的Snapshot方法的实现，没有实际意义
type snapshot struct{}

// Payload 用于发送raft.Apply的信息
type Payload struct {
	Key       string `json:"key" validate:"required"`
	Value     any    `json:"value"`
	Operation string `json:"operation"`
}

// ApplyRes 表示了raft.Apply的返回值
type ApplyRes struct {
	Data  any
	Error error
}

// Apply 处理Raft的log entry
func (dbFSM DatabaseFSM) Apply(log *raft.Log) any {
	switch log.Type {
	case raft.LogCommand:
		p := new(Payload)
		errUnMarshal := json.Unmarshal(log.Data, p)
		if errUnMarshal != nil {
			return errorskit.Wrap(errUnMarshal, "couldn't unmarshal storage payload")
		}
		switch p.Operation {
		case "SET":
			return &ApplyRes{
				Error: dbFSM.set(p.Key, p.Value),
			}
		case "DELETE":
			return &ApplyRes{
				Error: dbFSM.delete(p.Key),
			}
		case "RESTOREDB":
			return &ApplyRes{
				Error: dbFSM.RestoreDB(p.Value),
			}
		default:
			return &ApplyRes{
				Error: fmt.Errorf("operation type not recognized: %v", p.Operation),
			}
		}
	default:
		return fmt.Errorf("raft command not recognized: %v", log.Type)
	}
}

// Restore 从快照中恢复有限状态机
func (dbFSM DatabaseFSM) Restore(snap io.ReadCloser) error {
	d := json.NewDecoder(snap)
	for d.More() {
		dbValue := new(Payload)
		errDecode := d.Decode(&dbValue)
		if errDecode != nil {
			return errorskit.Wrap(errDecode, "couldn't decode snapshot")
		}

		errSet := dbFSM.set(dbValue.Key, dbValue.Value)
		if errSet != nil {
			return errorskit.Wrap(errSet, "couldn't restore key while restoring a snapshot")
		}
	}
	// 检查是否到达流的末尾
	_, errToken := d.Token()
	if errToken != nil && errToken != io.EOF { // If we reach the end of the stream, it isn't an error
		return errorskit.Wrap(errToken, "couldn't restore snapshot due to json malformation")
	}
	return snap.Close()
}

// Snapshot 是raft.FSM的interface，用于创建系统的当前状态的快照
// MinerDB已经持久化数据，这里不需要执行
func (dbFSM DatabaseFSM) Snapshot() (raft.FSMSnapshot, error) {
	return snapshot{}, nil
}

// Persist MinerDB已经持久化数据，这里不需要执行
func (s snapshot) Persist(_ raft.SnapshotSink) error {
	return nil
}

// Release MinerDB已经持久化数据，这里不需要执行
func (s snapshot) Release() {}

// New 创建DatabaseFSM实例
func New(storageDir string) (*DatabaseFSM, error) {
	database, err := newDB(storageDir)
	if err != nil {
		return nil, err
	}
	return &DatabaseFSM{db: database}, nil
}

// 创建MinerDB数据库
func newDB(storageDir string) (*execution.MinerDB, error) {
	options := constrants.DefaultOptions
	options.DirPath = storageDir
	return execution.Open(options)
}
