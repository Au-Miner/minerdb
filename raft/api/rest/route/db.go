package route

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/gofiber/fiber/v2"
	"github.com/narvikd/fiberparser"
	"io"
	"jdb/raft/api/rest/json_response"
	"jdb/raft/cluster"
	"jdb/raft/cluster/consensus/fsm"
	"strings"
)

func (a *ApiCtx) storeGet(fiberCtx *fiber.Ctx) error {
	payload := new(fsm.Payload)
	errParse := fiberparser.ParseAndValidate(fiberCtx, payload)
	if errParse != nil {
		return json_response.BadRequest(fiberCtx, errParse.Error())
	}
	value, errGet := a.Node.FSM.Get(payload.Key)
	if errGet != nil {
		if strings.Contains(strings.ToLower(errGet.Error()), "key not found") {
			return json_response.NotFound(fiberCtx, "key doesn't exist")
		}
		return json_response.ServerError(fiberCtx, "couldn't get key from DB: "+errGet.Error())
	}
	return json_response.OK(fiberCtx, "data retrieved successfully", value)
}

func (a *ApiCtx) storeGetKeys(fiberCtx *fiber.Ctx) error {
	keys := a.Node.FSM.GetKeys()
	if len(keys) <= 0 {
		return json_response.NotFound(fiberCtx, "no keys in DB")
	}
	return json_response.OK(fiberCtx, "data retrieved successfully", keys)
}

func (a *ApiCtx) storeSet(fiberCtx *fiber.Ctx) error {
	const operationType = "SET"
	payload := new(fsm.Payload)
	errParse := fiberparser.ParseAndValidate(fiberCtx, payload)
	if errParse != nil {
		return json_response.BadRequest(fiberCtx, errParse.Error())
	}
	payload.Operation = operationType
	errCluster := cluster.Execute(a.Node.Consensus, payload)
	if errCluster != nil {
		return json_response.ServerError(fiberCtx, errCluster.Error())
	}
	return json_response.OK(fiberCtx, "data persisted successfully", "")
}

func (a *ApiCtx) storeDelete(fiberCtx *fiber.Ctx) error {
	const operationType = "DELETE"
	payload := new(fsm.Payload)
	errParse := fiberparser.ParseAndValidate(fiberCtx, payload)
	if errParse != nil {
		return json_response.BadRequest(fiberCtx, errParse.Error())
	}
	payload.Operation = operationType
	errCluster := cluster.Execute(a.Node.Consensus, payload)
	if errCluster != nil {
		if strings.Contains(strings.ToLower(errCluster.Error()), "key not found") {
			return json_response.NotFound(fiberCtx, "key doesn't exist")
		}
		return json_response.ServerError(fiberCtx, errCluster.Error())
	}
	return json_response.OK(fiberCtx, "data deleted successfully", "")
}

func (a *ApiCtx) storeBackup(fiberCtx *fiber.Ctx) error {
	backup, err := a.Node.FSM.BackupDB()
	if err != nil {
		return json_response.ServerError(fiberCtx, "couldn't backup DB: "+err.Error())
	}
	headers := make(map[string]string)
	headers["Content-Disposition"] = "attachment; filename=backup.db"
	headers["Content-Type"] = "application/octet-stream"
	for k, v := range headers {
		fiberCtx.Response().Header.Set(k, v)
	}
	return fiberCtx.SendStream(bytes.NewReader(backup), len(backup))
}

func (a *ApiCtx) restoreBackup(fiberCtx *fiber.Ctx) error {
	const (
		key           = "backup"
		operationType = "RESTOREDB"
	)
	formFile, errFormFile := fiberCtx.FormFile(key)
	if errFormFile != nil {
		errMsg := fmt.Sprintf("couldn't get backup file: %v. Note: Key is '%s'", errFormFile, key)
		return json_response.ServerError(fiberCtx, errMsg)
	}
	multiPartFile, errFileOpen := formFile.Open()
	if errFileOpen != nil {
		return json_response.ServerError(fiberCtx, "couldn't open the received backup file: "+errFileOpen.Error())
	}
	defer multiPartFile.Close()
	buf := make([]byte, formFile.Size)
	_, errRead := multiPartFile.Read(buf)
	if errRead != nil && errRead != io.EOF {
		return json_response.ServerError(fiberCtx, "couldn't read the received backup file: "+errRead.Error())
	}
	payload := &fsm.Payload{
		Operation: operationType,
		Value:     json.RawMessage(buf),
	}
	errCluster := cluster.Execute(a.Node.Consensus, payload)
	if errCluster != nil {
		return json_response.ServerError(fiberCtx, errCluster.Error())
	}
	keys := a.Node.FSM.GetKeys()
	if len(keys) <= 0 {
		return json_response.NotFound(fiberCtx, "no keys were found in the DB after the restoring the backup file")
	}
	return fiberCtx.Status(fiber.StatusOK).JSON(&fiber.Map{
		"message": "data restored successfully",
		"keys":    keys,
	})
}
