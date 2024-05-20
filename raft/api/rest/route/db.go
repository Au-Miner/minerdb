package route

import (
	"encoding/json"
	"minerdb/min"
	"minerdb/raft/api/rest/json_response"
	"minerdb/raft/cluster"
	"minerdb/raft/cluster/consensus/fsm"
	"strings"
)

func (a *ApiCtx) storeGet(ctx *min.Context) {
	payload := new(fsm.Payload)

	err := json.NewDecoder(ctx.Req.Body).Decode(&payload)
	if err != nil {
		json_response.BadRequest(ctx, "request body is not valid JSON")
		return
	}
	value, errGet := a.Node.FSM.Get(payload.Key)
	if errGet != nil {
		if strings.Contains(strings.ToLower(errGet.Error()), "key not found") {
			json_response.NotFound(ctx, "key doesn't exist")
			return
		}
		json_response.ServerError(ctx, "couldn't get key from DB: "+errGet.Error())
		return
	}
	json_response.OK(ctx, "data retrieved successfully", value)
}

func (a *ApiCtx) storeGetKeys(ctx *min.Context) {
	keys := a.Node.FSM.GetKeys()
	if len(keys) <= 0 {
		json_response.NotFound(ctx, "no keys in DB")
		return
	}
	json_response.OK(ctx, "data retrieved successfully", keys)
	return
}

func (a *ApiCtx) storeSet(ctx *min.Context) {
	const operationType = "SET"
	payload := new(fsm.Payload)
	err := json.NewDecoder(ctx.Req.Body).Decode(&payload)
	if err != nil {
		json_response.BadRequest(ctx, "request body is not valid JSON")
		return
	}
	payload.Operation = operationType
	errCluster := cluster.Execute(a.Node.Consensus, payload)
	if errCluster != nil {
		json_response.ServerError(ctx, errCluster.Error())
		return
	}
	json_response.OK(ctx, "data persisted successfully", "")
}

func (a *ApiCtx) storeDelete(ctx *min.Context) {
	const operationType = "DELETE"
	payload := new(fsm.Payload)
	err := json.NewDecoder(ctx.Req.Body).Decode(&payload)
	if err != nil {
		json_response.BadRequest(ctx, "request body is not valid JSON")
		return
	}
	payload.Operation = operationType
	errCluster := cluster.Execute(a.Node.Consensus, payload)
	if errCluster != nil {
		if strings.Contains(strings.ToLower(errCluster.Error()), "key not found") {
			json_response.NotFound(ctx, "key doesn't exist")
			return
		}
		json_response.ServerError(ctx, errCluster.Error())
		return
	}
	json_response.OK(ctx, "data deleted successfully", "")
}

func (a *ApiCtx) storeBackup(ctx *min.Context) {
	// TODO
	// backup, err := a.Node.FSM.BackupDB()
	// if err != nil {
	// 	json_response.ServerError(ctx, "couldn't backup DB: "+err.Error())
	// 	return
	// }
	// headers := make(map[string]string)
	// headers["Content-Disposition"] = "attachment; filename=backup.db"
	// headers["Content-Type"] = "application/octet-stream"
	// for k, v := range headers {
	// 	ctx.Response().Header.Set(k, v)
	// }
	// ctx.SendStream(bytes.NewReader(backup), len(backup))
}

func (a *ApiCtx) restoreBackup(ctx *min.Context) {
	// TODO
	// const (
	// 	key           = "backup"
	// 	operationType = "RESTOREDB"
	// )
	// formFile, errFormFile := ctx.FormFile(key)
	// if errFormFile != nil {
	// 	errMsg := fmt.Sprintf("couldn't get backup file: %v. Note: Key is '%s'", errFormFile, key)
	// 	json_response.ServerError(ctx, errMsg)
	// 	return
	// }
	// multiPartFile, errFileOpen := formFile.Open()
	// if errFileOpen != nil {
	// 	json_response.ServerError(ctx, "couldn't open the received backup file: "+errFileOpen.Error())
	// 	return
	// }
	// defer multiPartFile.Close()
	// buf := make([]byte, formFile.Size)
	// _, errRead := multiPartFile.Read(buf)
	// if errRead != nil && errRead != io.EOF {
	// 	json_response.ServerError(ctx, "couldn't read the received backup file: "+errRead.Error())
	// 	return
	// }
	// payload := &fsm.Payload{
	// 	Operation: operationType,
	// 	Value:     json.RawMessage(buf),
	// }
	// errCluster := cluster.Execute(a.Node.Consensus, payload)
	// if errCluster != nil {
	// 	json_response.ServerError(ctx, errCluster.Error())
	// 	return
	// }
	// keys := a.Node.FSM.GetKeys()
	// if len(keys) <= 0 {
	// 	json_response.NotFound(ctx, "no keys were found in the DB after the restoring the backup file")
	// 	return
	// }
	// json_response.OK(ctx, "data restored successfully", keys)
}
