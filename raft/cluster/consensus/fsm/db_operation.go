package fsm

func (dbFSM DatabaseFSM) Get(k string) (any, error) {
	val, err := dbFSM.db.Get([]byte(k))
	return string(val), err
}

// GetKeys 获取所有的key
func (dbFSM DatabaseFSM) GetKeys() []string {
	// TODO
	return nil
}

func (dbFSM DatabaseFSM) set(k string, value any) error {
	return dbFSM.db.Put([]byte(k), []byte(value.(string)))
}

func (dbFSM DatabaseFSM) delete(k string) error {
	return dbFSM.db.Delete([]byte(k))
}

func (dbFSM DatabaseFSM) BackupDB() ([]byte, error) {
	// TODO
	return nil, nil
}

func (dbFSM DatabaseFSM) RestoreDB(contents any) error {
	// TODO
	return nil
}
