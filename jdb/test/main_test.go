package test

import (
	"fmt"
	"jdb/jdb/common/constrants"
	"jdb/jdb/common/utils"
	"jdb/jdb/execution"
	"os"
	"testing"
	"time"
)

func TestBasic(t *testing.T) {
	options := constrants.DefaultOptions
	options.DirPath = "/tmp/jdb"
	defer func() {
		_ = os.RemoveAll(options.DirPath)
	}()

	db, err := execution.Open(options)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close()
	}()

	err = db.Put([]byte("name"), []byte("jdb"))
	if err != nil {
		panic(err)
	}

	val, err := db.Get([]byte("name"))
	if err != nil {
		panic(err)
	}
	println(string(val))

	err = db.Delete([]byte("name"))
	if err != nil {
		panic(err)
	}
}

func TestLock(t *testing.T) {
	options := constrants.DefaultOptions
	options.DirPath = "/tmp/jdb"
	defer func() {
		_ = os.RemoveAll(options.DirPath)
	}()

	db, err := execution.Open(options)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close()
	}()

	err = db.ConcurrentPut([]byte("name"), []byte("jdb"))
	if err != nil {
		panic(err)
	}

	val, err := db.ConcurrentGet([]byte("name"))
	if err != nil {
		panic(err)
	}
	println(string(val))

	err = db.ConcurrentDelete([]byte("name"))
	if err != nil {
		panic(err)
	}
}

func TestBatch(t *testing.T) {
	options := constrants.DefaultOptions
	options.DirPath = "/tmp/jdb"

	db, err := execution.Open(options)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close()
	}()

	batch := db.NewExecutor(constrants.DefaultBatchOptions)

	_ = batch.ConcurrentPut([]byte("name"), []byte("rosedb"), db.LockManager)

	val, _ := batch.ConcurrentGet([]byte("name"), db.LockManager)
	println(string(val))

	_ = batch.ConcurrentDelete([]byte("name"), db.LockManager)

	_ = batch.Commit()
}

func TestMerge(t *testing.T) {
	options := constrants.DefaultOptions
	options.DirPath = "/tmp/jdb"

	db, err := execution.Open(options)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close()
	}()

	for i := 0; i < 100000; i++ {
		_ = db.ConcurrentPut([]byte(utils.GetTestKey(i)), utils.RandomValue(128))
	}

	for i := 0; i < 100000/2; i++ {
		_ = db.ConcurrentDelete([]byte(utils.GetTestKey(i)))
	}

	_ = db.Merge(true)
}

func TestWatch(t *testing.T) {
	options := constrants.DefaultOptions
	options.DirPath = "/tmp/jdb"
	options.WatchQueueSize = 1000

	db, err := execution.Open(options)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close()
	}()

	go func() {
		eventCh, err := db.Watch()
		if err != nil {
			return
		}
		for {
			event := <-eventCh
			if event == nil {
				fmt.Println("The db is Close, so the watch channel is Close.")
				return
			}
			fmt.Printf("Get a new event: key%s \n", event.Key)
		}
	}()

	for i := 0; i < 10; i++ {
		_ = db.ConcurrentPut(utils.GetTestKey(i), utils.RandomValue(64))
	}

	for i := 0; i < 10/2; i++ {
		_ = db.ConcurrentDelete(utils.GetTestKey(i))
	}

	time.Sleep(1 * time.Second)
}

func TestTTL(t *testing.T) {
	options := constrants.DefaultOptions
	options.DirPath = "/tmp/jdb"

	db, err := execution.Open(options)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close()
	}()

	err = db.PutWithTTL([]byte("name"), []byte("rosedb"), time.Second*5)
	if err != nil {
		panic(err)
	}

	ttl, err := db.TTL([]byte("name"))
	if err != nil {
		panic(err)
	}
	println(ttl.String())

	_ = db.ConcurrentPut([]byte("name2"), []byte("rosedb2"))

	err = db.Expire([]byte("name2"), time.Second*2)
	if err != nil {
		panic(err)
	}
	ttl, err = db.TTL([]byte("name2"))
	if err != nil {
		fmt.Println(err)
	}
	println(ttl.String())
}

func TestIterate(t *testing.T) {
	options := constrants.DefaultOptions
	options.DirPath = "/tmp/jdb"

	db, err := execution.Open(options)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close()
	}()

	_ = db.ConcurrentPut([]byte("key13"), []byte("value13"))
	_ = db.ConcurrentPut([]byte("key11"), []byte("value11"))
	_ = db.ConcurrentPut([]byte("key35"), []byte("value35"))
	_ = db.ConcurrentPut([]byte("key27"), []byte("value27"))
	_ = db.ConcurrentPut([]byte("key41"), []byte("value41"))

	db.AscendKeys(nil, true, func(k []byte) (bool, error) {
		fmt.Println("key = ", string(k))
		return true, nil
	})

	db.Ascend(func(k []byte, v []byte) (bool, error) {
		fmt.Printf("key = %s, value = %s\n", string(k), string(v))
		return true, nil
	})

	db.DescendKeys(nil, true, func(k []byte) (bool, error) {
		fmt.Println("key = ", string(k))
		return true, nil
	})

	db.Descend(func(k []byte, v []byte) (bool, error) {
		fmt.Printf("key = %s, value = %s\n", string(k), string(v))
		return true, nil
	})
}
