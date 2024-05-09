package test

import (
	"fmt"
	"github.com/rosedblabs/rosedb/v2"
	"github.com/rosedblabs/rosedb/v2/utils"
	"log"
	"os"
	"testing"
	"time"
)

func TestBasic(t *testing.T) {
	options := rosedb.DefaultOptions
	options.DirPath = "/tmp/jdb_basic"
	defer func() {
		_ = os.RemoveAll(options.DirPath)
	}()

	db, err := rosedb.Open(options)
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

func TestBatch(t *testing.T) {
	options := rosedb.DefaultOptions
	options.DirPath = "/tmp/rosedb_batch"

	db, err := rosedb.Open(options)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close()
	}()

	batch := db.NewBatch(rosedb.DefaultBatchOptions)

	_ = batch.Put([]byte("name"), []byte("rosedb"))

	val, _ := batch.Get([]byte("name"))
	println(string(val))

	_ = batch.Delete([]byte("name"))

	_ = batch.Commit()
}

func TestMerge(t *testing.T) {
	options := rosedb.DefaultOptions
	options.DirPath = "/tmp/rosedb_merge"

	db, err := rosedb.Open(options)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close()
	}()

	for i := 0; i < 100000; i++ {
		_ = db.Put([]byte(utils.GetTestKey(i)), utils.RandomValue(128))
	}

	for i := 0; i < 100000/2; i++ {
		_ = db.Delete([]byte(utils.GetTestKey(i)))
	}

	_ = db.Merge(true)
}

func TestWatch(t *testing.T) {
	options := rosedb.DefaultOptions
	options.DirPath = "/tmp/rosedb_watch"
	options.WatchQueueSize = 1000

	db, err := rosedb.Open(options)
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
				fmt.Println("The db is closed, so the watch channel is closed.")
				return
			}
			fmt.Printf("Get a new event: key%s \n", event.Key)
		}
	}()

	for i := 0; i < 10; i++ {
		_ = db.Put(utils.GetTestKey(i), utils.RandomValue(64))
	}

	for i := 0; i < 10/2; i++ {
		_ = db.Delete(utils.GetTestKey(i))
	}

	time.Sleep(1 * time.Second)
}

func TestTTL(t *testing.T) {
	options := rosedb.DefaultOptions
	options.DirPath = "/tmp/rosedb_ttl"

	db, err := rosedb.Open(options)
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

	_ = db.Put([]byte("name2"), []byte("rosedb2"))

	err = db.Expire([]byte("name2"), time.Second*2)
	if err != nil {
		panic(err)
	}
	ttl, err = db.TTL([]byte("name2"))
	if err != nil {
		log.Println(err)
	}
	println(ttl.String())
}

func TestIterate(t *testing.T) {
	options := rosedb.DefaultOptions
	options.DirPath = "/tmp/rosedb_iterate"

	db, err := rosedb.Open(options)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close()
	}()

	_ = db.Put([]byte("key13"), []byte("value13"))
	_ = db.Put([]byte("key11"), []byte("value11"))
	_ = db.Put([]byte("key35"), []byte("value35"))
	_ = db.Put([]byte("key27"), []byte("value27"))
	_ = db.Put([]byte("key41"), []byte("value41"))

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
