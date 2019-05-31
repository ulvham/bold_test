package main

import (
	"fmt"
	"sync"
	"time"

	. "github.com/ulvham/helper"

	"github.com/boltdb/bolt"
)

func (db *QuickDB) SetVal_(tx *bolt.Tx, buc string, k string, v string) {
	db.Lock()
	b, _ := tx.CreateBucketIfNotExists([]byte(buc))
	b.Put([]byte(k), []byte(v))
	db.Unlock()
}

func (db *QuickDB) GetVal(buc string, k string) {
	db.Lock()
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(buc))
		v := b.Get([]byte(k))
		fmt.Printf("The answer is: %s\n", v)
		return nil
	})
	db.Unlock()
}

type QuickDB struct {
	sync.Mutex
	*bolt.DB
}

//type simulateHandler func(tx *bolt.Tx, qdb *QuickDB)

//func simulateGetHandler(tx *bolt.Tx, qdb *QuickDB) {

//}

func NewQuickDB() *QuickDB {
	return &QuickDB{}
}

type QuickDB_ interface {
	SetVal(string, string, string)
	GetVal(string, string)
	SetVal_(*bolt.Tx, string, string, string)
}

func (db *QuickDB) SetVal(buc string, k string, v string) {
	db.Lock()
	//db.Update(func(tx *bolt.Tx) error {
	//		b, err := tx.CreateBucketIfNotExists([]byte(buc))
	//		if err != nil {
	//			return fmt.Errorf("create bucket: %s", err)
	//		}
	//		err = b.Put([]byte(k), []byte(v))
	//		if err != nil {
	//			return fmt.Errorf("put bucket: %s", err)
	//		}
	//	return nil
	//})
	fmt.Println(k, v)
	db.Unlock()

}

func main() {
	FlagDbg = true
	qdb := NewQuickDB()
	qdb.DB, _ = bolt.Open("test4.db", 0750, &bolt.Options{Timeout: 1 * time.Second})

	var wg sync.WaitGroup

		for i := 1; i <= 10; i++ {
			func(ii int) {
				wg.Add(1)
				fmt.Println(ii)
				qdb.SetVal("buc", "key"+ToStr(ii), "val_"+ToStr(ii))
				wg.Done()
			}(i)
		}
		defer qdb.DB.Close()
	//---
//	tx, _ := qdb.Begin(true)
//	defer tx.Rollback()
//	for i := 1; i <= 1000; i++ {
//		wg.Add(1)
//		func() {
//			qdb.SetVal_(tx, "buc", "key"+ToStr(i), "val_"+ToStr(i))
//			wg.Done()
//		}()
//	}

//	tx.Commit()
//	wg.Wait()
//	//---
//	qdb.DB.Sync()

	qdb.GetVal("buc", "key7")

	//qdb.Update(func(tx *bolt.Tx) error {
	//tx.CreateBucketIfNotExists([]byte("buc"))
	//if err != nil {
	//	return fmt.Errorf("create bucket: %s", err)
	//}
	//	return nil
	//})

	//	for ii := 1; ii <= 1; ii++ {
	//		wg.Add(1)
	//		func(i int) {
	//			db.Update(func(tx *bolt.Tx) error {
	//				b, _ := tx.CreateBucketIfNotExists([]byte("Test6"))
	//				//Dbg(i)
	//				b.Put([]byte("answer3"+ToStr(i)), []byte(ToStr(i)))
	//				return err
	//			})
	//			wg.Done()
	//		}(ii)
	//	}
	//	wg.Wait()
	//	tx.Commit()
	//------
	//	qdb.View(func(tx *bolt.Tx) error {
	//		b := tx.Bucket([]byte("buc"))

	//		b.ForEach(func(k, v []byte) error {
	//			fmt.Printf("key=%s, value=%s\n", k, v)
	//			return nil
	//		})
	//		return nil
	//	})
	//	qdb.View(func(tx *bolt.Tx) error {
	//		b := tx.Bucket([]byte("buc"))
	//		v := b.Get([]byte("key7"))
	//		fmt.Printf("The answer is: %s\n", v)
	//		return nil
	//	})

}
