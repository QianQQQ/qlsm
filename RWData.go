package lsm

import (
	"encoding/json"
	"log"
	"qlsm/kv"
)

// Get 获取一个元素
func Get[T any](key string) (ans T, ok bool) {
	db.RLock()
	defer db.RUnlock()
	//log.Printf("Get %s", key)
	// 先查内存表
	value, result := db.MemTable.Search(key)
	if result == kv.Success {
		return getInstance[T](value.Value)
	}
	if result == kv.Deleted {
		return ans, false
	}
	// 再逐层查 SsTable 文件
	if db.TablesTree != nil {
		value, result = db.TablesTree.Search(key)
		if result == kv.Success {
			return getInstance[T](value.Value)
		}
		if result == kv.Deleted {
			return ans, false
		}
	}
	return ans, false
}

// Set 插入元素
func Set[T any](key string, value T) bool {
	db.Lock()
	defer db.Unlock()
	//log.Printf("Insert %s", key)
	data, err := json.Marshal(value)
	if err != nil {
		log.Println(err)
		return false
	}
	db.MemTable.Set(key, data)
	// 写入 wal.log
	db.Wal.Write(kv.Data{
		Key:     key,
		Value:   data,
		Deleted: false,
	})
	return true
}

// Delete 删除元素
func Delete(key string) {
	db.Lock()
	defer db.Unlock()
	//log.Printf("Delete %s", key)
	db.MemTable.Delete(key)
	db.Wal.Write(kv.Data{
		Key:     key,
		Value:   nil,
		Deleted: true,
	})
}

// 将字节数组转为类型对象
func getInstance[T any](data []byte) (T, bool) {
	var value T
	err := json.Unmarshal(data, &value)
	if err != nil {
		log.Println(err)
	}
	return value, true
}
