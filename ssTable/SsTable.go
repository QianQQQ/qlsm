package ssTable

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"qlsm/kv"
	"sync"
	"time"
)

/*
索引从数据区开始
0 ─────────────────────────────────────────────────────────►
◄──────────────────────────
          dataLen           ◄────────────────
                                indexLen      ◄─────────────┐
┌──────────────────────────┬─────────────────┬──────────────┤
│                          │                 │              │
│           Data           │   SparseIndex   │   MetaInfo   │
│                          │                 │              │
└──────────────────────────┴─────────────────┴──────────────┘
*/

type SsTable struct {
	f           *os.File            //文件句柄
	filepath    string              // SsTable 文件路径
	metaInfo    MetaInfo            // SsTable 元数据
	sparseIndex map[string]Position // 文件的稀疏索引列表
	sync.Mutex
}

// MetaInfo 是 SsTable 的元数据, 存储在文件的末尾
type MetaInfo struct {
	version    int64 // 版本号
	dataStart  int64 // 数据区起始索引
	dataLen    int64 // 数据区长度
	indexStart int64 // 稀疏索引区起始索引
	indexLen   int64 // 稀疏索引区长度
}

// Position 存储在 SparseIndex 中, 表示 KV 的起始位置和长度
type Position struct {
	Start   int64 // 起始位置
	Len     int64 // 长度
	Deleted bool  //删除标志
}

// Load 将 db 文件 加载成 SsTable
func (t *SsTable) Load(filepath string) {
	t.filepath = filepath
	t.sparseIndex = map[string]Position{}

	// 加载文件句柄
	f, err := os.OpenFile(t.filepath, os.O_RDONLY, 0666)
	if err != nil {
		log.Panicln("can not open file", t.filepath, ":", err.Error())
	}
	t.f = f

	// 加载元数据
	_, err = f.Seek(-8*5, 2)
	if err != nil {
		log.Panicln("can not read metadata for version:", err.Error())
	}
	_ = binary.Read(f, binary.LittleEndian, &t.metaInfo.version)
	_, err = f.Seek(-8*4, 2)
	if err != nil {
		log.Panicln("can not read metadata for dataStart:", err.Error())
	}
	_ = binary.Read(f, binary.LittleEndian, &t.metaInfo.dataStart)
	_, err = f.Seek(-8*3, 2)
	if err != nil {
		log.Panicln("can not read metadata for dataLen:", err.Error())
	}
	_ = binary.Read(f, binary.LittleEndian, &t.metaInfo.dataLen)
	_, err = f.Seek(-8*2, 2)
	if err != nil {
		log.Panicln("can not read metadata for indexStart:", err.Error())
	}
	_ = binary.Read(f, binary.LittleEndian, &t.metaInfo.indexStart)
	_, err = f.Seek(-8*1, 2)
	if err != nil {
		log.Panicln("can not read metadata for indexLen:", err.Error())
	}
	_ = binary.Read(f, binary.LittleEndian, &t.metaInfo.indexLen)

	// 加载稀疏索引区
	bs := make([]byte, t.metaInfo.indexLen)
	if _, err = f.Seek(t.metaInfo.indexStart, 0); err != nil {
		log.Panicln("can not seek sparseIndex:", err.Error())
	}
	if _, err = f.Read(bs); err != nil {
		log.Panicln("can not read sparseIndex:", err.Error())
	}
	if err = json.Unmarshal(bs, &t.sparseIndex); err != nil {
		log.Panicln("can not unmarshal for sparseIndex:", err.Error())
	}
	_, _ = f.Seek(0, 0)
}

// Search 先通过 sparseIndex 找到 Position, 再从数据区加载
// sparseIndex 常驻内存
func (t *SsTable) Search(key string) (value kv.Data, result kv.SearchResult) {
	t.Lock()
	defer t.Unlock()

	position, exist := t.sparseIndex[key]
	if !exist {
		return kv.Data{}, kv.None
	}
	if position.Deleted {
		return kv.Data{}, kv.Deleted
	}
	if t.f == nil {
		fmt.Println(t.f)
		t.Unlock()
		time.Sleep(time.Microsecond * 100)
		value, result = t.Search(key)
		fmt.Println(key, value, result, t.f)
		t.Lock()
		return
	}
	// 从磁盘文件中查找
	bs := make([]byte, position.Len)
	if _, err := t.f.Seek(position.Start, 0); err != nil {
		log.Println(position, t.f, t.filepath)
		log.Println("can not seek for data:", key, err)
		return kv.Data{}, kv.None
	}
	if _, err := t.f.Read(bs); err != nil {
		log.Println("can not read for data:", err)
		return kv.Data{}, kv.None
	}
	if err := json.Unmarshal(bs, &value); err != nil {
		log.Println("can not unmarshal for data:", err)
		return kv.Data{}, kv.None
	}
	return value, kv.Success
}
