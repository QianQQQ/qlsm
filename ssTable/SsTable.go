package ssTable

import (
	"encoding/binary"
	"encoding/json"
	"log"
	"os"
	"qlsm/kv"
	"sync"
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

// Load 将 db 文件 加载成 SsTable, sparseIndex 常驻内存
func (t *SsTable) Load(filepath string) {
	t.filepath = filepath
	t.sparseIndex = map[string]Position{}

	// 加载文件句柄
	f, err := os.OpenFile(t.filepath, os.O_RDONLY, 0666)
	if err != nil {
		log.Panic("fail to open file", t.filepath, ":", err.Error())
	}
	t.f = f

	// 加载元数据
	if _, err = f.Seek(-8*5, 2); err != nil {
		log.Panic("fail to seek metadata for version:", err.Error())
	}
	_ = binary.Read(f, binary.LittleEndian, &t.metaInfo.version)

	if _, err = f.Seek(-8*4, 2); err != nil {
		log.Panic("fail to seek metadata for dataStart:", err.Error())
	}
	_ = binary.Read(f, binary.LittleEndian, &t.metaInfo.dataStart)

	if _, err = f.Seek(-8*3, 2); err != nil {
		log.Panic("fail to seek metadata for dataLen:", err.Error())
	}
	_ = binary.Read(f, binary.LittleEndian, &t.metaInfo.dataLen)

	if _, err = f.Seek(-8*2, 2); err != nil {
		log.Panic("fail to seek metadata for indexStart:", err.Error())
	}
	_ = binary.Read(f, binary.LittleEndian, &t.metaInfo.indexStart)

	if _, err = f.Seek(-8*1, 2); err != nil {
		log.Panic("fail to seek metadata for indexLen:", err.Error())
	}
	_ = binary.Read(f, binary.LittleEndian, &t.metaInfo.indexLen)

	// 加载稀疏索引区
	bs := make([]byte, t.metaInfo.indexLen)
	if _, err = f.Seek(t.metaInfo.indexStart, 0); err != nil {
		log.Panic("fail to seek sparseIndex:", err.Error())
	}
	if _, err = f.Read(bs); err != nil {
		log.Panic("fail to read sparseIndex:", err.Error())
	}
	if err = json.Unmarshal(bs, &t.sparseIndex); err != nil {
		log.Panic("fail to unmarshal for sparseIndex:", err.Error())
	}
	_, _ = f.Seek(0, 0)
}

// Search 先通过 sparseIndex 找到 Position, 再从数据区加载
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
	// 从磁盘文件中查找
	bs := make([]byte, position.Len)
	if _, err := t.f.Seek(position.Start, 0); err != nil {

		log.Println("fail to seek for data:", key, err)
		return kv.Data{}, kv.None
	}
	if _, err := t.f.Read(bs); err != nil {
		log.Println("fail to read for data:", err)
		return kv.Data{}, kv.None
	}
	if err := json.Unmarshal(bs, &value); err != nil {
		log.Println("fail to unmarshal for data:", err)
		return kv.Data{}, kv.None
	}
	return value, kv.Success
}
