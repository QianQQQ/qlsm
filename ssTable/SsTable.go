package ssTable

import (
	"encoding/binary"
	"encoding/json"
	"log"
	"os"
	"qlsm/kv"
	"sort"
	"sync"
)

/*
索引是从数据区开始！
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
	f           *os.File //文件句柄
	filepath    string   // SsTable 文件路径
	metaInfo    MetaInfo
	sparseIndex map[string]Position // 文件的稀疏索引列表
	sortIndex   []string            // 排序后的 key 列表
	lock        sync.Locker
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
	Start   int64
	Len     int64
	Deleted bool
}

// Load 将 db 文件 加载成 SsTable
func (t *SsTable) Load(filepath string) {
	t.filepath = filepath
	t.lock = &sync.Mutex{}
	t.sparseIndex = map[string]Position{}
	// 加载文件句柄
	f, err := os.OpenFile(t.filepath, os.O_RDONLY, 0666)
	if err != nil {
		log.Panicln("Can not open file ", t.filepath, ": ", err.Error())
	}
	t.f = f

	// 加载元数据
	_, err = f.Seek(-8*5, 2)
	if err != nil {
		log.Panicln("Can not read metadata for version:", err.Error())
	}
	_ = binary.Read(f, binary.LittleEndian, &t.metaInfo.version)

	_, err = f.Seek(-8*4, 2)
	if err != nil {
		log.Panicln("Can not read metadata for dataStart:", err.Error())
	}
	_ = binary.Read(f, binary.LittleEndian, &t.metaInfo.dataStart)

	_, err = f.Seek(-8*3, 2)
	if err != nil {
		log.Panicln("Can not read metadata for dataLen:", err.Error())
	}
	_ = binary.Read(f, binary.LittleEndian, &t.metaInfo.dataLen)

	_, err = f.Seek(-8*2, 2)
	if err != nil {
		log.Panicln("Can not read metadata for indexStart:", err.Error())
	}
	_ = binary.Read(f, binary.LittleEndian, &t.metaInfo.indexStart)

	_, err = f.Seek(-8*1, 2)
	if err != nil {
		log.Panicln("Can not read metadata for indexLen:", err.Error())
	}
	_ = binary.Read(f, binary.LittleEndian, &t.metaInfo.indexLen)

	// 加载稀疏索引区
	bs := make([]byte, t.metaInfo.indexLen)
	if _, err = f.Seek(t.metaInfo.indexStart, 0); err != nil {
		log.Panicln("Can not seek sparseIndex:", err.Error())
	}
	if _, err = f.Read(bs); err != nil {
		log.Panicln("Can not read sparseIndex:", err.Error())
	}
	if err = json.Unmarshal(bs, &t.sparseIndex); err != nil {
		log.Panicln("Can not unmarshal for sparseIndex:", err.Error())
	}
	t.sortIndex = make([]string, 0, len(t.sparseIndex))
	for k := range t.sparseIndex {
		t.sortIndex = append(t.sortIndex, k)
	}
	sort.Strings(t.sortIndex)
	_, _ = f.Seek(0, 0)
}

// Search 先从 sortIndex 二分查找 Key, 如果存在, 通过 sparseIndex 找到 Position, 再从数据区加载
// sortIndex 与 sparseIndex 常驻内存
func (t *SsTable) Search(key string) (value kv.Value, result kv.SearchResult) {
	t.lock.Lock()
	defer t.lock.Unlock()

	position := Position{Start: -1}
	l, r := 0, len(t.sortIndex)-1

	// 二分查找法，查找 key 是否存在
	for l <= r {
		m := (l + r) / 2
		if t.sortIndex[m] == key {
			position = t.sparseIndex[key]
			if position.Deleted {
				return kv.Value{}, kv.Deleted
			}
			break
		} else if t.sortIndex[m] < key {
			l = m + 1
		} else if t.sortIndex[m] > key {
			r = m - 1
		}
	}

	if position.Start == -1 {
		return kv.Value{}, kv.None
	}

	// Todo：如果读取失败，需要增加错误处理过程
	// 从磁盘文件中查找
	bs := make([]byte, position.Len)
	if _, err := t.f.Seek(position.Start, 0); err != nil {
		log.Println("Can not seek for data:", err)
		return kv.Value{}, kv.None
	}
	if _, err := t.f.Read(bs); err != nil {
		log.Println("Can not read for data:", err)
		return kv.Value{}, kv.None
	}

	value, err := kv.Decode(bs)
	if err != nil {
		log.Println(err)
		return kv.Value{}, kv.None
	}
	return value, kv.Success
}
