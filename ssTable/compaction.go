package ssTable

import (
	"encoding/json"
	"log"
	"os"
	"qlsm/config"
	"qlsm/kv"
	"qlsm/memTable/skiplist"
	"time"
)

// Compaction 对 SsTable 进行压实
func (tt *TablesTree) Compaction() {
	cfg := config.GetConfig()
	for levelIndex := range tt.levels {
		// 转为 MB
		tableSize := int(tt.getLevelSize(levelIndex) >> 20)
		// 如果 db 文件数量 > PartSize 或者 该层 db 文件 总大小 > levelMaxSize, 触发对应层的 compaction
		if tt.getCount(levelIndex) >= cfg.PartSize || tableSize >= levelMaxSize[levelIndex] {
			log.Printf("compress level %d Sstables, the tableSize is %d MB", levelIndex, tableSize)
			tt.majorCompactionLevel(levelIndex)
		}
	}
}

// 压缩当前层的文件到下一层, 只能被 Compaction() 调用
func (tt *TablesTree) majorCompactionLevel(level int) {
	start := time.Now()
	defer func() {
		log.Println("completed compression, consumption of time", time.Since(start))
	}()

	curr := tt.levels[level]

	// 将当前层的 SsTable 合并到一个 MemTable 中
	mt := skiplist.New()
	tt.Lock()
	for curr != nil {
		t := curr.table
		data := make([]byte, t.metaInfo.dataLen)
		// 读取 SsTable 的数据区
		if _, err := t.f.Seek(0, 0); err != nil {
			log.Println("fail to open file ", t.filepath)
			panic(err)
		}
		if _, err := t.f.Read(data); err != nil {
			log.Println("fail to read file ", t.filepath)
			panic(err)
		}
		// 读取每一个元素
		for k, p := range t.sparseIndex {
			if !p.Deleted {
				var value kv.Data
				if err := json.Unmarshal(data[p.Start:(p.Start+p.Len)], &value); err != nil {
					log.Fatal(err)
				}
				mt.Set(k, value.Value)
			} else {
				mt.Delete(k)
			}
		}
		curr = curr.next
	}
	tt.Unlock()
	// 将 SortTree 压缩合并成一个 SsTable
	values := mt.GetValues()
	newLevel := level + 1
	// 目前最多支持 10 层
	if newLevel > 10 {
		newLevel = 10
	}
	// 创建新的 SsTable
	tt.CreateTable(values, newLevel)
	// 清理该层的文件
	oldNode := tt.levels[level]
	// 重置该层
	if level < 10 {
		tt.clearLevel(oldNode)
		tt.levels[level] = nil
	}
}

func (tt *TablesTree) getCount(level int) int {
	curr := tt.levels[level]
	count := 0
	for curr != nil {
		count++
		curr = curr.next
	}
	return count
}

func (tt *TablesTree) clearLevel(oldNode *tableNode) {
	tt.Lock()
	defer tt.Unlock()
	// 清理当前层的每个的 SsTable
	for oldNode != nil {
		oldNode.table.Lock()
		err := oldNode.table.f.Close()
		if err != nil {
			log.Println("fail to close file,", oldNode.table.filepath)
			panic(err)
		}
		err = os.Remove(oldNode.table.filepath)
		if err != nil {
			log.Println("fail to delete file,", oldNode.table.filepath)
			panic(err)
		}
		oldNode.table.Unlock()
		oldNode.table.f = nil
		oldNode.table = nil
		oldNode = oldNode.next
	}
}
