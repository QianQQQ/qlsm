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

// Check 检查是否需要压缩数据库文件
func (tt *TablesTree) Check() {
	tt.majorCompaction()
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

func (tt *TablesTree) majorCompaction() {
	cfg := config.GetConfig()
	for levelIndex := range tt.levels {
		// 转为 MB
		tableSize := int(tt.getLevelSize(levelIndex) >> 20)
		// 如果 db 文件数量 > PartSize 或者 该层 db 文件 总大小 > levelMaxSize, 触发对应层的 compaction
		if tt.getCount(levelIndex) > cfg.PartSize || tableSize > levelMaxSize[levelIndex] {
			tt.majorCompactionLevel(levelIndex)
		}
	}
}

// 压缩当前层的文件到下一层, 只能被 majorCompaction() 调用
func (tt *TablesTree) majorCompactionLevel(level int) {
	log.Println("compressing layer", level, "files")
	start := time.Now()
	defer func() {
		log.Println("completed compression, consumption of time", time.Since(start))
	}()

	// 用于加载一个 SsTable 的数据区到缓存中
	log.Printf("compressing layer %d.db files\n", level)
	//tableCache := make([]byte, levelMaxSize[level])
	curr := tt.levels[level]

	// 将当前层的 SsTable 合并到一个 MemTable 中
	mt := skiplist.New()
	tt.Lock()
	for curr != nil {
		t := curr.table
		// 将 SsTable 的数据区加载到 tableCache 内存中
		//if int64(len(tableCache)) < t.metaInfo.dataLen {
		//	tableCache = make([]byte, t.metaInfo.dataLen)
		//}
		newSlice := make([]byte, t.metaInfo.dataLen)
		// 读取 SsTable 的数据区
		if _, err := t.f.Seek(0, 0); err != nil {
			log.Println("can not open file ", t.filepath)
			panic(err)
		}
		if _, err := t.f.Read(newSlice); err != nil {
			log.Println("can not read file ", t.filepath)
			panic(err)
		}
		// 读取每一个元素
		for k, position := range t.sparseIndex {
			if !position.Deleted {
				var value kv.Data
				if err := json.Unmarshal(newSlice[position.Start:(position.Start+position.Len)], &value); err != nil {
					log.Fatal(err)
				}
				mt.Set(k, value.Value, false)
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
	tt.createTable(values, newLevel)
	// 清理该层的文件
	oldNode := tt.levels[level]
	// 重置该层
	if level < 10 {
		tt.clearLevel(oldNode)
		tt.levels[level] = nil
	}

}

func (tt *TablesTree) clearLevel(oldNode *tableNode) {
	tt.Lock()
	defer tt.Unlock()
	// 清理当前层的每个的 SsTable
	for oldNode != nil {
		oldNode.table.Lock()
		err := oldNode.table.f.Close()
		if err != nil {
			log.Println("error close file,", oldNode.table.filepath)
			panic(err)
		}
		err = os.Remove(oldNode.table.filepath)
		if err != nil {
			log.Println("error delete file,", oldNode.table.filepath)
			panic(err)
		}
		oldNode.table.Unlock()
		oldNode.table.f = nil
		oldNode.table = nil
		oldNode = oldNode.next
	}
}
