package ssTable

import (
	"log"
	"os"
	"qlsm/config"
	"qlsm/kv"
	"qlsm/memTable"
	"time"
)

// Check 检查是否需要压缩数据库文件
func (tt *TablesTree) Check() {
	tt.majorCompaction()
}

func (tt *TablesTree) getCount(level int) int {
	node := tt.levels[level]
	count := 0
	for node != nil {
		count++
		node = node.next
	}
	return count
}

// 压缩文件
func (tt *TablesTree) majorCompaction() {
	cfg := config.GetConfig()
	for levelIndex := range tt.levels {
		tableSize := int(tt.GetLevelSize(levelIndex) / 1000 / 1000) // 转为 MB
		if tt.getCount(levelIndex) > cfg.PartSize || tableSize > levelMaxSize[levelIndex] {
			tt.majorCompactionLevel(levelIndex)
		}
	}
}

// TODO 如何用外存进行合并
// 压缩当前层的文件到下一层，只能被 majorCompaction() 调用
func (tt *TablesTree) majorCompactionLevel(level int) {
	log.Println("Compressing layer ", level, " files")
	start := time.Now()
	defer func() {
		log.Println("Completed compression,consumption of time : ", time.Since(start))
	}()

	log.Printf("Compressing layer %d.db files\r\n", level)
	// 用于加载 一个 SsTable 的数据区到缓存中
	tableCache := make([]byte, levelMaxSize[level])
	curr := tt.levels[level]

	// 将当前层的 SsTable 合并到一个有序二叉树中
	memoryTree := &memTable.BST{}

	tt.Lock()
	for curr != nil {
		t := curr.table
		// 将 SsTable 的数据区加载到 tableCache 内存中
		if int64(len(tableCache)) < t.metaInfo.dataLen {
			tableCache = make([]byte, t.metaInfo.dataLen)
		}
		newSlice := tableCache[:t.metaInfo.dataLen]
		// 读取 SsTable 的数据区
		if _, err := t.f.Seek(0, 0); err != nil {
			log.Println(" error open file ", t.filepath)
			panic(err)
		}
		if _, err := t.f.Read(newSlice); err != nil {
			log.Println(" error read file ", t.filepath)
			panic(err)
		}
		// 读取每一个元素
		for k, position := range t.sparseIndex {
			if !position.Deleted {
				value, err := kv.Decode(newSlice[position.Start:(position.Start + position.Len)])
				if err != nil {
					log.Fatal(err)
				}
				memoryTree.Set(k, value.Value)
			} else {
				memoryTree.Delete(k)
			}
		}
		curr = curr.next
	}
	tt.Unlock()

	// 将 SortTree 压缩合并成一个 SsTable
	values := memoryTree.GetValues()
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
		tt.levels[level] = nil
		tt.clearLevel(oldNode)
	}

}

func (tt *TablesTree) clearLevel(oldNode *tableNode) {
	tt.Lock()
	defer tt.Unlock()
	// 清理当前层的每个的 SsTable
	for oldNode != nil {
		err := oldNode.table.f.Close()
		if err != nil {
			log.Println(" error close file,", oldNode.table.filepath)
			panic(err)
		}
		err = os.Remove(oldNode.table.filepath)
		if err != nil {
			log.Println(" error delete file,", oldNode.table.filepath)
			panic(err)
		}
		oldNode.table.f = nil
		oldNode.table = nil
		oldNode = oldNode.next
	}
}
