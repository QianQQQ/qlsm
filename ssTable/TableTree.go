package ssTable

import (
	"log"
	"os"
	"path"
	"path/filepath"
	"qlsm/config"

	"qlsm/kv"
	"sync"
	"time"
)

var levelMaxSize []int

type tableNode struct {
	index int
	table *SsTable
	next  *tableNode
}

type TablesTree struct {
	levels []*tableNode
	sync.RWMutex
}

// Search 从所有 SsTable 表中查找数据
func (tt *TablesTree) Search(key string) (kv.Data, kv.SearchResult) {
	tt.RLock()
	defer tt.RUnlock()
	// 依次遍历每层 SsTable
	for _, t := range tt.levels {
		// 整理 SsTable 列表
		var tables []*SsTable
		for t != nil {
			tables = append(tables, t.table)
			t = t.next
		}
		// 从最新的 SsTable 开始查找
		for i := len(tables) - 1; i >= 0; i-- {
			value, searchResult := tables[i].Search(key)
			// 未找到, 则查找下一个 SsTable
			if searchResult == kv.None {
				continue
			}
			// 如果找到或已被删除, 则直接返回结果
			return value, searchResult
		}
	}
	// 没有找到
	return kv.Data{}, kv.None
}

// Insert 在 TablesTree 的 level 层的末尾插入 SsTable, 并返回新插入的 SsTable 的 index
func (tt *TablesTree) Insert(t *SsTable, level int) (index int) {
	tt.Lock()
	defer tt.Unlock()
	curr := tt.levels[level]
	newNode := &tableNode{table: t}
	// 简单的按序插入逻辑
	if curr == nil {
		tt.levels[level] = newNode
	} else {
		for curr.next != nil {
			curr = curr.next
		}
		newNode.index = curr.index + 1
		curr.next = newNode
	}
	return newNode.index
}

// Init 初始化 TablesTree
func (tt *TablesTree) Init(dir string) {
	log.Println("the TablesTree starts loading...")
	start := time.Now()
	defer func() { log.Println("the TablesTree finishes loading, consumption of time:", time.Since(start)) }()
	// 获取各层文件大小
	cfg := config.GetConfig()
	levelMaxSize = make([]int, 10)
	levelMaxSize[0] = cfg.Level0Size
	for i := 1; i < 10; i++ {
		levelMaxSize[i] = levelMaxSize[i-1] * 10
	}
	// 加载各层 db 文件
	tt.levels = make([]*tableNode, 10)
	files, err := os.ReadDir(dir)
	if err != nil {
		log.Panicln("failed to read the database files:", err.Error())
	}
	for _, f := range files {
		if path.Ext(f.Name()) == ".db" {
			tt.loadDBFile(path.Join(dir, f.Name()))
		}
	}
}

// 加载一个 db 文件到 TablesTree 中
func (tt *TablesTree) loadDBFile(path string) {
	log.Println("start loading the", path)
	start := time.Now()
	defer func() {
		log.Println("finish loading the ", path, ", consumption of time:", time.Since(start))
	}()
	// 获取 db 对应的 level 和 index 信息
	level, index, err := getSsTableInfo(filepath.Base(path))
	if err != nil {
		log.Println("can not load the", path)
		return
	}
	t := &SsTable{}
	t.Load(path)
	newNode := &tableNode{index: index, table: t}

	// 根据 index 将 SsTable 插入到合适的位置
	curr := tt.levels[level]
	if curr == nil || curr.index > newNode.index {
		tt.levels[level] = newNode
		newNode.next = curr
		return
	}
	for curr.next != nil && curr.next.index <= newNode.index {
		curr = curr.next
	}
	newNode.next = curr.next
	curr.next = newNode
}
