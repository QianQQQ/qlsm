package lsm

import (
	"log"
	"qlsm/config"
	"time"
)

func Check() {
	cfg := config.GetConfig()
	log.Println("performing background checks...")
	checkMemory()
	db.TablesTree.Check()
	ticker := time.Tick(time.Duration(cfg.CheckInterval) * time.Second)
	for range ticker {
		log.Println("performing background checks...")
		checkMemory()
		db.TablesTree.Check()
	}
}

func checkMemory() {
	cfg := config.GetConfig()
	count := db.MemTable.GetCount()
	log.Println("count is", count)
	if count < cfg.Threshold {
		return
	}
	log.Println("compressing memory")
	tmpTree := db.MemTable.Swap()
	// 将内存表存储到 SsTable 中
	db.TablesTree.CreateNewTable(tmpTree.GetValues())
	db.Wal.Reset()
}
