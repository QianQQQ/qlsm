package lsm

import (
	"log"
	"qlsm/config"
	"time"
)

func Check() {
	cfg := config.GetConfig()
	checkMemory()
	db.TablesTree.Compaction()
	ticker := time.Tick(time.Duration(cfg.CheckInterval) * time.Millisecond)
	for range ticker {
		checkMemory()
		db.TablesTree.Compaction()
	}
}

func checkMemory() {
	cfg := config.GetConfig()
	count := db.MemTable.GetCount()
	if count < cfg.Threshold {
		return
	}
	log.Printf("MemTable has %d Nodes, compressing memory\n", count)
	tmp := db.MemTable.Swap()
	// 将内存表存储到 SsTable 中
	db.TablesTree.CreateTable(tmp.GetValues(), 0)
	db.Wal.Reset()
}
