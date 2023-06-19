package lsm

import (
	"log"
	"qlsm/config"
	"time"
)

func Check() {
	cfg := config.GetConfig()
	ticker := time.Tick(time.Duration(cfg.CheckInterval) * time.Second)
	for range ticker {
		log.Println("Performing background checks...")
		checkMemory()
		db.TableTree.Check()
	}
}

func checkMemory() {
	cfg := config.GetConfig()
	count := db.MemoryTree.GetCount()
	if count < cfg.Threshold {
		return
	}
	log.Println("Compressing memory")
	log.Println(db.MemoryTree)
	tmpTree := db.MemoryTree.Swap()
	log.Println(db.MemoryTree)
	// 将内存表存储到 SsTable 中
	db.TableTree.CreateNewTable(tmpTree.GetValues())
	db.Wal.Reset()
}
