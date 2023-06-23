package lsm

import (
	"log"
	"os"
	"qlsm/config"
	"qlsm/memTable"
	"qlsm/ssTable"
	"qlsm/wal"
)

// Start 启动数据库
func Start(cfg config.Config) {
	if db != nil {
		return
	}

	log.Println("loading the configuration...")
	config.Init(cfg)

	log.Println("initializing DB...")
	initDatabase(cfg.DataDir)

	go Check()
}

// 初始化 DB, 从磁盘文件中还原 SsTable、Wal、MemTable等
func initDatabase(dir string) {
	if _, err := os.Stat(dir); err != nil {
		log.Printf("the %s directory does not exist.\n the %s directory is being created.\n", dir, dir)
		if err = os.Mkdir(dir, 0666); err != nil {
			log.Panicln("can not create the db directory:", err)
		}
	}
	db = &DB{
		MemTable:   memTable.NewSL(),
		Wal:        &wal.Wal{},
		TablesTree: &ssTable.TablesTree{},
	}

	log.Println("loading Wal, recovery for MemTable...")
	db.MemTable = db.Wal.Load(dir)
	log.Println("loading DB...")
	db.TablesTree.Init(dir)
}
