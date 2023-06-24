package lsm

import (
	"log"
	"os"
	"qlsm/config"
	"qlsm/memTable"
	"qlsm/memTable/skiplist"
	"qlsm/ssTable"
	"qlsm/wal"
)

type DB struct {
	MemTable   memTable.MemTable
	TablesTree *ssTable.TablesTree
	Wal        *wal.Wal
}

var db *DB

// Start 启动数据库
func Start(cfg config.Config) {
	if db != nil {
		return
	}

	log.Println("loading the configuration...")
	config.Init(cfg)

	log.Println("initializing DB...")
	initDatabase(cfg.DataDir)

	log.Println("start checking in the background...")
	go Check()
}

// 初始化 DB, 从磁盘文件中还原 SsTable, Wal, MemTable
func initDatabase(dir string) {
	if _, err := os.Stat(dir); err != nil {
		log.Printf("the %s directory does not exist.\n the %s directory is being created.\n", dir, dir)
		if err = os.Mkdir(dir, 0666); err != nil {
			log.Panicln("fail to create the db directory:", err)
		}
	}
	db = &DB{
		MemTable:   skiplist.New(),
		Wal:        &wal.Wal{},
		TablesTree: &ssTable.TablesTree{},
	}

	log.Println("loading Wal, recovering MemTable...")
	db.MemTable = db.Wal.Load(dir)

	log.Println("loading DB...")
	db.TablesTree.Init(dir)
}
