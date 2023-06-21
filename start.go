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

	log.Println("Loading the Configuration...")
	config.Init(cfg)

	log.Println("Initializing the db...")
	initDatabase(cfg.DataDir)

	go Check()
}

// 初始化 DB, 从磁盘文件中还原 SsTable、Wal、MemTable等
func initDatabase(dir string) {
	if _, err := os.Stat(dir); err != nil {
		log.Printf("The %s directory does not exist.\n The %s directory is being created.\n", dir, dir)
		if err = os.Mkdir(dir, 0666); err != nil {
			log.Panicln("Can not create the db directory:", err)
		}
	}
	db = &DB{
		MemoryTree: memTable.NewSL(),
		Wal:        &wal.Wal{},
		TableTree:  &ssTable.TablesTree{},
	}

	log.Println("loading Wal, recovery for MemTable...")
	db.MemoryTree = db.Wal.Load(dir)
	log.Println("Loading db...")
	db.TableTree.Init(dir)
}
