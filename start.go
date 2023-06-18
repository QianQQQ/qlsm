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
	log.Println("Loading the Configuration")
	config.Init(cfg)
	// 初始化数据库
	log.Println("Initializing the db")
	initDatabase(cfg.DataDir)

	// 数据库启动前进行一次数据压缩
	log.Println("Performing background checks...")
	checkMemory()
	db.TableTree.Check()
	go Check()
}

// 初始化 DB, 从磁盘文件中还原 SsTable、WalF、内存表等
func initDatabase(dir string) {
	db = &DB{
		MemoryTree: &memTable.BST{},
		Wal:        &wal.Wal{},
		TableTree:  &ssTable.TablesTree{},
	}
	if _, err := os.Stat(dir); err != nil {
		log.Printf("The %s directory does not exist.\n The %s directory is being created.\n", dir, dir)
		if err = os.Mkdir(dir, 0666); err != nil {
			log.Panicln("Can not create the db directory:", err)
		}
	}
	// 从数据目录中，加载 WalF、db 文件
	// 非空数据库，则开始恢复数据，加载 WalF 和 SsTable 文件
	memoryTree := db.Wal.Load(dir)
	db.MemoryTree = memoryTree
	log.Println("Loading db...")
	db.TableTree.Init(dir)
}
