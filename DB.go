package lsm

import (
	"qlsm/memTable"
	"qlsm/ssTable"
	"qlsm/wal"
)

type DB struct {
	// 内存表
	MemoryTree *memTable.SL
	// SsTable 列表
	TableTree *ssTable.TablesTree
	// WalF 文件句柄
	Wal *wal.Wal
}

// 数据库, 全局唯一实例
var db *DB
