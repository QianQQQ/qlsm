package lsm

import (
	"qlsm/memTable"
	"qlsm/ssTable"
	"qlsm/wal"
)

type DB struct {
	MemoryTree memTable.MemTable
	TableTree  *ssTable.TablesTree
	Wal        *wal.Wal
}

var db *DB
