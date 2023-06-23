package lsm

import (
	"qlsm/memTable"
	"qlsm/ssTable"
	"qlsm/wal"
)

type DB struct {
	MemTable   memTable.MemTable
	TablesTree *ssTable.TablesTree
	Wal        *wal.Wal
}

var db *DB
