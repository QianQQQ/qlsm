package memTable

import "qlsm/kv"

type MemTable interface {
	GetCount() int
	Search(key string) (kv.Data, kv.SearchResult)
	Set(key string, value []byte) (oldValue kv.Data, hasOld bool)
	Delete(key string) (oldValue kv.Data, hasOld bool)
	GetValues() (values []kv.Data)
	Swap() MemTable
}
