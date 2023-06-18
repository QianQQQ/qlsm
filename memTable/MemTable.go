package memTable

import "qlsm/kv"

type MemTable interface {
	GetCount() int
	Search(key string) (kv.Value, kv.SearchResult)
	Set(key string, value []byte) (oldValue kv.Value, hasOld bool)
	Delete(key string) (oldValue kv.Value, hasOld bool)
	GetValues() (values []kv.Value)
}
