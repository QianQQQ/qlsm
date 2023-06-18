package memTable

import "qlsm/kv"

type Node struct {
	KV    kv.Value
	Left  *Node
	Right *Node
}
