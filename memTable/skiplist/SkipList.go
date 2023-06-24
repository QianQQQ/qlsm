package skiplist

import (
	"math/rand"
	"qlsm/kv"
	"qlsm/memTable"
	"sync"
)

const maxLevel = 32
const pFactor = 0.25

type Node struct {
	KV      kv.Data
	forward []*Node
}

type SL struct {
	head  *Node
	level int
	count int
	sync.RWMutex
}

var _ memTable.MemTable = (*SL)(nil)

func New() *SL {
	sl := SL{}
	sl.head = &Node{
		KV:      kv.Data{Key: "", Value: nil, Deleted: true},
		forward: make([]*Node, maxLevel),
	}
	return &sl
}

func (sl *SL) randomLevel() int {
	lv := 1
	for lv < maxLevel && rand.Float64() < pFactor {
		lv++
	}
	return lv
}

// GetCount 获取树中的元素数量
func (sl *SL) GetCount() int {
	return sl.count
}

// Search 查找 Key 的值
func (sl *SL) Search(key string) (kv.Data, kv.SearchResult) {
	sl.RLock()
	defer sl.RUnlock()
	curr := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for curr.forward[i] != nil && curr.forward[i].KV.Key < key {
			curr = curr.forward[i]
		}
	}
	curr = curr.forward[0]
	if curr != nil && curr.KV.Key == key {
		if curr.KV.Deleted {
			return kv.Data{}, kv.Deleted
		}
		return curr.KV, kv.Success
	}
	return kv.Data{}, kv.None
}

// Set 设置 Key 的值并返回旧值
func (sl *SL) Set(key string, value []byte) (oldValue kv.Data, hasOld bool) {
	sl.Lock()
	defer sl.Unlock()
	update := make([]*Node, maxLevel)
	for i := range update {
		update[i] = sl.head
	}
	lv := sl.randomLevel()
	sl.level = max(sl.level, lv)
	newNode := &Node{
		KV:      kv.Data{Key: key, Value: value, Deleted: false},
		forward: make([]*Node, lv),
	}
	curr := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for curr.forward[i] != nil && curr.forward[i].KV.Key < key {
			curr = curr.forward[i]
		}
		update[i] = curr
	}
	flag := true
	curr = curr.forward[0]
	if curr == nil || curr.KV.Key != key {
		sl.count++
		flag = false
	}
	for i, node := range update[:lv] {
		newNode.forward[i] = node.forward[i]
		node.forward[i] = newNode
	}
	if !flag {
		return kv.Data{}, false
	}
	return *curr.KV.Copy(), true
}

// Delete 删除 key 并返回旧值
func (sl *SL) Delete(key string) (oldValue kv.Data, hasOld bool) {
	sl.Lock()
	defer sl.Unlock()
	update := make([]*Node, maxLevel)
	for i := range update {
		update[i] = sl.head
	}
	curr := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for curr.forward[i] != nil && curr.forward[i].KV.Key < key {
			curr = curr.forward[i]
		}
		update[i] = curr
	}
	curr = curr.forward[0]
	// 如果有这个节点
	if curr != nil && curr.KV.Key == key {
		if curr.KV.Deleted {
			return kv.Data{}, false
		} else {
			curr.KV.Deleted = true
			return *curr.KV.Copy(), true
		}
	}
	// 没有这节点的话就要插入
	lv := sl.randomLevel()
	sl.level = max(sl.level, lv)
	newNode := &Node{
		KV:      kv.Data{Key: key, Value: nil, Deleted: true},
		forward: make([]*Node, lv),
	}
	for i, node := range update[:lv] {
		newNode.forward[i] = node.forward[i]
		node.forward[i] = newNode
	}
	return kv.Data{}, false
}

// GetValues 获取树中的所有元素
func (sl *SL) GetValues() (values []kv.Data) {
	sl.RLock()
	defer sl.RUnlock()
	curr := sl.head.forward[0]
	for curr != nil {
		values = append(values, curr.KV)
		curr = curr.forward[0]
	}
	return values
}

func (sl *SL) Swap() memTable.MemTable {
	sl.Lock()
	defer sl.Unlock()
	// 生成tmpSL
	tmpSL := &SL{}
	tmpSL.count = sl.count
	tmpSL.level = sl.level
	tmpSL.head = sl.head

	// 将 sl 初始化
	sl.count = 0
	sl.level = 0
	sl.head = &Node{
		KV:      kv.Data{Key: "", Value: nil, Deleted: true},
		forward: make([]*Node, maxLevel),
	}
	return tmpSL
}

func max(i, j int) int {
	if i > j {
		return i
	}
	return j
}
