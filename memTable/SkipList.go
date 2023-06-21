package memTable

import (
	"fmt"
	"math/rand"
	"qlsm/kv"
	"sync"
)

const maxLevel = 32
const pFactor = 0.25

type SLNode struct {
	KV      kv.Data
	forward []*SLNode
}

type SL struct {
	head  *SLNode
	level int
	count int
	sync.RWMutex
}

var _ MemTable = (*SL)(nil)

func NewSL() *SL {
	sl := SL{}
	sl.head = &SLNode{
		KV:      kv.Data{Key: "", Value: nil, Deleted: true},
		forward: make([]*SLNode, maxLevel),
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
	update := make([]*SLNode, maxLevel)
	for i := range update {
		update[i] = sl.head
	}
	lv := sl.randomLevel()
	sl.level = max(sl.level, lv)
	newNode := &SLNode{
		KV:      kv.Data{Key: key, Value: value, Deleted: false},
		forward: make([]*SLNode, lv),
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
	update := make([]*SLNode, maxLevel)
	curr := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for curr.forward[i] != nil && curr.forward[i].KV.Key < key {
			curr = curr.forward[i]
		}
		update[i] = curr
	}
	curr = curr.forward[0]
	if curr == nil || curr.KV.Key != key || curr.KV.Deleted {
		return kv.Data{}, false
	}
	f := make([]*SLNode, len(curr.forward))
	copy(f, curr.forward)
	newNode := &SLNode{
		KV:      kv.Data{Key: key, Value: nil, Deleted: true},
		forward: f,
	}
	for i := 0; i < sl.level && update[i].forward[i] == curr; i++ {
		newNode.forward[i] = update[i].forward[i]
		update[i].forward[i] = newNode
	}
	return *curr.KV.Copy(), true
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

func (sl *SL) Swap() MemTable {
	//sl.Lock()
	//defer sl.Unlock()
	//newSL := NewSL()
	//newSL.head = sl.head
	//newSL.count = sl.count
	//sl.head = nil
	//sl.count = 0
	sl.Lock()
	defer sl.Unlock()
	tmpSL := &SL{}
	tmpSL.level = sl.level
	tmpSL.count = sl.count
	tmpSL.head = sl.head
	sl.count = 0
	sl.level = 0
	sl.head = &SLNode{
		KV:      kv.Data{Key: "", Value: nil, Deleted: true},
		forward: make([]*SLNode, maxLevel),
	}
	return tmpSL
}

func (sl *SL) Show() {
	curr := sl.head
	for curr != nil {
		fmt.Println(curr.KV.Key)
		curr = curr.forward[0]
	}
}

func max(i, j int) int {
	if i > j {
		return i
	}
	return j
}
