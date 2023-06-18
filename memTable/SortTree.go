package memTable

import (
	"log"
	"qlsm/kv"
	"sync"
)

type Node struct {
	KV    kv.Value
	Left  *Node
	Right *Node
}

type BST struct {
	root  *Node
	count int
	sync.RWMutex
}

// GetCount 获取树中的元素数量
func (t *BST) GetCount() int {
	return t.count
}

// Search 查找 Key 的值
func (t *BST) Search(key string) (kv.Value, kv.SearchResult) {
	t.RLock()
	defer t.RUnlock()

	if t == nil {
		log.Fatal("The tree is nil")
	}

	curr := t.root
	for curr != nil {
		if key == curr.KV.Key {
			if !curr.KV.Deleted {
				return curr.KV, kv.Success
			} else {
				return kv.Value{}, kv.Deleted
			}
		}
		if key < curr.KV.Key {
			curr = curr.Left
		} else {
			curr = curr.Right
		}
	}
	return kv.Value{}, kv.None
}

// Set 设置 Key 的值并返回旧值
func (t *BST) Set(key string, value []byte) (oldValue kv.Value, hasOld bool) {
	t.Lock()
	defer t.Unlock()

	if t == nil {
		log.Fatal("The tree is nil")
	}

	curr := t.root
	newNode := &Node{
		KV: kv.Value{
			Key:   key,
			Value: value,
		},
	}

	if curr == nil {
		t.root = newNode
		t.count++
		return kv.Value{}, false
	}

	for curr != nil {
		// 如果已经存在键，则替换值
		if key == curr.KV.Key {
			oldKV := curr.KV.Copy()
			curr.KV.Value = value
			curr.KV.Deleted = false
			// 返回旧值
			if oldKV.Deleted {
				// FIXME 需要增加
				t.count++
				return kv.Value{}, false
			} else {
				return *oldKV, true
			}
		}
		if key < curr.KV.Key {
			if curr.Left == nil {
				curr.Left = newNode
				t.count++
				return kv.Value{}, false
			}
			curr = curr.Left
		} else {
			if curr.Right == nil {
				curr.Right = newNode
				t.count++
				return kv.Value{}, false
			}
			curr = curr.Right
		}
	}
	log.Fatalf("tree fail to Set value, key: %s, value: %v", key, value)
	return kv.Value{}, false
}

// Delete 删除 key 并返回旧值
func (t *BST) Delete(key string) (oldValue kv.Value, hasOld bool) {
	t.Lock()
	defer t.Unlock()

	if t == nil {
		log.Fatal("The tree is nil")
	}

	newNode := &Node{
		KV: kv.Value{
			Key:     key,
			Value:   nil,
			Deleted: true,
		},
	}

	curr := t.root
	if curr == nil {
		t.root = newNode
		return kv.Value{}, false
	}

	for curr != nil {
		if key == curr.KV.Key {
			// 存在且未被删除
			if !curr.KV.Deleted {
				oldKV := curr.KV.Copy()
				curr.KV.Value = nil
				curr.KV.Deleted = true
				// count 应该是统计当前树中存在的有效节点，但是如果删除一个不存在的key，这个count会计算错误
				// 应该要在添加删除Node的时候count增加一下来保证count数量正确
				t.count--
				return *oldKV, true
			} else { // 已被删除过
				return kv.Value{}, false
			}
		}
		// 往下一层查找
		if key < curr.KV.Key {
			// 如果不存在此 key，则插入一个删除标记
			if curr.Left == nil {
				curr.Left = newNode
				// FIXME 为什么要++
				//t.count++
			}
			// 继续对比下一层
			curr = curr.Left
		} else {
			// 如果不存在此 key，则插入一个删除标记
			if curr.Right == nil {
				curr.Right = newNode
				// FIXME 为什么要++
				//t.count++
			}
			// 继续对比下一层
			curr = curr.Right
		}
	}
	log.Fatalf("The tree fail to delete key, key: %s", key)
	return kv.Value{}, false
}

// GetValues 获取树中的所有元素，这是一个有序元素列表
func (t *BST) GetValues() (values []kv.Value) {
	t.RLock()
	defer t.RUnlock()

	// 使用栈，而非递归，栈使用了切片，可以自动扩展大小，不必担心栈满
	stack := InitStack(t.count)

	// 从小到大获取树的元素
	curr := t.root
	for {
		if curr != nil {
			stack.Push(curr)
			curr = curr.Left
		} else {
			popNode, ok := stack.Pop()
			if !ok {
				break
			}
			values = append(values, popNode.KV)
			curr = popNode.Right
		}
	}
	return values
}

func (t *BST) Swap() *BST {
	t.Lock()
	defer t.Unlock()

	newTree := &BST{}
	newTree.root = t.root
	// FIXME
	newTree.count = t.count
	t.root = nil
	t.count = 0
	return newTree
}
