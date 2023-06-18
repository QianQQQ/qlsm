package memTable

// Stack 顺序栈
type Stack struct {
	stack []*Node
	base  int // 栈底索引
	top   int //  栈顶索引
}

// 简化的栈，不存在栈满的情况

// InitStack 初始化栈
func InitStack(n int) Stack {
	return Stack{make([]*Node, n), 0, 0}
}

func (st *Stack) Push(value *Node) {
	if st.top == len(st.stack) {
		st.stack = append(st.stack, value)
	} else {
		st.stack[st.top] = value
	}
	st.top++
}

func (st *Stack) Pop() (*Node, bool) {
	if st.top == st.base {
		return nil, false
	}
	st.top--
	return st.stack[st.top], true
}
