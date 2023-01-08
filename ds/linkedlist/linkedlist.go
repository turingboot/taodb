package linkedlist

type ListNode struct {
	prev  *ListNode
	next  *ListNode
	value any
}

func (l *ListNode) Value() any {
	return l.value
}

type LinkedList struct {
	header *ListNode
	tail   *ListNode
	length uint64
}

func New() *LinkedList {
	//不带任何节点的空链
	return &LinkedList{}
}

func PushFront(v any) {

}

func PushBack() {

}

func Remove() {

}

// 在at节点后插入目标节点e
func (l *LinkedList) insert(e, at *ListNode) *ListNode {
	e.prev = at
	e.next = at.next
	e.prev.next = e
	e.next.prev = e
	l.length++
	return e
}

func (l *LinkedList) insertWithValue(value any, at *ListNode) *ListNode {

	return l.insert(&ListNode{value: value}, at)
}

func (l *LinkedList) Header() *ListNode {
	return l.header
}

func (l *LinkedList) Tail() *ListNode {
	return l.tail
}

func (l *LinkedList) Length() uint64 {
	return l.length
}
