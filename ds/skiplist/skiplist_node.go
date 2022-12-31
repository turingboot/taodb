package skiplist

type SkipListNode struct {
	level    []*skipListLevel
	forward  *SkipListNode
	backward *SkipListNode
	//跳跃表节点按照score从小到大排序
	score float32
	key   string
	value interface{}
}

func (s SkipListNode) Key() string {
	return s.key
}

func (s SkipListNode) Value() interface{} {
	return s.value
}

type skipListLevel struct {
	forward *SkipListNode
	//前进指针所指向的节点和当前节点的距离
	span uint
}

func slCreateNode(level int, key string, value interface{}) *SkipListNode {
	node := &SkipListNode{
		key:   key,
		value: value,
		level: make([]*skipListLevel, level),
	}

	for i := range node.level {
		node.level[i] = new(skipListLevel)
	}
	return node
}
