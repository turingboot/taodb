package skiplist

import (
	"bytes"
	"errors"
	"math/rand"
)

const (
	SKIPLIST_MAXLEVEL = 16
	SKIPLIST_P        = 1 / 4.0
)

type SkipList struct {
	header *SkipListNode
	tail   *SkipListNode
	level  int
	//不计表头节点
	length int64
}

func Create() *SkipList {
	l := &SkipList{
		length: 0,
		level:  1,
		header: createNode(SKIPLIST_MAXLEVEL, "", nil),
		tail:   nil,
	}

	//初始化表头结点
	//for _, v := range l.header.level {
	//	v.forward = nil
	//	v.span = 0
	//}
	return l
}

func randomLevel() int {
	level := 1
	for rand.Float64() < SKIPLIST_P && level < SKIPLIST_MAXLEVEL {
		level += 1
	}
	return level
}

func (s *SkipList) Insert(key string, value interface{}) *SkipListNode {

	updates := make([]*SkipListNode, SKIPLIST_MAXLEVEL)
	rank := make([]uint, SKIPLIST_MAXLEVEL)

	x := s.header
	//在各层查找节点插入的位置
	for i := s.level - 1; i >= 0; i-- {
		// 如果 i 不是 zsl->level-1 层
		// 那么 i 层的起始 rank 值为 i+1 层的 rank 值
		// 各个层的 rank 值一层层累积
		// 最终 rank[0] 的值加一就是新节点的前置节点的排位
		// rank[0] 会在后面成为计算 span 值和 rank 值的基础
		if i == s.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}
		// 沿着前进指针遍历跳跃表
		if x.level[i] != nil {
			for x.level[i].forward != nil &&
				bytes.Compare([]byte(x.level[i].forward.key), []byte(key)) < 0 {
				// 记录沿途跨越了多少个节点
				rank[i] += x.level[i].span
				// 移动至下一指针
				x = x.level[i].forward
			}
		}
		// 记录将要和新节点相连接的节点
		updates[i] = x
	}

	//开始插入
	//获取随机层数作为新的索引层数
	newLevel := randomLevel()
	// 如果新节点的层数比表中其他节点的层数都要大
	// 那么初始化表头节点中未使用的层，并将它们记录到 update 数组中
	// 将来也指向新节点
	if newLevel > s.level {
		// 初始化未使用层
		// T = O(1)
		for i := s.level; i < s.level; i++ {
			rank[i] = 0
			updates[i] = s.header
			updates[i].level[i].span = uint(s.length)
		}
		s.level = newLevel
	}
	// 创建新节点
	x = createNode(newLevel, key, value)
	// 将前面记录的指针指向新节点，并做相应的设置
	for i := 0; i < s.level; i++ {
		// 设置新节点的forward指针
		x.level[i].forward = updates[i].level[i].forward
		// 将中途记录的各个节点的forward指针指向新节点
		updates[i].level[i].forward = x

		// 计算新节点跨越的节点数量
		x.level[i].span = updates[i].level[i].span - (rank[0] - rank[i])

		// 更新新节点插入之后，沿途节点的 span 值
		// 其中的 +1 计算的是新节点
		updates[i].level[i].span = (rank[0] - rank[i]) + 1
	}

	// 未接触的节点的 span 值也需要增一，这些节点直接从表头指向新节点
	for i := 0; i < s.level; i++ {
		updates[i].level[i].span++
	}

	////设置新节点的后退指针
	//if updates[0] == s.header {
	//	x.backward = nil
	//} else {
	//	x.backward = updates[0]
	//}

	if x.level[0].forward != nil {
		//x.level[0].forward.backward = x
		s.tail = x
	}
	//} else {
	//	s.tail = x
	//}

	s.length++
	return x
}

func (s *SkipList) deleteNode(x *SkipListNode, updates []*SkipListNode) {
	for i := 0; i < s.level; i++ {
		if updates[i].level[i].forward == x {
			updates[i].level[i].span += x.level[i].span - 1
			updates[i].level[i].forward = x.level[i].forward
		} else {
			updates[i].level[i].span--
		}
	}

	for s.level > 1 && s.header.level[s.level-1].forward == nil {
		s.level--
	}

	s.length--
}

func (s *SkipList) Delete(key string) {
	update := make([]*SkipListNode, SKIPLIST_MAXLEVEL)

	x := s.header
	for i := s.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil &&
			bytes.Compare([]byte(x.level[i].forward.key), []byte(key)) < 0 {
			x = x.level[i].forward
		}
		update[i] = x
	}

	x = x.level[0].forward
	if x != nil && x.key == key {
		s.deleteNode(x, update)
		return
	}

	update = make([]*SkipListNode, SKIPLIST_MAXLEVEL)

	x = s.header
	for i := s.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil &&
			bytes.Compare([]byte(x.level[i].forward.key), []byte(key)) < 0 {
			x = x.level[i].forward
		}
		update[i] = x
	}

	x = x.level[0].forward
	if x != nil && x.key == key {
		s.deleteNode(x, update)
		return
	}
}

func (s *SkipList) Get(key string) (*SkipListNode, error) {
	if key == "" {
		return nil, errors.New("key is nil")
	}
	x := s.header
	for i := s.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil &&
			bytes.Compare([]byte(x.level[i].forward.key), []byte(key)) <= 0 {
			x = x.level[i].forward
		}

		if x.key == key {
			return x, nil
		}
	}

	return nil, errors.New("key is not is exist")
}
