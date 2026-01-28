package chronicle

// BTree is a B-tree index for efficiently finding partitions by time range.
type BTree struct {
	order int
	root  *btreeNode
}

type btreeNode struct {
	keys     []int64
	values   []*Partition
	children []*btreeNode
	leaf     bool
}

func newBTree(order int) *BTree {
	if order < 3 {
		order = 3
	}
	return &BTree{order: order}
}

func (t *BTree) Insert(key int64, value *Partition) {
	if t.root == nil {
		t.root = &btreeNode{leaf: true, keys: []int64{key}, values: []*Partition{value}}
		return
	}

	if len(t.root.keys) == 2*t.order-1 {
		oldRoot := t.root
		t.root = &btreeNode{leaf: false, children: []*btreeNode{oldRoot}}
		t.splitChild(t.root, 0)
	}

	t.insertNonFull(t.root, key, value)
}

func (t *BTree) insertNonFull(node *btreeNode, key int64, value *Partition) {
	i := len(node.keys) - 1
	if node.leaf {
		node.keys = append(node.keys, 0)
		node.values = append(node.values, nil)
		for i >= 0 && key < node.keys[i] {
			node.keys[i+1] = node.keys[i]
			node.values[i+1] = node.values[i]
			i--
		}
		node.keys[i+1] = key
		node.values[i+1] = value
		return
	}

	for i >= 0 && key < node.keys[i] {
		i--
	}
	i++

	if len(node.children[i].keys) == 2*t.order-1 {
		t.splitChild(node, i)
		if key > node.keys[i] {
			i++
		}
	}

	t.insertNonFull(node.children[i], key, value)
}

func (t *BTree) splitChild(parent *btreeNode, i int) {
	order := t.order
	child := parent.children[i]
	newChild := &btreeNode{leaf: child.leaf}

	if child.leaf {
		mid := order - 1
		newChild.keys = append(newChild.keys, child.keys[mid:]...)
		newChild.values = append(newChild.values, child.values[mid:]...)
		child.keys = child.keys[:mid]
		child.values = child.values[:mid]

		parent.keys = append(parent.keys, 0)
		copy(parent.keys[i+1:], parent.keys[i:])
		parent.keys[i] = newChild.keys[0]
	} else {
		midKey := child.keys[order-1]
		newChild.keys = append(newChild.keys, child.keys[order:]...)
		child.keys = child.keys[:order-1]
		newChild.children = append(newChild.children, child.children[order:]...)
		child.children = child.children[:order]

		parent.keys = append(parent.keys, 0)
		copy(parent.keys[i+1:], parent.keys[i:])
		parent.keys[i] = midKey
	}

	parent.children = append(parent.children, nil)
	copy(parent.children[i+2:], parent.children[i+1:])
	parent.children[i+1] = newChild
}

func (t *BTree) Range(start, end int64) []*Partition {
	if t.root == nil {
		return nil
	}
	var result []*Partition
	t.root.rangeSearch(start, end, &result)
	return result
}

func (n *btreeNode) rangeSearch(start, end int64, out *[]*Partition) {
	if n == nil {
		return
	}

	i := 0
	for i < len(n.keys) {
		if !n.leaf {
			n.children[i].rangeSearch(start, end, out)
		}
		key := n.keys[i]
		if (end == 0 || key < end) && key >= start {
			if n.leaf && i < len(n.values) {
				*out = append(*out, n.values[i])
			}
		}
		i++
	}

	if !n.leaf && i < len(n.children) {
		n.children[i].rangeSearch(start, end, out)
	}
}
