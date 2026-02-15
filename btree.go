package chronicle

// BTree is a B-tree index for efficiently finding partitions by time range.
// Keys are partition start timestamps (int64 nanoseconds); values are the
// corresponding Partition pointers. The order parameter controls the maximum
// number of children per node (max keys = 2*order-1).
type BTree struct {
	order int
	root  *btreeNode
}

// btreeNode is an internal or leaf node. Leaf nodes store key/value pairs;
// internal nodes store keys as separators between child pointers.
type btreeNode struct {
	keys     []int64
	values   []*Partition
	children []*btreeNode
	leaf     bool
}

// newBTree creates a B-tree with the given order (minimum 3).
func newBTree(order int) *BTree {
	if order < 3 {
		order = 3
	}
	return &BTree{order: order}
}

// Insert adds a key/value pair to the tree. If the root is full
// (2*order-1 keys), it is split proactively before descending,
// ensuring every node visited during insertion has room for a new key.
func (t *BTree) Insert(key int64, value *Partition) {
	if t.root == nil {
		t.root = &btreeNode{leaf: true, keys: []int64{key}, values: []*Partition{value}}
		return
	}

	// Proactive root split: create a new root and split the old one so the
	// tree grows upward. This keeps the insert path free of full nodes.
	if len(t.root.keys) == 2*t.order-1 {
		oldRoot := t.root
		t.root = &btreeNode{leaf: false, children: []*btreeNode{oldRoot}}
		t.splitChild(t.root, 0)
	}

	t.insertNonFull(t.root, key, value)
}

// insertNonFull inserts key/value into a node that is guaranteed to have room.
// For leaf nodes it shifts keys right to maintain sorted order and inserts
// directly. For internal nodes it locates the correct child, splits it if
// full, and recurses.
func (t *BTree) insertNonFull(node *btreeNode, key int64, value *Partition) {
	i := len(node.keys) - 1
	if node.leaf {
		// Shift keys/values right to make room for the new entry.
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

	// Find the child subtree that should contain key.
	for i >= 0 && key < node.keys[i] {
		i--
	}
	i++

	// Split the target child preemptively if it is full.
	if len(node.children[i].keys) == 2*t.order-1 {
		t.splitChild(node, i)
		if key > node.keys[i] {
			i++
		}
	}

	t.insertNonFull(node.children[i], key, value)
}

// splitChild splits parent.children[i] into two nodes around the midpoint.
//
// For leaf nodes the split point is at index (order-1): the left half keeps
// keys[:mid] and the right half gets keys[mid:]. The first key of the new
// right node is promoted to the parent.
//
// For internal nodes the median key (index order-1) is promoted to the parent
// and removed from both children. Child pointers are divided so that the left
// node retains children[:order] and the right node gets children[order:].
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

// Range returns all partitions whose start timestamp falls within [start, end).
// An end value of 0 is treated as unbounded (returns all keys >= start).
func (t *BTree) Range(start, end int64) []*Partition {
	if t.root == nil {
		return nil
	}
	var result []*Partition
	t.root.rangeSearch(start, end, &result)
	return result
}

// rangeSearch performs an in-order traversal collecting partitions whose keys
// fall within [start, end). It visits child subtrees interleaved with key
// checks so that all matching entries are found in sorted order.
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

	// Visit the rightmost child subtree.
	if !n.leaf && i < len(n.children) {
		n.children[i].rangeSearch(start, end, out)
	}
}
