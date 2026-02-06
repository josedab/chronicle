package chronicle

import (
	"testing"
)

func TestBTree_NewBTree(t *testing.T) {
	tree := newBTree(4)
	if tree.order != 4 {
		t.Errorf("expected order 4, got %d", tree.order)
	}

	// Minimum order should be 3
	tree = newBTree(1)
	if tree.order != 3 {
		t.Errorf("expected minimum order 3, got %d", tree.order)
	}
}

func TestBTree_Insert(t *testing.T) {
	tree := newBTree(3)

	// Insert single element
	p1 := &Partition{id: 1, startTime: 100}
	tree.Insert(100, p1)

	if tree.root == nil {
		t.Fatal("root should not be nil after insert")
	}
	if len(tree.root.keys) != 1 {
		t.Errorf("expected 1 key, got %d", len(tree.root.keys))
	}
	if tree.root.keys[0] != 100 {
		t.Errorf("expected key 100, got %d", tree.root.keys[0])
	}
}

func TestBTree_InsertMultiple(t *testing.T) {
	tree := newBTree(3)

	// Insert multiple elements
	for i := int64(1); i <= 10; i++ {
		p := &Partition{id: uint64(i), startTime: i * 100}
		tree.Insert(i*100, p)
	}

	// Should be able to find all
	result := tree.Range(0, 2000)
	if len(result) != 10 {
		t.Errorf("expected 10 results, got %d", len(result))
	}
}

func TestBTree_InsertDescending(t *testing.T) {
	tree := newBTree(3)

	// Insert in descending order
	for i := int64(10); i >= 1; i-- {
		p := &Partition{id: uint64(i), startTime: i * 100}
		tree.Insert(i*100, p)
	}

	result := tree.Range(0, 2000)
	if len(result) != 10 {
		t.Errorf("expected 10 results, got %d", len(result))
	}
}

func TestBTree_Range_Empty(t *testing.T) {
	tree := newBTree(3)

	result := tree.Range(0, 1000)
	if result != nil {
		t.Error("expected nil for empty tree")
	}
}

func TestBTree_Range_PartialMatch(t *testing.T) {
	tree := newBTree(3)

	for i := int64(1); i <= 10; i++ {
		p := &Partition{id: uint64(i), startTime: i * 100}
		tree.Insert(i*100, p)
	}

	// Range should match subset
	result := tree.Range(300, 700)
	if len(result) < 3 || len(result) > 5 {
		t.Errorf("expected 3-5 results for range 300-700, got %d", len(result))
	}
}

func TestBTree_Range_NoEndBound(t *testing.T) {
	tree := newBTree(3)

	for i := int64(1); i <= 5; i++ {
		p := &Partition{id: uint64(i), startTime: i * 100}
		tree.Insert(i*100, p)
	}

	// End = 0 means no upper bound
	result := tree.Range(200, 0)
	if len(result) < 4 {
		t.Errorf("expected at least 4 results with no upper bound, got %d", len(result))
	}
}

func TestBTree_SplitChild(t *testing.T) {
	tree := newBTree(2) // Small order to force splits

	// Insert enough to cause splits
	for i := int64(1); i <= 20; i++ {
		p := &Partition{id: uint64(i), startTime: i}
		tree.Insert(i, p)
	}

	result := tree.Range(0, 100)
	if len(result) != 20 {
		t.Errorf("expected 20 results after splits, got %d", len(result))
	}
}

func TestBTree_DuplicateKeys(t *testing.T) {
	tree := newBTree(3)

	// Insert same key multiple times
	p1 := &Partition{id: 1, startTime: 100}
	p2 := &Partition{id: 2, startTime: 100}
	tree.Insert(100, p1)
	tree.Insert(100, p2)

	result := tree.Range(0, 200)
	if len(result) != 2 {
		t.Errorf("expected 2 results for duplicate keys, got %d", len(result))
	}
}

func TestBTree_LargeDataset(t *testing.T) {
	tree := newBTree(8)

	// Insert large number of elements
	for i := int64(1); i <= 1000; i++ {
		p := &Partition{id: uint64(i), startTime: i}
		tree.Insert(i, p)
	}

	result := tree.Range(0, 2000)
	if len(result) != 1000 {
		t.Errorf("expected 1000 results, got %d", len(result))
	}

	// Range query
	result = tree.Range(500, 600)
	if len(result) < 99 || len(result) > 101 {
		t.Errorf("expected ~100 results for range 500-600, got %d", len(result))
	}
}
