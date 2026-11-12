package btree

import "testing"

func TestRebalanceInternalSiblingsMergeAndRedistribute(t *testing.T) {
	makeInternal := func(base uint64, count int) *cowNode {
		children := make([]cowChild, count)
		for i := range children {
			children[i] = cowChild{id: base + uint64(i), minKey: uint64(i)*10, level: 0}
		}
		return &cowNode{level: 1, children: children}
	}
	left := makeInternal(100, 60)
	right := makeInternal(200, 60)
	parent := &cowNode{level: 2, children: []cowChild{refForNode(left), refForNode(right)}}
	if err := rebalanceChild(nil, parent, 0); err != nil { t.Fatal(err) }
	if len(parent.children) != 1 { t.Fatalf("merged parent has %d children, want 1", len(parent.children)) }
	merged := parent.children[0].node
	if merged == nil || len(merged.children) != 120 || !merged.dirty {
		t.Fatalf("invalid merged internal node: %+v", merged)
	}

	left = makeInternal(300, 130)
	right = makeInternal(500, 130)
	parent = &cowNode{level: 2, children: []cowChild{refForNode(left), refForNode(right)}}
	if err := rebalanceChild(nil, parent, 0); err != nil { t.Fatal(err) }
	if len(parent.children) != 2 { t.Fatalf("redistributed parent has %d children, want 2", len(parent.children)) }
	if len(left.children) != 130 || len(right.children) != 130 {
		t.Fatalf("unexpected redistribution sizes: left=%d right=%d", len(left.children), len(right.children))
	}
}
