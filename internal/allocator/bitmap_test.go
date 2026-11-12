package allocator

import "testing"

func TestFindFree(t *testing.T) {
	bm := []byte{0xff, 0xfb}
	got, ok := findFree(bm)
	if !ok || got != 10 {
		t.Fatalf("got %d, %v; want 10, true", got, ok)
	}
}
