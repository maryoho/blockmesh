package format

import "testing"

func TestSuperblockRoundTrip(t *testing.T) {
	s, err := NewSuperblock(2048)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeSuperblock(s.Encode())
	if err != nil {
		t.Fatal(err)
	}
	if got.PoolSize != s.PoolSize || got.DataStartSector != 2048 || got.Generation != 1 {
		t.Fatalf("unexpected decoded superblock: %+v", got)
	}
}
func TestPageCRC(t *testing.T) {
	p, err := EncodePage(7, 123, 9, []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	id, seq, payload, err := DecodePage(p, 7)
	if err != nil {
		t.Fatal(err)
	}
	if id != 123 || seq != 9 || string(payload) != "hello" {
		t.Fatal("round trip mismatch")
	}
	p[100] ^= 1
	if _, _, _, err := DecodePage(p, 7); err == nil {
		t.Fatal("expected CRC error")
	}
}
func TestLayout(t *testing.T) {
	if err := ValidateLayout(); err != nil {
		t.Fatal(err)
	}
}
