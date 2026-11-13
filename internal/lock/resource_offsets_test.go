package lock

import (
 "errors"
 "testing"
)

func TestHashedResourceOffsetsStableAndDistinct(t *testing.T){
 r,err:=NewHashedResourceOffsets(1<<20,1<<16);if err!=nil{t.Fatal(err)}
 a,err:=r.Offset("owner/vol-a");if err!=nil{t.Fatal(err)}
 again,err:=r.Offset("owner/vol-a");if err!=nil{t.Fatal(err)}
 if a!=again{t.Fatalf("resource offset changed: %d != %d",a,again)}
 b,err:=r.Offset("owner/vol-b");if err!=nil{t.Fatal(err)}
 if a==b{t.Fatalf("unexpected test hash collision at %d",a)}
}

func TestHashedResourceOffsetsRejectCollision(t *testing.T){
 r,err:=NewHashedResourceOffsets(512,1);if err!=nil{t.Fatal(err)}
 if _,err=r.Offset("owner/a");err!=nil{t.Fatal(err)}
 if _,err=r.Offset("owner/b");err==nil{t.Fatal("expected collision rejection")}
}

func TestHashedResourceOffsetsRejectInvalidRange(t *testing.T){
 for _,tc:=range []struct{base,slots uint64}{{0,1},{513,1},{512,0},{^uint64(0)-511,2}}{
  if _,err:=NewHashedResourceOffsets(tc.base,tc.slots);err==nil{t.Fatalf("accepted invalid range base=%d slots=%d",tc.base,tc.slots)}
 }
 if !errors.Is(nil,nil){t.Fatal("unreachable")}
}
