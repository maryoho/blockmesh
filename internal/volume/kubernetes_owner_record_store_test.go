package volume

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

type ownerLeaseAPIServer struct {
	mu sync.Mutex
	object map[string]any
	forcePutConflict bool
	requests []string
}
func (a *ownerLeaseAPIServer) ServeHTTP(w http.ResponseWriter,r *http.Request) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.requests=append(a.requests,r.Method+" "+r.URL.Path)
	if r.Header.Get("Authorization")!="Bearer test-token" { http.Error(w,"unauthorized",http.StatusUnauthorized);return }
	switch r.Method {
	case http.MethodGet:
		if a.object==nil { http.Error(w,"not found",http.StatusNotFound);return }
		_ = json.NewEncoder(w).Encode(a.object)
	case http.MethodPost:
		if a.object!=nil { http.Error(w,"already exists",http.StatusConflict);return }
		if err:=json.NewDecoder(r.Body).Decode(&a.object);err!=nil { http.Error(w,err.Error(),400);return }
		meta:=nestedMap(a.object,"metadata");meta["resourceVersion"]="1"
		w.WriteHeader(http.StatusCreated);_ = json.NewEncoder(w).Encode(a.object)
	case http.MethodPut:
		if a.forcePutConflict { http.Error(w,"resourceVersion conflict",http.StatusConflict);return }
		var incoming map[string]any
		if err:=json.NewDecoder(r.Body).Decode(&incoming);err!=nil { http.Error(w,err.Error(),400);return }
		if a.object==nil { http.Error(w,"not found",404);return }
		oldMeta:=nestedMap(a.object,"metadata")
		newMeta:=nestedMap(incoming,"metadata")
		if oldMeta["resourceVersion"]!=newMeta["resourceVersion"] { http.Error(w,"resourceVersion conflict",409);return }
		newMeta["resourceVersion"]="2"
		a.object=incoming
		_ = json.NewEncoder(w).Encode(a.object)
	default:
		http.Error(w,"method not allowed",405)
	}
}

func newTestKubernetesOwnerRecordStore(t *testing.T, api *ownerLeaseAPIServer) (*KubernetesOwnerRecordStore,*httptest.Server) {
	t.Helper()
	server:=httptest.NewServer(api)
	store,err:=NewKubernetesOwnerRecordStore(KubernetesOwnerRecordStoreConfig{
		BaseURL:server.URL,Namespace:"blockmesh-system",BearerToken:"test-token",HTTPClient:server.Client(),
	})
	if err!=nil { server.Close();t.Fatal(err) }
	return store,server
}

func TestKubernetesOwnerRecordStoreCreateLoadAndCASUpdate(t *testing.T) {
	api:=&ownerLeaseAPIServer{}
	store,server:=newTestKubernetesOwnerRecordStore(t,api);defer server.Close()
	ctx:=context.Background()
	if _,exists,err:=store.Load(ctx,"vol-a");err!=nil||exists { t.Fatalf("initial Load=(exists %v, err %v)",exists,err) }
	first:=OwnerRecord{VolumeID:"vol-a",NodeID:"node-a",BootID:"boot-a",Epoch:2}
	if err:=store.CompareAndSwap(ctx,"vol-a",nil,first);err!=nil { t.Fatal(err) }
	got,exists,err:=store.Load(ctx,"vol-a")
	if err!=nil||!exists||got!=first { t.Fatalf("Load=(%+v,%v,%v), want %+v",got,exists,err,first) }
	second:=OwnerRecord{VolumeID:"vol-a",NodeID:"node-b",BootID:"boot-b",Epoch:3}
	if err=store.CompareAndSwap(ctx,"vol-a",&first,second);err!=nil { t.Fatal(err) }
	got,exists,err=store.Load(ctx,"vol-a")
	if err!=nil||!exists||got!=second { t.Fatalf("updated Load=(%+v,%v,%v), want %+v",got,exists,err,second) }
	api.mu.Lock()
	defer api.mu.Unlock()
	meta:=nestedMap(api.object,"metadata")
	if meta["namespace"]!="blockmesh-system" { t.Fatalf("namespace=%v",meta["namespace"]) }
	if meta["resourceVersion"]!="2" { t.Fatalf("resourceVersion=%v",meta["resourceVersion"]) }
	spec:=nestedMap(api.object,"spec")
	if spec["holderIdentity"]!="node-b/boot-b" { t.Fatalf("holderIdentity=%v",spec["holderIdentity"]) }
}

func TestKubernetesOwnerRecordStoreCASRejectsStaleAndAPIRace(t *testing.T) {
	api:=&ownerLeaseAPIServer{}
	store,server:=newTestKubernetesOwnerRecordStore(t,api);defer server.Close()
	ctx:=context.Background()
	first:=OwnerRecord{VolumeID:"vol-a",NodeID:"node-a",BootID:"boot-a",Epoch:2}
	if err:=store.CompareAndSwap(ctx,"vol-a",nil,first);err!=nil { t.Fatal(err) }
	stale:=first;stale.Epoch=1
	next:=OwnerRecord{VolumeID:"vol-a",NodeID:"node-b",BootID:"boot-b",Epoch:3}
	if err:=store.CompareAndSwap(ctx,"vol-a",&stale,next);!errors.Is(err,ErrOwnerRecordConflict) { t.Fatalf("stale CAS error=%v",err) }
	api.mu.Lock();api.forcePutConflict=true;api.mu.Unlock()
	if err:=store.CompareAndSwap(ctx,"vol-a",&first,next);!errors.Is(err,ErrOwnerRecordConflict) { t.Fatalf("API 409 error=%v",err) }
	got,exists,err:=store.Load(ctx,"vol-a")
	if err!=nil||!exists||got!=first { t.Fatalf("conflicted CAS changed record: (%+v,%v,%v)",got,exists,err) }
}

func TestKubernetesOwnerRecordStoreRejectsMalformedAndUnsafeConfig(t *testing.T) {
	if _,err:=NewKubernetesOwnerRecordStore(KubernetesOwnerRecordStoreConfig{BaseURL:"http://10.0.0.1:8080",Namespace:"ns",BearerToken:"token"});err==nil {
		t.Fatal("unencrypted non-loopback API endpoint accepted")
	}
	api:=&ownerLeaseAPIServer{object:map[string]any{
		"metadata":map[string]any{"annotations":map[string]any{
			ownerVolumeAnnotation:"other-volume",ownerNodeAnnotation:"node-a",ownerBootAnnotation:"boot-a",ownerEpochAnnotation:"2",
		}},
	}}
	store,server:=newTestKubernetesOwnerRecordStore(t,api);defer server.Close()
	if _,_,err:=store.Load(context.Background(),"vol-a");err==nil||!strings.Contains(err.Error(),"another Volume") {
		t.Fatalf("malformed identity error=%v",err)
	}
}

func TestKubernetesOwnerRecordStoreRefusesUpdateWithoutResourceVersion(t *testing.T) {
	record:=OwnerRecord{VolumeID:"vol-a",NodeID:"node-a",BootID:"boot-a",Epoch:2}
	api:=&ownerLeaseAPIServer{object:map[string]any{
		"apiVersion":"coordination.k8s.io/v1","kind":"Lease",
		"metadata":map[string]any{"name":ownerLeaseName("vol-a"),"namespace":"blockmesh-system","annotations":map[string]any{
			ownerVolumeAnnotation:record.VolumeID,ownerNodeAnnotation:record.NodeID,
			ownerBootAnnotation:record.BootID,ownerEpochAnnotation:"2",
		}},
		"spec":map[string]any{"holderIdentity":"node-a/boot-a"},
	}}
	store,server:=newTestKubernetesOwnerRecordStore(t,api);defer server.Close()
	next:=OwnerRecord{VolumeID:"vol-a",NodeID:"node-b",BootID:"boot-b",Epoch:3}
	if err:=store.CompareAndSwap(context.Background(),"vol-a",&record,next);err==nil||!strings.Contains(err.Error(),"resourceVersion") {
		t.Fatalf("missing resourceVersion error=%v",err)
	}
}

func TestKubernetesOwnerRecordStoreCreateRaceMapsConflict(t *testing.T) {
	api:=&ownerLeaseAPIServer{object:map[string]any{}}
	store,server:=newTestKubernetesOwnerRecordStore(t,api);defer server.Close()
	record:=OwnerRecord{VolumeID:"vol-a",NodeID:"node-a",BootID:"boot-a",Epoch:2}
	if err:=store.CompareAndSwap(context.Background(),"vol-a",nil,record);!errors.Is(err,ErrOwnerRecordConflict) {
		t.Fatalf("create race error=%v",err)
	}
}

func TestKubernetesOwnerRecordStoreRereadsProjectedToken(t *testing.T) {
	tokenPath:=filepath.Join(t.TempDir(),"token")
	if err:=os.WriteFile(tokenPath,[]byte("token-one"),0600);err!=nil{t.Fatal(err)}
	var mu sync.Mutex
	var seen []string
	server:=httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,r *http.Request){
		mu.Lock();seen=append(seen,r.Header.Get("Authorization"));mu.Unlock()
		http.Error(w,"not found",http.StatusNotFound)
	}))
	defer server.Close()
	store,err:=NewKubernetesOwnerRecordStore(KubernetesOwnerRecordStoreConfig{
		BaseURL:server.URL,Namespace:"blockmesh-system",TokenFile:tokenPath,HTTPClient:server.Client(),
	})
	if err!=nil{t.Fatal(err)}
	_,_,_=store.Load(context.Background(),"vol-a")
	if err=os.WriteFile(tokenPath,[]byte("token-two"),0600);err!=nil{t.Fatal(err)}
	_,_,_=store.Load(context.Background(),"vol-a")
	mu.Lock();defer mu.Unlock()
	if len(seen)!=2||seen[0]!="Bearer token-one"||seen[1]!="Bearer token-two"{
		t.Fatalf("observed Authorization headers=%v",seen)
	}
}
