package volume

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

var (
	ErrKubernetesOwnerRecordNotFound = errors.New("Kubernetes OwnerRecord Lease not found")
	ErrKubernetesOwnerRecordConflict = errors.New("Kubernetes OwnerRecord Lease resourceVersion conflict")
)

const (
	ownerVolumeAnnotation = "blockmesh.io/owner-volume-id"
	ownerNodeAnnotation   = "blockmesh.io/owner-node-id"
	ownerBootAnnotation   = "blockmesh.io/owner-boot-id"
	ownerEpochAnnotation  = "blockmesh.io/owner-epoch"
)

// KubernetesOwnerRecordStore stores the last Owner identity in a coordination.k8s.io/v1
// Lease. The Lease is a durable CAS record, NOT a distributed lock: SAN ownership
// remains exclusively governed by sanlock plus SCSI PR fencing.
//
// The API client uses GET + resourceVersion-guarded PUT for updates and POST for
// initial creation. Kubernetes API 409 responses map to ErrOwnerRecordConflict.
type KubernetesOwnerRecordStore struct {
	baseURL   string
	namespace string
	token     string
	tokenFile string
	client    *http.Client
}

type KubernetesOwnerRecordStoreConfig struct {
	BaseURL    string
	Namespace  string
	BearerToken string
	TokenFile  string
	HTTPClient *http.Client
}

func NewKubernetesOwnerRecordStore(cfg KubernetesOwnerRecordStoreConfig) (*KubernetesOwnerRecordStore, error) {
	u, err := url.Parse(strings.TrimRight(cfg.BaseURL, "/"))
	if err != nil || u.Scheme == "" || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") {
		return nil, errors.New("Kubernetes API BaseURL must be an absolute http(s) URL")
	}
	if u.Scheme != "https" {
		host, _, splitErr := net.SplitHostPort(u.Host)
		if splitErr != nil { host = u.Hostname() }
		ip := net.ParseIP(host)
		if host != "localhost" && (ip == nil || !ip.IsLoopback()) {
			return nil, errors.New("unencrypted Kubernetes API is allowed only on loopback for tests")
		}
	}
	if strings.TrimSpace(cfg.Namespace) == "" {
		return nil, errors.New("Kubernetes namespace is required")
	}
	if cfg.BearerToken == "" && cfg.TokenFile == "" {
		return nil, errors.New("Kubernetes bearer token or token file is required")
	}
	if cfg.BearerToken != "" && cfg.TokenFile != "" {
		return nil, errors.New("configure either BearerToken or TokenFile, not both")
	}
	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	return &KubernetesOwnerRecordStore{
		baseURL: strings.TrimRight(cfg.BaseURL, "/"),
		namespace: cfg.Namespace,
		token: cfg.BearerToken,
		tokenFile: cfg.TokenFile,
		client: client,
	}, nil
}

// NewInClusterKubernetesOwnerRecordStore builds a TLS-verified client from the
// standard projected ServiceAccount files. The token file is read for each
// request so projected-token rotation is honored.
func NewInClusterKubernetesOwnerRecordStore() (*KubernetesOwnerRecordStore, error) {
	host := os.Getenv("KUBERNETES_SERVICE_HOST")
	port := os.Getenv("KUBERNETES_SERVICE_PORT")
	if host == "" || port == "" { return nil, errors.New("KUBERNETES_SERVICE_HOST/PORT are not set") }
	namespaceBytes, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
	if err != nil { return nil, fmt.Errorf("read ServiceAccount namespace: %w", err) }
	tokenPath := "/var/run/secrets/kubernetes.io/serviceaccount/token"
	caBytes, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
	if err != nil { return nil, fmt.Errorf("read ServiceAccount CA: %w", err) }
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caBytes) { return nil, errors.New("ServiceAccount CA contains no valid certificates") }
	baseTransport, ok := http.DefaultTransport.(*http.Transport)
	if !ok { return nil, errors.New("default HTTP transport is not an *http.Transport") }
	transport := baseTransport.Clone()
	transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}
	return NewKubernetesOwnerRecordStore(KubernetesOwnerRecordStoreConfig{
		BaseURL: "https://" + net.JoinHostPort(host, port),
		Namespace: strings.TrimSpace(string(namespaceBytes)),
		TokenFile: tokenPath,
		HTTPClient: &http.Client{Transport: transport, Timeout: 10*time.Second},
	})
}

func ownerLeaseName(volumeID string) string {
	sum := sha256.Sum256([]byte(volumeID))
	return "blockmesh-owner-" + hex.EncodeToString(sum[:])[:32]
}

func (s *KubernetesOwnerRecordStore) collectionURL() string {
	return s.baseURL + "/apis/coordination.k8s.io/v1/namespaces/" + url.PathEscape(s.namespace) + "/leases"
}
func (s *KubernetesOwnerRecordStore) objectURL(volumeID string) string {
	return s.collectionURL() + "/" + ownerLeaseName(volumeID)
}

func (s *KubernetesOwnerRecordStore) request(ctx context.Context, method, target string, body any) (map[string]any, int, error) {
	var payload io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil { return nil, 0, err }
		payload = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, target, payload)
	if err != nil { return nil, 0, err }
	token := s.token
	if s.tokenFile != "" {
		raw, readErr := os.ReadFile(s.tokenFile)
		if readErr != nil { return nil, 0, fmt.Errorf("read Kubernetes ServiceAccount token: %w", readErr) }
		token = strings.TrimSpace(string(raw))
	}
	if token == "" { return nil, 0, errors.New("Kubernetes bearer token is empty") }
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")
	if body != nil { req.Header.Set("Content-Type", "application/json") }
	resp, err := s.client.Do(req)
	if err != nil { return nil, 0, err }
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil { return nil, resp.StatusCode, err }
	var obj map[string]any
	if len(raw) != 0 { _ = json.Unmarshal(raw, &obj) }
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if resp.StatusCode == http.StatusConflict { return obj, resp.StatusCode, ErrKubernetesOwnerRecordConflict }
		if resp.StatusCode == http.StatusNotFound { return obj, resp.StatusCode, ErrKubernetesOwnerRecordNotFound }
		message := strings.TrimSpace(string(raw))
		if message == "" { message = resp.Status }
		return obj, resp.StatusCode, fmt.Errorf("Kubernetes API %s %s: %s", method, target, message)
	}
	return obj, resp.StatusCode, nil
}

func nestedMap(obj map[string]any, key string) map[string]any {
	m, _ := obj[key].(map[string]any)
	if m == nil { m = make(map[string]any); obj[key] = m }
	return m
}
func annotations(obj map[string]any) map[string]any { return nestedMap(nestedMap(obj,"metadata"),"annotations") }

func decodeKubernetesOwnerRecord(obj map[string]any, volumeID string) (OwnerRecord, error) {
	meta, _ := obj["metadata"].(map[string]any)
	ann, _ := meta["annotations"].(map[string]any)
	get := func(key string) string { v, _ := ann[key].(string); return v }
	epochText := get(ownerEpochAnnotation)
	epoch, err := strconv.ParseUint(epochText,10,64)
	if err != nil || epoch == 0 { return OwnerRecord{}, errors.New("Kubernetes OwnerRecord has invalid owner epoch annotation") }
	record := OwnerRecord{VolumeID:get(ownerVolumeAnnotation),NodeID:get(ownerNodeAnnotation),BootID:get(ownerBootAnnotation),Epoch:epoch}
	if record.VolumeID != volumeID || record.NodeID == "" || record.BootID == "" {
		return OwnerRecord{}, errors.New("Kubernetes OwnerRecord Lease annotations are incomplete or identify another Volume")
	}
	return record,nil
}

func (s *KubernetesOwnerRecordStore) Load(ctx context.Context, volumeID string) (OwnerRecord, bool, error) {
	if strings.TrimSpace(volumeID)=="" { return OwnerRecord{},false,errors.New("volume ID is required") }
	obj,_,err:=s.request(ctx,http.MethodGet,s.objectURL(volumeID),nil)
	if errors.Is(err,ErrKubernetesOwnerRecordNotFound) { return OwnerRecord{},false,nil }
	if err!=nil { return OwnerRecord{},false,fmt.Errorf("get Kubernetes OwnerRecord Lease: %w",err) }
	record,err:=decodeKubernetesOwnerRecord(obj,volumeID)
	if err!=nil { return OwnerRecord{},false,err }
	return record,true,nil
}

func writeOwnerRecord(obj map[string]any, record OwnerRecord) {
	meta:=nestedMap(obj,"metadata")
	meta["name"]=ownerLeaseName(record.VolumeID)
	meta["namespace"]=meta["namespace"]
	ann:=annotations(obj)
	ann[ownerVolumeAnnotation]=record.VolumeID
	ann[ownerNodeAnnotation]=record.NodeID
	ann[ownerBootAnnotation]=record.BootID
	ann[ownerEpochAnnotation]=strconv.FormatUint(record.Epoch,10)
	spec:=nestedMap(obj,"spec")
	holder:=record.NodeID+"/"+record.BootID
	spec["holderIdentity"]=holder
}

func (s *KubernetesOwnerRecordStore) CompareAndSwap(ctx context.Context, volumeID string, expected *OwnerRecord, next OwnerRecord) error {
	if strings.TrimSpace(volumeID)=="" || next.VolumeID!=volumeID || strings.TrimSpace(next.NodeID)=="" ||
		strings.TrimSpace(next.BootID)=="" || next.Epoch==0 {
		return errors.New("invalid Kubernetes OwnerRecord CAS arguments")
	}
	if expected!=nil && expected.VolumeID!=volumeID { return errors.New("expected OwnerRecord VolumeID mismatch") }
	if expected!=nil {
		obj,_,err:=s.request(ctx,http.MethodGet,s.objectURL(volumeID),nil)
		if err!=nil { if errors.Is(err,ErrKubernetesOwnerRecordNotFound){return ErrOwnerRecordConflict};return fmt.Errorf("read OwnerRecord before CAS: %w",err) }
		current,decodeErr:=decodeKubernetesOwnerRecord(obj,volumeID)
		if decodeErr!=nil { return decodeErr }
		if current!=*expected { return ErrOwnerRecordConflict }
		meta,_:=obj["metadata"].(map[string]any)
		if meta==nil || strings.TrimSpace(fmt.Sprint(meta["resourceVersion"]))=="" || meta["resourceVersion"]==nil {
			return errors.New("Kubernetes OwnerRecord Lease has no resourceVersion; refusing non-CAS update")
		}
		writeOwnerRecord(obj,next)
		_,_,err=s.request(ctx,http.MethodPut,s.objectURL(volumeID),obj)
		if errors.Is(err,ErrKubernetesOwnerRecordConflict) { return ErrOwnerRecordConflict }
		if err!=nil { return fmt.Errorf("resourceVersion-guarded OwnerRecord update: %w",err) }
		return nil
	}
	obj:=map[string]any{
		"apiVersion":"coordination.k8s.io/v1",
		"kind":"Lease",
		"metadata":map[string]any{
			"name":ownerLeaseName(volumeID),
			"namespace":s.namespace,
			"labels":map[string]any{"app.kubernetes.io/managed-by":"blockmesh"},
			"annotations":map[string]any{},
		},
		"spec":map[string]any{},
	}
	writeOwnerRecord(obj,next)
	nestedMap(obj,"metadata")["namespace"]=s.namespace
	_,_,err:=s.request(ctx,http.MethodPost,s.collectionURL(),obj)
	if errors.Is(err,ErrKubernetesOwnerRecordConflict) { return ErrOwnerRecordConflict }
	if err!=nil { return fmt.Errorf("create initial Kubernetes OwnerRecord Lease: %w",err) }
	return nil
}

var _ OwnerRecordStore = (*KubernetesOwnerRecordStore)(nil)
