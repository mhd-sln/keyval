package main

import (
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
)

type KeyValue struct {
	InMem   map[string][]string
	version int
	stores  store
	mu      sync.Mutex
}

func (kv *KeyValue) load() error {
	kv.mu.Lock()
	f, err := os.Open(kv.stores.filename())
	if err != nil {
		return err
	}
	defer f.Close()
	defer kv.mu.Unlock()

	kv.version = 1000
	err = kv.stores.decode(f, &kv.InMem)

	return err
}

func (kv *KeyValue) handler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		kv.mu.Lock()
		defer kv.mu.Unlock()
		value := kv.InMem[strings.Trim(r.URL.Path, "/")]
		fmt.Fprintf(w, "Value = %s", value)
	case "POST":
		queries := r.URL.Query()
		kv.mu.Lock()
		defer kv.mu.Unlock()
		for qKey, qValue := range queries {
			// The value is the latest value, shall we combine all the values
			currVal := kv.InMem[qKey]
			kv.InMem[qKey] = append(currVal, qValue...)
		}
		f, err := os.Create(kv.stores.filename())
		err = kv.stores.encode(f, &kv.InMem)
		if err != nil {
			fmt.Println(err)
		}
	case "PUT":
		queries := r.URL.Query()
		kv.mu.Lock()
		kv.mu.Unlock()

		for qKey, qValue := range queries {
			kv.InMem[qKey] = qValue
		}
		f, err := os.Create(kv.stores.filename())
		err = kv.stores.encode(f, &kv.InMem)
		if err != nil {
			fmt.Println(err)
		}
	case "DELETE":
		// todo
	default:
		fmt.Fprintf(w, "Sorry, only GET and POST methods are supported.")
	}

}

type gobstore struct {
}

type jsonstore struct {
}

func (gobstore) encode(w io.Writer, data interface{}) error {
	return gob.NewEncoder(w).Encode(data)
}

func (jsonstore) encode(w io.Writer, data interface{}) error {
	return json.NewEncoder(w).Encode(data)
}

func NewKeyValue() *KeyValue {
	var k KeyValue
	k.InMem = make(map[string][]string)
	return &k
}

func (jsonstore) decode(r io.Reader, data interface{}) error {
	return json.NewDecoder(r).Decode(data)
}

func (gobstore) decode(r io.Reader, data interface{}) error {
	return gob.NewDecoder(r).Decode(data)
}

func (gobstore) filename() string {
	return "keyvalue.gob"
}

func (jsonstore) filename() string {
	return "keyvalue.json"
}

type store interface {
	decode(r io.Reader, data interface{}) error
	encode(w io.Writer, data interface{}) error
	filename() string
}

type Store struct {
	RaftDir  string
	RaftBind string
	inmem    bool

	mu sync.Mutex
	m  map[string]string // The key-value store for the system.

	raft *raft.Raft // The consensus mechanism

	logger *log.Logger
}

func New(inmem bool) *Store {
	return &Store{
		m:      make(map[string]string),
		inmem:  inmem,
		logger: log.New(os.Stderr, "[store] ", log.LstdFlags),
	}
}

func (s *Store) Open(enableSingle bool, localID string) error {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID("123")

	boltDB, err := raftboltdb.NewBoltStore(filepath.Join("/Users/salmanmanekia/dev/jaywren-golang/keyval", "raft.db"))
	if err != nil {
		fmt.Errorf("new bolt store: %s", err)
	}
	logStore := boltDB
	stableStore := boltDB

	ra, err := raft.NewRaft(config, (*fsm)(s), logStore, stableStore, snapshots, transport)
	if err != nil {
		return fmt.Errorf("new raft: %s", err)
	}
	s.raft = ra
}

func main() {

	/*store := flag.String("store", "json", "json or gob")
	flag.Parse()

	kv := NewKeyValue()
	if *store == "gob" {
		kv.stores = gobstore{}
	} else {
		kv.stores = jsonstore{}
	}
	kv.load()
	fmt.Println(kv.version)
	http.HandleFunc("/", kv.handler)
	http.ListenAndServe(":8090", nil)*/
}
