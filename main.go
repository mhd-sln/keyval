package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
)

type KeyValue struct {
	//InMem        map[string]string
	version      int
	mu           sync.Mutex
	RaftStore    *Store
	redisLikeMap map[string]func(http.ResponseWriter, *http.Request)
}

type jr struct {
	Addr string
	Id   string
}

func (kv *KeyValue) join(w http.ResponseWriter, r *http.Request) {
	jr := jr{}
	err := json.NewDecoder(r.Body).Decode(&jr)
	if err != nil {
		log.Printf("Decoding error %s", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	err = kv.RaftStore.Join(jr.Id, jr.Addr)
	if err != nil {
		log.Printf("Join error %s", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(200)
}

func (kv *KeyValue) root(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		kv.mu.Lock()
		defer kv.mu.Unlock()
		value := kv.RaftStore.m[strings.Trim(r.URL.Path, "/")]

		fmt.Fprintf(w, "Value = %s", value)
	case "POST":
		queries := r.URL.Query()
		kv.mu.Lock()
		defer kv.mu.Unlock()
		for qKey, qValue := range queries {
			// The value is the latest value, shall we combine all the values
			err := kv.RaftStore.Set(qKey, qValue[0])
			log.Print(err)
		}
	case "PUT":
		/*queries := r.URL.Query()
		kv.mu.Lock()
		kv.mu.Unlock()

		for qKey, qValue := range queries {
			kv.InMem[qKey] = qValue
		}
		f, err := os.Create(kv.stores.filename())
		err = kv.stores.encode(f, &kv.InMem)
		if err != nil {
			fmt.Println(err)
		}*/
	case "DELETE":
		// todo
	default:
		fmt.Fprintf(w, "Sorry, only GET and POST methods are supported.")
	}

}

func (kv *KeyValue) redislike(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	cmd := query.Get("cmd")
	kv.redisLikeMap[cmd](w, r)
}

func (kv *KeyValue) snapshot(w http.ResponseWriter, r *http.Request) {
	future := kv.RaftStore.raft.Snapshot()
	err := future.Error()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	meta, rc, err := future.Open()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	rc.Close()
	err = json.NewEncoder(w).Encode(meta)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

func join(joinAddr, raftAddr, nodeID string) error {
	b, err := json.Marshal(map[string]string{"addr": raftAddr, "id": nodeID})
	if err != nil {
		return err
	}
	resp, err := http.Post(fmt.Sprintf("http://%s/join", joinAddr), "application-type/json", bytes.NewReader(b))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return nil
}

func (kv *KeyValue) redisLikeGet(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	val := kv.RaftStore.m[key]
	sVal, ok := val.(string)
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	io.WriteString(w, sVal)
}

func (kv *KeyValue) redisLikeSet(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	val := r.URL.Query().Get("value")
	kv.RaftStore.Set(key, val)
	w.WriteHeader(200)
}
func (kv *KeyValue) redisLikeIncr(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	val, err := kv.RaftStore.incr(key)

	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	w.WriteHeader(200)
	io.WriteString(w, val)
}

// key=key1&value=bob&value=alice

func (kv *KeyValue) redisLikeRPush(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	key := q.Get("key")
	vals := q["value"]
	kv.RaftStore.RPush(key, vals)
	w.WriteHeader(200)
}

func NewKeyValue() *KeyValue {
	kv := &KeyValue{}
	kv.redisLikeMap = map[string]func(http.ResponseWriter, *http.Request){
		"get":   kv.redisLikeGet,
		"set":   kv.redisLikeSet,
		"incr":  kv.redisLikeIncr,
		"rpush": kv.redisLikeRPush,
		// TODO : lrange and expires
	}
	return kv
}

func main() {
	var nodeID string
	var raftAddr string
	var joinAddr string
	var webAddr string

	flag.StringVar(&nodeID, "id", "", "Node ID")
	flag.StringVar(&raftAddr, "raddr", "localhost:12000", "Set Raft bind address")
	flag.StringVar(&joinAddr, "join", "", "Set join address, if any")
	flag.StringVar(&webAddr, "webaddr", ":8090", "Web address for the app")
	flag.Parse()

	kv := NewKeyValue()

	raftDir := flag.Arg(0)
	if raftDir == "" {
		fmt.Fprintf(os.Stderr, "No Raft storage directory specified\n")
		os.Exit(1)
	}
	os.MkdirAll(raftDir, 0700)

	s := New()
	kv.RaftStore = s
	s.RaftDir = raftDir
	s.RaftBind = raftAddr

	fmt.Print(s.Open(joinAddr != "", nodeID))

	// If join was specified, make the join request.
	if joinAddr != "" {
		if err := join(joinAddr, raftAddr, nodeID); err != nil {
			log.Fatalf("failed to join node at %s: %s", joinAddr, err.Error())
		}
	}

	http.HandleFunc("/", kv.root)
	http.HandleFunc("/join", kv.join)
	http.HandleFunc("/redislike", kv.redislike)
	http.HandleFunc("/snapshot", kv.snapshot)

	err := http.ListenAndServe(webAddr, nil)
	if err != nil {
		fmt.Printf("http server error %s", err)
	}
}
