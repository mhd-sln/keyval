package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

type Store struct {
	RaftDir  string
	RaftBind string

	mu sync.Mutex
	m  map[string]interface{} // The key-value store for the system.

	expires map[string]time.Time
	raft    *raft.Raft // The consensus mechanism

	logger *log.Logger
}

type fsm Store

var _ raft.FSM = &fsm{}

const (
	retainSnapshotCount = 2
	raftTimeout         = 10 * time.Second
)

type command struct {
	Op      string   `json:"op,omitempty"`
	Key     string   `json:"key,omitempty"`
	Value   string   `json:"value,omitempty"`
	Params  []string `json:"params,omitempty"`
	Start   int      `json:"start,omitempty"`
	End     int      `json:"end,omitempty"`
	Seconds int      `json:"seconds,omitempty"`
}

type fsmSnapshot struct {
	store map[string]interface{}
}

var _ raft.FSMSnapshot = &fsmSnapshot{}

type ticker struct {
	C chan time.Time
}

var tick *ticker = nil
var last time.Time

func (f *fsm) Apply(l *raft.Log) interface{} {
	var c command
	if err := json.Unmarshal(l.Data, &c); err != nil {
		panic(fmt.Sprintf("failed to unmarshal command: %s", err.Error()))
	}
	switch c.Op {
	case "set":
		return f.applySet(c.Key, c.Value)
	case "delete":
		return f.applyDelete(c.Key)
	case "incr":
		return f.applyIncr(c.Key)
	case "rpush":
		return f.applyRPush(c.Key, c.Params)
	case "lrange":
		return f.applyLRange(c.Key, c.Start, c.End)
	case "expire":
		return f.applyExpire(c.Key, c.Seconds)
	default:
		panic(fmt.Sprintf("unrecognized command op: %s", c.Op))
	}
}

// Snapshot returns a snapshot of the key-value store.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	log.Println("snapshot is called")
	// Clone the map.
	// TODO: do we need to deep copy
	o := make(map[string]interface{})
	for k, v := range f.m {
		o[k] = v
	}
	return &fsmSnapshot{store: o}, nil
}

// Restore stores the key-value store to a previous state.
func (f *fsm) Restore(rc io.ReadCloser) error {
	log.Println("restore is called")
	o := make(map[string]interface{})
	if err := json.NewDecoder(rc).Decode(&o); err != nil {
		return err
	}

	// Set the state from the snapshot, no lock required according to
	// Hashicorp docs.
	f.m = o
	log.Printf("in the restore %v", f.m)
	return nil
}

func (f *fsm) applyIncr(key string) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	i, _ := strconv.Atoi(f.m[key].(string))
	f.m[key] = strconv.Itoa(i + 1)
	log.Printf("applyincr key %s and value %s", key, f.m[key])
	return f.m[key]
}

func (f *fsm) applySet(key, value string) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	log.Printf("applyset key %s and value %s", key, value)
	f.m[key] = value
	return nil
}

func (f *fsm) applyDelete(key string) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.m, key)
	return nil
}

// cmd="rpush&key=&value=""
// "":
func (f *fsm) applyRPush(key string, val []string) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(val) == 0 {
		return fmt.Errorf("(error) ERR wrong number of arguments for 'rpush' command")
	}

	if _, ok := f.m[key]; !ok {
		f.m[key] = []string{}
	}

	v, ok := f.m[key].([]string)
	if !ok {
		return fmt.Errorf("(error) WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	v = append(v, val...)
	f.m[key] = v
	return len(v)
}

func (f *fsm) applyLRange(key string, start int, end int) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.m[key].([]string)
	if !ok {
		return fmt.Errorf("(error) WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	if start < 0 {
		start = 0
	}
	if end > len(v) {
		end = len(v) - 1
	}
	if start > len(v) {
		start = len(v)
	}
	return v[start : end+1]
}

func (f *fsm) applyExpire(key string, seconds int) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.m[key]
	if !ok {
		return 0
	}
	f.expires[key] = Now().Add(time.Second * time.Duration(seconds))
	log.Printf("key would expire at %s ", f.expires[key])
	return 1
}

func (s *Store) expireTick(next time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, v := range s.expires {
		log.Printf("expire %s %v", v, next)
		if v.Sub(next)/time.Second == 0 {
			delete(s.expires, k)
			delete(s.m, k)
		}
	}
}

func (s *Store) expireLoop() {
	c := Tick(1 * time.Second)
	for next := range c {
		s.expireTick(next)
	}
}

func Tick(t time.Duration) <-chan time.Time {
	if last.IsZero() {
		return time.Tick(t)
	}
	tick = &ticker{
		C: make(chan time.Time),
	}
	return tick.C
}

func Now() time.Time {
	if last.IsZero() {
		return time.Now()
	}
	return last
}

// move to a time file
func Sleep(d time.Duration) {
	log.Printf("Sleep with duration %s", d)
	defer func() { log.Printf("Sleep returned %s", Now()) }()

	if last.IsZero() {
		time.Sleep(d)
		return
	}

	last = last.Add(d / 2)

	tick.C <- last

	last = last.Add(d / 2)

	tick.C <- last
}

func New() *Store {
	s := &Store{
		m:       make(map[string]interface{}),
		expires: make(map[string]time.Time),
		logger:  log.New(os.Stderr, "[store] ", log.LstdFlags),
	}

	go s.expireLoop()
	return s
}

func (s *Store) jsonApply(c command) (raft.ApplyFuture, error) {
	b, err := json.Marshal(c)
	if err != nil {
		return nil, err
	}

	f := s.raft.Apply(b, raftTimeout)
	return f, f.Error()
}

func (s *Store) Set(key, value string) error {
	if s.raft.State() != raft.Leader {
		return fmt.Errorf("not leader")
	}
	c := &command{
		Op:    "set",
		Key:   key,
		Value: value,
	}
	_, err := s.jsonApply(*c)
	return err
}

func (s *Store) Incr(key string) (string, error) {
	if s.raft.State() != raft.Leader {
		return "", fmt.Errorf("not leader")
	}
	val, ok := s.m[key].(string)
	if !ok {
		return "", fmt.Errorf("(error) WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	_, err := strconv.Atoi(val)
	if err != nil {
		return "", err
	}
	c := command{
		Op:  "incr",
		Key: key,
	}
	f, err := s.jsonApply(c)
	return f.Response().(string), err
}

func (s *Store) RPush(key string, val []string) (int, error) {
	if s.raft.State() != raft.Leader {
		return -1, fmt.Errorf("not leader")
	}
	c := command{
		Op:     "rpush",
		Key:    key,
		Params: val,
	}
	f, err := s.jsonApply(c)
	return f.Response().(int), err

}

func (s *Store) LRange(key string, start int, end int) ([]string, error) {
	if s.raft.State() != raft.Leader {
		return nil, fmt.Errorf("not leader")
	}
	c := command{
		Op:    "lrange",
		Key:   key,
		Start: start,
		End:   end,
	}
	f, err := s.jsonApply(c)
	switch v := f.Response().(type) {
	case []string:
		return v, err
	case error:
		return nil, v
	default:
		return nil, errors.New("unhandled type")
	}
}

func (s *Store) Expire(key string, seconds int) (int, error) {
	if s.raft.State() != raft.Leader {
		return -1, fmt.Errorf("not leader")
	}
	c := command{
		Op:      "expire",
		Key:     key,
		Seconds: seconds,
	}
	f, err := s.jsonApply(c)
	return f.Response().(int), err
}

/*
func (s *Store) Get(key string) (string, error) {
	if s.raft.State() != raft.Leader {
		return "", fmt.Errorf("not leader")
	}

	c := &command{
		Op:  "get",
		Key: key,
	}
	b, err := json.Marshal(c)
	if err != nil {
		return "", err
	}

	f := s.raft.Apply(b, raftTimeout)
	if err := f.Error(); err != nil {
		return "", err
	}
	resp := f.Response()
	log.Print(resp)
	return resp.(string), nil

}
*/
type RaftWrapper struct {
	raft.StableStore
	raft.LogStore
}

func (bDb *RaftWrapper) Set(key []byte, val []byte) error {
	log.Printf("In set, key parameter %s", string(key))
	return bDb.StableStore.Set(key, val)
}

func (bDb *RaftWrapper) SetUint64(key []byte, val uint64) error {
	log.Printf("In set uin64, key parameter %s", string(key))
	return bDb.StableStore.SetUint64(key, val)
}

// FirstIndex returns the first index written. 0 for no entries.
func (bDb *RaftWrapper) FirstIndex() (uint64, error) {
	log.Printf("In log store first index")
	return bDb.LogStore.FirstIndex()
}

// LastIndex returns the last index written. 0 for no entries.
func (bDb *RaftWrapper) LastIndex() (uint64, error) {
	return bDb.LogStore.LastIndex()
}

// GetLog gets a log entry at a given index.
func (bDb *RaftWrapper) GetLog(index uint64, l *raft.Log) error {
	var c command
	err := bDb.LogStore.GetLog(index, l)
	if len(l.Data) != 0 {

		if err := json.Unmarshal(l.Data, &c); err == nil {
			//panic(fmt.Sprintf("failed to unmarshal command: %s", err.Error()))
			//log.Printf("In log store getlog %d %v %#v", index, c, l)
		}

	}
	return err
}

// StoreLog stores a log entry.
func (bDb *RaftWrapper) StoreLog(l *raft.Log) error {
	log.Printf("In log store storelog")
	return bDb.LogStore.StoreLog(l)
}

// StoreLogs stores multiple log entries.
func (bDb *RaftWrapper) StoreLogs(ls []*raft.Log) error {
	log.Printf("In log store storelogs")
	return bDb.LogStore.StoreLogs(ls)
}

// DeleteRange deletes a range of log entries. The range is inclusive.
func (bDb *RaftWrapper) DeleteRange(min uint64, max uint64) error {
	log.Printf("In log store deleterange")
	return bDb.LogStore.DeleteRange(min, max)
}

func (s *Store) Open(initialized bool, localID string) error {
	config := raft.DefaultConfig()
	//config.SnapshotThreshold = 3
	//config.SnapshotInterval = 3 * time.Second
	config.LocalID = raft.ServerID(localID)
	s.logger.Print("Resolving TCP addr....")

	addr, err := net.ResolveTCPAddr("tcp", s.RaftBind)
	if err != nil {
		return fmt.Errorf("Here I am : %w", err)
	}
	transport, err := raft.NewTCPTransport(s.RaftBind, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return fmt.Errorf("There it is : %w", err)
	}

	// Create the snapshot store. This allows the Raft to truncate the log.
	snapshots, err := raft.NewFileSnapshotStore(s.RaftDir, retainSnapshotCount, os.Stderr)

	boltDB, err := raftboltdb.NewBoltStore(filepath.Join(s.RaftDir, "raft.db"))
	if err != nil {
		return fmt.Errorf("new bolt store: %s", err)
	}
	mDb := &RaftWrapper{
		StableStore: boltDB,
		LogStore:    boltDB,
	}

	logStore := mDb

	ra, err := raft.NewRaft(config, (*fsm)(s), logStore, mDb, snapshots, transport)
	if err != nil {
		return fmt.Errorf("new raft: %s", err)
	}
	s.raft = ra
	if !initialized {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		ra.BootstrapCluster(configuration)
	}
	return nil
}

func (s *Store) Join(nodeID, addr string) error {
	s.logger.Printf("received join request for remote node %s at %s", nodeID, addr)

	configFuture := s.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		s.logger.Printf("failed to get raft configuration: %v", err)
		return err
	}

	for _, srv := range configFuture.Configuration().Servers {
		// If a node already exists with either the joining node's ID or address,
		// that node may need to be removed from the config first.
		if srv.ID == raft.ServerID(nodeID) || srv.Address == raft.ServerAddress(addr) {
			// However if *both* the ID and the address are the same, then nothing -- not even
			// a join operation -- is needed.
			if srv.Address == raft.ServerAddress(addr) && srv.ID == raft.ServerID(nodeID) {
				s.logger.Printf("node %s at %s already member of cluster, ignoring join request", nodeID, addr)
				return nil
			}

			future := s.raft.RemoveServer(srv.ID, 0, 0)
			if err := future.Error(); err != nil {
				return fmt.Errorf("error removing existing node %s at %s: %s", nodeID, addr, err)
			}
		}
	}

	f := s.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(addr), 0, 0)
	if f.Error() != nil {
		return f.Error()
	}
	s.logger.Printf("node %s at %s joined successfully", nodeID, addr)
	return nil
}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		// Encode data.
		b, err := json.Marshal(f.store)
		if err != nil {
			return err
		}

		// Write data to sink.
		if _, err := sink.Write(b); err != nil {
			return err
		}

		// Close the sink.
		return sink.Close()
	}()

	if err != nil {
		sink.Cancel()
	}

	return err
}

func (f *fsmSnapshot) Release() {}
