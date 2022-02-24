package main

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
)

func Test_fsm_applyRPush(t *testing.T) {
	type fields struct {
		m map[string]interface{}
	}
	type args struct {
		key string
		val []string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *OurResponse
	}{
		{
			name: "addingone emptylist",
			fields: fields{
				m: map[string]interface{}{},
			},
			args: args{
				val: []string{"one"},
			},
			want: &OurResponse{length: 1},
		},
		{
			name: "adding to existing list",
			fields: fields{
				m: map[string]interface{}{
					"": []string{"one"},
				},
			},
			args: args{
				val: []string{"two"},
			},
			want: &OurResponse{length: 2},
		},
		{
			name: "emptyname emptylist",
			fields: fields{
				m: map[string]interface{}{},
			},
			want: &OurResponse{err: fmt.Errorf("(error) ERR wrong number of arguments for 'rpush' command")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &fsm{
				m: tt.fields.m,
			}
			if got := f.applyRPush(tt.args.key, tt.args.val); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("fsm.applyRPush() = %v, want %v", got, tt.want)
			}
		})
	}
}

func getTestStore(t *testing.T) (s *Store) {
	s = New()
	last = time.Now()
	//s.RaftDir = "testing"
	//s.RaftBind = "localhost:1234"
	//err := s.Open(false, "testing")
	addr, transport := raft.NewInmemTransport("")
	store := raft.NewInmemStore()
	config := raft.DefaultConfig()
	config.LocalID = "testRaft"
	ra, err := raft.NewRaft(config, (*fsm)(s), store, store, &raft.InmemSnapshotStore{}, transport)
	configuration := raft.Configuration{
		Servers: []raft.Server{
			{
				Address: addr,
				ID:      config.LocalID,
			},
		},
	}
	assert.Equal(t, nil, err)
	ra.BootstrapCluster(configuration)

	s.raft = ra
	//defer s.raft.Shutdown()
	<-s.raft.LeaderCh()
	return s
}

func Test_Store_ListCommands(t *testing.T) {
	s := getTestStore(t)
	defer s.raft.Shutdown()

	t.Run("RPush returns 2", func(t *testing.T) {
		c, err := s.RPush("key", []string{"val1", "val2"})
		assert.Equal(t, nil, err)
		assert.Equal(t, c, 2)

	})

	t.Run("second RPush returns 3", func(t *testing.T) {
		c, err := s.RPush("key", []string{"val3"})
		assert.Equal(t, nil, err)
		assert.Equal(t, c, 3)
	})

	t.Run("LRANGE key 0 0 returns 1st element", func(t *testing.T) {
		c, err := s.LRange("key", 0, 0)
		assert.Equal(t, nil, err)
		assert.Equal(t, []string{"val1"}, c)
	})
	t.Run("LRANGE key 1 0 returns 2nd element", func(t *testing.T) {
		c, err := s.LRange("key", 1, 1)
		assert.Equal(t, nil, err)
		assert.Equal(t, []string{"val2"}, c)
	})
	t.Run("LRANGE key -3 2 returns three element", func(t *testing.T) {
		c, err := s.LRange("key", -3, 2)
		assert.Equal(t, nil, err)
		assert.Equal(t, []string{"val1", "val2", "val3"}, c)
	})
	t.Run("LRANGE key -100 100 returns three elements", func(t *testing.T) {
		c, err := s.LRange("key", -100, 100)
		assert.Equal(t, nil, err)
		assert.Equal(t, []string{"val1", "val2", "val3"}, c)
	})
	t.Run("LRANGE key 5 10 returns none", func(t *testing.T) {
		c, err := s.LRange("key", 5, 10)
		assert.Equal(t, nil, err)
		assert.Equal(t, []string{}, c)
	})
	t.Run("LRANGE wrong type", func(t *testing.T) {
		err := s.Set("foo", "bar")
		assert.Equal(t, nil, err)
		_, err = s.LRange("foo", 0, 0)
		assert.NotEqual(t, nil, err)
	})

	t.Run("EXPIRES the key in 1 second", func(t *testing.T) {
		tm := 1
		flag, err := s.Expire("key", tm)
		assert.Equal(t, nil, err)
		// if checked immediately then flag is 1
		assert.Equal(t, flag, true)
		Sleep((time.Duration(tm)) * time.Second)

		flag, err = s.Expire("key", 10)
		//if checked after 1 sec then flag is 0
		assert.Equal(t, flag, false)
	})

	t.Run("TTL for a key after 1 second that expires in 2 second is 1 second", func(t *testing.T) {
		tm := 2
		err := s.Set("key", "any")
		assert.Equal(t, nil, err)

		flag, err := s.Expire("key", tm)
		assert.Equal(t, nil, err)
		assert.Equal(t, flag, true)

		ttl, err := s.TTL("key")
		assert.Equal(t, ttl, 2)

		Sleep((time.Duration(1)) * time.Second)
		ttl, err = s.TTL("key")
		assert.Equal(t, ttl, 1)
	})

}

func Test_WEB_API(t *testing.T) {
	s := getTestStore(t)
	defer s.raft.Shutdown()
	kv := NewKeyValue()
	kv.RaftStore = s
	ts := httptest.NewServer(http.HandlerFunc(kv.parseCmd))
	defer ts.Close()

	t.Run("SETS a value", func(t *testing.T) {
		resp, err := http.Get(ts.URL + "?cmd=set&key=key1&value=value1")
		assert.Equal(t, nil, err)
		assert.Equal(t, 200, resp.StatusCode)
		assert.Equal(t, s.m["key1"], "value1")
	})

	t.Run("RPush a value", func(t *testing.T) {
		resp, err := http.Get(ts.URL + "?cmd=rpush&key=key2&value=value2&value=value3")
		assert.Equal(t, nil, err)
		assert.Equal(t, 200, resp.StatusCode)
		assert.Equal(t, s.m["key2"], []string{"value2", "value3"})
	})

	t.Run("RPush a value to a existing key with a wrong type", func(t *testing.T) {
		resp, _ := http.Get(ts.URL + "?cmd=rpush&key=key1&value=value2&value=value3")
		body, _ := io.ReadAll(resp.Body)
		assert.Equal(t, "(error) WRONGTYPE Operation against a key holding the wrong kind of value\n", string(body))
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	})
}
