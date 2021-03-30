package main

import (
	"fmt"
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
		want   interface{}
	}{
		{
			name: "addingone emptylist",
			fields: fields{
				m: map[string]interface{}{},
			},
			args: args{
				val: []string{"one"},
			},
			want: 1,
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
			want: 2,
		},
		{
			name: "emptyname emptylist",
			fields: fields{
				m: map[string]interface{}{},
			},
			want: fmt.Errorf("(error) ERR wrong number of arguments for 'rpush' command"),
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

func Test_Store_ListCommands(t *testing.T) {
	s := New()
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
	defer s.raft.Shutdown()
	/*for s.raft.Leader() == "" {
		time.Sleep(500 * time.Millisecond)
	}*/
	<-s.raft.LeaderCh()

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
		flag, err := s.Expire("key", 1)
		assert.Equal(t, nil, err)
		// if checked immediately then flag is 1
		assert.Equal(t, flag, 1)
		Sleep(1 * time.Second)

		flag, err = s.Expire("key", 10)
		//if checked after 1 sec then flag is 0
		assert.Equal(t, flag, 0)
	})

}
