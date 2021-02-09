package main

import (
	"encoding/gob"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
)

type KeyValue struct {
	InMem   map[string]string
	version int
	stores  store
}

type Message struct {
	Key   string
	Value string
}

func (kv *KeyValue) load() error {
	f, err := os.Open(kv.stores.filename())
	if err != nil {
		return err
	}
	defer f.Close()
	kv.version = 1000

	return kv.stores.decode(f, &kv.InMem)
}

func (kv *KeyValue) handler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		value := kv.InMem[strings.Trim(r.URL.Path, "/")]
		fmt.Fprintf(w, "Value = %s", value)
	case "POST":
		queries := r.URL.Query()
		for qKey, qValue := range queries {
			// The value is the latest value, shall we combine all the values
			valueLen := len(qValue)
			kv.InMem[qKey] = qValue[valueLen-1]
		}
		//err := ioutil.WriteFile("keyvalue.json", data, 0644)
		f, err := os.Create(kv.stores.filename())
		err = kv.stores.encode(f, &kv.InMem)
		if err != nil {
			fmt.Println(err)
		}
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
	k.InMem = make(map[string]string)
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

func main() {
	store := flag.String("store", "json", "json or gob")
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
	http.ListenAndServe(":8090", nil)
}
