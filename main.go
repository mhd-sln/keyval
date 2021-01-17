package main

import (
	"fmt"
	"net/http"
	"strings"
)

type KeyValue struct {
	pairs map[string]string
}

func (kv KeyValue) handler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		path := kv.pairs[strings.Trim(r.URL.Path, "/")]
		fmt.Fprintf(w, "Path = %s", path)
	case "POST":
		queries := r.URL.Query()
		for qKey, qValue := range queries {
			kv.pairs[qKey] = qValue[0]
		}
	default:
		fmt.Fprintf(w, "Sorry, only GET and POST methods are supported.")
	}

}

func NewKeyValue() *KeyValue {
	var k KeyValue
	k.pairs = make(map[string]string)
	return &k
}

func main() {
	kv := NewKeyValue()
	http.HandleFunc("/", kv.handler)
	http.ListenAndServe(":8090", nil)
}
