package internal

import (
	"errors"
	"fmt"
	"sort"
	"testing"
	"unsafe"
)

type C struct {
	BTree
	ref   map[string]string // the reference data
	pages map[uint64]BNode  // in-memory pages
}

func newC() *C {
	pages := map[uint64]BNode{}
	return &C{
		BTree: BTree{
			get: func(ptr uint64) []byte {
				node, ok := pages[ptr]
				if ok != true {
					panic("pages not found")
				}
				return node
			},
			new: func(node []byte) uint64 {
				if !(BNode(node).nBytes() <= BtreePageSize) {
					panic("page is too large")
				}

				ptr := uint64(uintptr(unsafe.Pointer(&node[0])))
				if pages[ptr] != nil {
					panic("pages pointer is taken")
				}
				pages[ptr] = node
				return ptr
			},
			del: func(ptr uint64) {
				if !(pages[ptr] != nil) {
					panic("pages pointer is nil")
				}
				delete(pages, ptr)
			},
		},
		ref:   map[string]string{},
		pages: pages,
	}
}

func (c *C) add(key string, val string) {
	err := c.Insert([]byte(key), []byte(val))
	if err != nil {
		fmt.Println(err)
		return
	}
	c.ref[key] = val // reference data
}

func (c *C) del(key string) error {
	_, err := c.Delete([]byte(key))
	if err != nil {
		return errors.New("Key not found")
	}
	c.ref[key] = ""
	return nil
}

func seed(c *C) {
	keys := make(map[string]string)
	for i := 0; i < 100; i++ {
		for j := 'a'; j <= 'z'; j++ {
			k := fmt.Sprintf("key%d%c", i, j)
			keys[k] = fmt.Sprintf("value%c%d", j, i)
		}
	}
	for k, v := range keys {
		c.add(k, v)
	}

}

func TestInsertAndAllKeysAreSorted(t *testing.T) {
	c := newC()
	seed(c)

	dbAsSlice := c.ToSlice()
	isSorted := sort.SliceIsSorted(dbAsSlice, func(i, j int) bool {
		return string(dbAsSlice[i].Key) < string(dbAsSlice[j].Key)
	})

	if !isSorted {
		t.Error("dbAsSlice is not sorted")
	}
}

func TestInsertUpdateAndDelete(t *testing.T) {
	c := newC()
	seed(c)

	dbAsSlice := c.ToSlice()
	uniq := make(map[string]bool)
	for _, k := range dbAsSlice {
		uniq[string(k.Key)] = true
	}

	if len(uniq) != len(dbAsSlice) {
		t.Error("dbAsSlice is not unique")
	}

	c.add("a", "b")
	get, err := c.Get([]byte("a"))
	if err != nil {
		return
	}

	if string(get) != "b" {
		t.Error("get != \"b\"", string(get))
	}

	err = c.del("a")
	if err != nil {
		t.Error("del failed", err)
		return
	}
	get, err = c.Get([]byte("a"))
	if err == nil {
		t.Error("the key should not exist")
	}

}
