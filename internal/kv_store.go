package internal

import (
	"encoding/binary"
	"fmt"
	"golang.org/x/sys/unix"
	"syscall"
)

type KV struct {
	Path string //filename

	//internals
	fd     int
	failed bool
	tree   BTree

	page struct {
		flushed uint64
		temp    [][]byte
	}

	mmap struct {
		total  int
		chunks [][]byte
	}
}

func (db *KV) Open() error {
	db.tree.get = db.pageRead
	db.tree.new = db.pageAppend
	db.tree.del = func(uint64) {}

	return nil
}

func (db *KV) Get(key []byte) ([]byte, bool) {
	val, err := db.tree.Get(key)
	if err != nil {
		return nil, false
	}
	return val, true
}

func (db *KV) Set(key, value []byte) error {
	meta := saveMeta(db)
	if err := db.tree.Insert(key, value); err != nil {
		return err
	}

	return updateOrRevert(db, meta)
}

func (db *KV) Delete(key []byte) (bool, error) {
	deleted, err := db.tree.Delete(key)
	if err != nil {
		return deleted, err
	}
	return deleted, updateFile(db)
}

func (db *KV) pageRead(ptr uint64) []byte {
	start := uint64(0)

	for _, chunk := range db.mmap.chunks {
		end := start + uint64(len(chunk))/BtreePageSize
		if ptr < end {
			offset := BtreePageSize * (ptr - start)
			return chunk[offset : offset+BtreePageSize]
		}
		start = end
	}
	panic("bad ptr")
}

func (db *KV) pageAppend(node []byte) uint64 {
	ptr := db.page.flushed + uint64(len(db.page.temp))
	db.page.temp = append(db.page.temp, node)
	return ptr
}

func writePages(db *KV) error {
	size := (int(db.page.flushed) + len(db.page.temp)) * BtreePageSize
	if err := extendMmap(db, size); err != nil {
		return err
	}

	//write data pages to file
	offset := int64(db.page.flushed * BtreePageSize)
	if _, err := unix.Pwritev(db.fd, db.page.temp, offset); err != nil {
		return err
	}

	db.page.flushed += uint64(len(db.page.temp))
	db.page.temp = db.page.temp[:0]
	return nil
}

func extendMmap(db *KV, size int) error {
	if size <= db.mmap.total {
		return nil // no need to extend
	}

	alloc := max(db.mmap.total, 64<<20) //double the address space
	for db.mmap.total+alloc < size {
		alloc *= 2 //still extend?
	}

	//read only
	chunk, err := syscall.Mmap(db.fd, int64(db.mmap.total), alloc, syscall.PROT_READ, syscall.MAP_SHARED)

	if err != nil {
		return fmt.Errorf("mmap failed: %v", err)
	}

	db.mmap.total += alloc
	db.mmap.chunks = append(db.mmap.chunks, chunk)

	return nil
}

const DbSig = "GoDBMetaData"

// | sig | root_ptr | page_used |
// | 16B |    8B    |     8B    |
func saveMeta(db *KV) []byte {
	var data [32]byte
	copy(data[:16], []byte(DbSig))
	binary.LittleEndian.PutUint64(data[16:], db.tree.root)
	binary.LittleEndian.PutUint64(data[24:], db.page.flushed)
	return data[:]
}

func loadMeta(db *KV, data []byte) {
	db.tree.root = binary.LittleEndian.Uint64(data[16:24])
	db.page.flushed = binary.LittleEndian.Uint64(data[24:32])
}

func updateRoot(db *KV) error {
	if _, err := syscall.Pwrite(db.fd, saveMeta(db), 0); err != nil {
		return fmt.Errorf("write meta page failed: %v", err)
	}
	return nil
}

func updateOrRevert(db *KV, meta []byte) error {
	//2-phase update
	if db.failed {
		saveMeta(db)
		db.failed = false
	}
	err := updateFile(db)
	//revert on error
	if err != nil {
		db.failed = true
		//discard temps
		loadMeta(db, meta)
		db.page.temp = db.page.temp[:0]
	}
	return err
}
