package internal

import (
	"bytes"
	"encoding/binary"
)

const HEADER = 4

//| type | nkeys |  pointers  |   offsets  | key-values | unused |
//|  2B  |   2B  | nkeys * 8B | nkeys * 2B |     ...    |        |

//	Key values
//
// |klen | vlen | key | val |
// | 2B  |  2B  | ... | ... |
const BtreePageSize = 4096
const BtreeMaxKeySize = 1000
const BtreeMaxValSize = 3000

const (
	BNodeNode = 1 // internal node (no values)
	BNodeLeaf = 2 // leaf node (contains values)
)

type BTree struct {
	//pointer (nonzero page number)
	root uint64

	get func(uint64) []byte //dereference a pointer
	new func([]byte) uint64 // allocate new page
	del func(uint64)        // deallocate a page
}

type BNode []byte

func (node BNode) bType() uint16 {
	return binary.LittleEndian.Uint16(node[0:2])
}

func (node BNode) nKeys() uint16 {
	return binary.LittleEndian.Uint16(node[2:4])
}

func (node BNode) setHeader(bType, nKeys uint16) {
	binary.LittleEndian.PutUint16(node[0:2], bType)
	binary.LittleEndian.PutUint16(node[2:4], nKeys)
}

func (node BNode) getPtr(index uint16) uint64 {
	if index >= node.nKeys() {
		panic("index out of range")
	}
	pos := HEADER + 8*index
	return binary.LittleEndian.Uint64(node[pos:])
}

func (node BNode) setPtr(index uint16, val uint64) {
	if index > node.nKeys() {
		panic("index out of range")
	}
	pos := HEADER + 8*index
	binary.LittleEndian.PutUint64(node[pos:], val)
}

func offsetPos(node BNode, index uint16) uint16 {
	if index < 1 || index > node.nKeys() {
		panic("index out of range")
	}
	return HEADER + 8*node.nKeys() + 2*(index-1)
}

func (node BNode) getOffset(index uint16) uint16 {
	if index == 0 {
		return 0
	}
	return binary.LittleEndian.Uint16(node[offsetPos(node, index):])
}

func (node BNode) setOffset(index, offset uint16) {
	binary.LittleEndian.PutUint16(node[offsetPos(node, index):], offset)
}

func (node BNode) kvPos(index uint16) uint16 {
	if index > node.nKeys() {
		panic("index out of range")
	}
	return HEADER + 8*node.nKeys() + 2*node.nKeys() + node.getOffset(index)
}

func (node BNode) getKey(index uint16) []byte {
	if index > node.nKeys() {
		panic("index out of range")
	}
	pos := node.kvPos(index)
	kLen := binary.LittleEndian.Uint16(node[pos:])
	return node[pos+4:][:kLen]
}

func (node BNode) getVal(index uint16) []byte {
	if index >= node.nKeys() {
		panic("index out of range")
	}
	pos := node.kvPos(index)
	kLen := binary.LittleEndian.Uint16(node[pos:])
	vLen := binary.LittleEndian.Uint16(node[pos+2:])
	return node[pos+4+kLen:][:vLen]
}

func (node BNode) nBytes() uint16 {
	return node.kvPos(node.nKeys())
}

func nodeLookupLE(node BNode, key []byte) uint16 {
	nKeys := node.nKeys()
	found := uint16(0)
	for i := uint16(1); i < nKeys; i++ {
		cmp := bytes.Compare(node.getKey(i), key)
		if cmp <= 0 {
			found = i
		}
		if cmp >= 0 {
			break
		}
	}
	return found
}

func leafInsert(
	new BNode, old BNode, index uint16, key []byte, value []byte) {
	new.setHeader(BNodeLeaf, old.nKeys()+1)
	nodeAppendRange(new, old, 0, 0, index)
	nodeAppendKV(new, index, 0, key, value)
	nodeAppendRange(new, old, index+1, index, old.nKeys()-index)
}

func leafUpdate(
	new BNode, old BNode, index uint16, key []byte, value []byte) {
	new.setHeader(BNodeLeaf, old.nKeys())
	nodeAppendRange(new, old, 0, 0, index)
	nodeAppendKV(new, index, 0, key, value)
	nodeAppendRange(new, old, index+1, index+1, old.nKeys()-(index+1))
}

func nodeAppendKV(new BNode, index uint16, ptr uint64, key, value []byte) {
	//ptrs
	new.setPtr(index, ptr)
	// KVs
	pos := new.kvPos(index)
	kLen := uint16(len(key))
	vLen := uint16(len(value))
	binary.LittleEndian.PutUint16(new[pos+0:], kLen)
	binary.LittleEndian.PutUint16(new[pos+2:], vLen)
	copy(new[pos+4:], key)
	copy(new[pos+kLen:], value)
	// the offset of the next key
	new.setOffset(index+1, new.getOffset(index)+4+kLen+vLen)
}

//func nodeAppendRange(new, old BNode, newDst, oldSrc, n uint16) {
//	if oldSrc+n > old.nKeys() {
//		panic("nodeAppendRange: old index out of range")
//	}
//	if newDst+n > new.nKeys() {
//		panic("nodeAppendRange: new index out of range")
//	}
//	if n == 0 {
//		return
//	}
//
//	//ptrs
//	for i := uint16(0); i < n; i++ {
//		new.setPtr(newDst+i, old.getPtr(oldSrc+i))
//	}
//
//	//offsets
//	beginDst := new.getOffset(newDst)
//	beginSrc := old.getOffset(oldSrc)
//
//	for i := uint16(1); i <= n; i++ {
//		offset := beginDst + old.getOffset(oldSrc+i) - beginSrc
//		new.setOffset(newDst+i, offset)
//	}
//
//	//KVs
//	begin := old.kvPos(oldSrc)
//	end := old.kvPos(oldSrc + n)
//	copy(new[new.kvPos(newDst):], old[begin:end])
//}

func nodeAppendRange(new, old BNode, newDst, oldSrc, n uint16) {
	for i := uint16(0); i < n; i++ {
		dst, src := newDst+i, oldSrc+i
		nodeAppendKV(new, dst, old.getPtr(src), old.getKey(src), old.getVal(src))
	}
}

func nodeReplaceKidN(tree *BTree, new, old BNode, index uint16, kids ...BNode) {
	inc := uint16(len(kids))
	new.setHeader(BNodeNode, old.nKeys()+inc-1)
	nodeAppendRange(new, old, 0, 0, index)
	for i, node := range kids {
		nodeAppendKV(new, index+uint16(i), tree.new(node), node.getKey(0), nil)
	}
	nodeAppendRange(new, old, index+inc, index+1, old.nKeys()-(index+1))
}

//func nodeSplit2(left, right, old BNode) {
//	currentNode := &right
//	for i := uint16(0); i < old.nKeys(); i++ {
//		pos := old.kvPos(i)
//		kLen := binary.LittleEndian.Uint16(old[pos:])
//		vLen := binary.LittleEndian.Uint16(old[pos+2:])
//		if right.nBytes()+4+kLen+vLen > BtreePageSize {
//			currentNode = &left
//		}
//		ptr := currentNode.getPtr(i)
//		nodeAppendKV(*currentNode, i, ptr, currentNode.getKey(i), currentNode.getVal(i))
//	}
//
//}

func nodeSplit2(left, right, old BNode) {
	if old.nKeys() < 2 {
		panic("nodeSplit2: Can't split a node with one key!")
	}
	nLeft := old.nKeys() / 2
	leftBytes := func() uint16 {
		return 4 + 8*nLeft + 2*nLeft + old.getOffset(nLeft)
	}
	for leftBytes() > BtreePageSize {
		nLeft--
	}
	if nLeft < 1 {
		panic("nodeSplit2: Can't fit a key in page")
	}
	rightBytes := func() uint16 {
		return old.nBytes() - leftBytes() + 4
	}
	for rightBytes() > BtreePageSize {
		nLeft++
	}

	if nLeft >= old.nKeys() {
		panic("nodeSplit2: Can't split the node")
	}
	nRight := old.nKeys() - nLeft

	//new nodes
	left.setHeader(old.bType(), nLeft)
	right.setHeader(old.bType(), nRight)
	nodeAppendRange(left, old, 0, 0, nLeft)
	nodeAppendRange(right, old, 0, nLeft, nRight)

	if right.nBytes() > BtreePageSize {
		panic("nodeSplit2: The right node should be smaller, but something went wrong.")
	}
}

func nodeSplit3(old BNode) (uint16, [3]BNode) {
	if old.nBytes() <= BtreePageSize {
		old = old[:BtreePageSize]
		return 1, [3]BNode{old}
	}

	left := BNode(make([]byte, 2*BtreePageSize))
	right := BNode(make([]byte, BtreePageSize))
	nodeSplit2(left, right, old)
	if left.nBytes() <= BtreePageSize {
		left = left[:BtreePageSize]
		return 2, [3]BNode{left, right}
	}
	leftleft := BNode(make([]byte, BtreePageSize))
	middle := BNode(make([]byte, BtreePageSize))
	nodeSplit2(leftleft, middle, left)
	if leftleft.nBytes() > BtreePageSize {
		panic("nodeSplit3: Unable to split into third node!!!")
	}
	return 3, [3]BNode{leftleft, middle, left}
}

// insert a KV into a node, the result might split be split.
// the caller is responsible for deallocating the input node
// and splitting and allocating result nodes.
func treeInsert(tree *BTree, node BNode, key, value []byte) BNode {
	newNode := BNode(make([]byte, 2*BtreePageSize))

	//where to insert the key
	index := nodeLookupLE(node, key)

	switch node.bType() {
	case BNodeLeaf:
		//leaf, node.getKey(idx) <= key
		if bytes.Equal(key, node.getKey(index)) {
			//key is found
			leafUpdate(newNode, node, index, key, value)
		} else {
			leafInsert(newNode, node, index+1, key, value)
		}
	case BNodeNode:
		nodeInsert(tree, newNode, node, index, key, value)
	default:
		panic("bad node")
	}
	return newNode
}

func nodeInsert(tree *BTree, new, node BNode, index uint16, key, value []byte) {
	kPtr := node.getPtr(index)
	// insert into the kid node
	kNode := treeInsert(tree, tree.get(kPtr), key, value)
	//split the result
	nSplit, split := nodeSplit3(kNode)
	tree.del(kPtr)
	// update the child links
	nodeReplaceKidN(tree, new, node, index, split[:nSplit]...)
}

func init() {
	node1max := HEADER + 8 + 2 + 4 + BtreeMaxKeySize + BtreeMaxValSize

	//noinspection GoBoolExpressions
	if node1max > BtreePageSize {
		panic("Node size is too big")
	}
}
