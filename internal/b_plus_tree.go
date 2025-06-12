package internal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
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
	copy(new[pos+4+kLen:], value)
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
func nodeReplace2Kid(new, old BNode, index uint16, ptr uint64, key []byte) {
	new.setHeader(BNodeNode, old.nKeys()-1)
	nodeAppendRange(new, old, 0, 0, index)
	nodeAppendKV(new, index, ptr, key, nil)
	nodeAppendRange(new, old, index, index+2, old.nKeys()-(index+1))
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
	leftLeft := BNode(make([]byte, BtreePageSize))
	middle := BNode(make([]byte, BtreePageSize))
	nodeSplit2(leftLeft, middle, left)
	if leftLeft.nBytes() > BtreePageSize {
		panic("nodeSplit3: Unable to split into third node!!!")
	}
	return 3, [3]BNode{leftLeft, middle, left}
}

func nodeGet(tree *BTree, node BNode, index uint16, key []byte) ([]byte, error) {
	kPtr := node.getPtr(index)

	kNode := BNode(tree.get(kPtr))
	return treeGet(tree, kNode, key)
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

func nodeDelete(tree *BTree, node BNode, index uint16, key []byte) BNode {
	kPtr := node.getPtr(index)
	updated := treeDelete(tree, tree.get(kPtr), key)
	if len(updated) == 0 {
		return BNode{} //not found
	}

	tree.del(kPtr)

	//check for merging
	newNode := BNode(make([]byte, BtreePageSize))
	mergeDir, sibling := shouldMerge(tree, node, index, updated)
	switch {
	case mergeDir < 0: //left
		merged := BNode(make([]byte, BtreePageSize))
		nodeMerge(merged, sibling, updated)
		tree.del(node.getPtr(index - 1))
		nodeReplace2Kid(newNode, node, index-1, tree.new(merged), merged.getKey(0))
	case mergeDir > 0: //right
		merged := BNode(make([]byte, BtreePageSize))
		nodeMerge(merged, updated, sibling)
		tree.del(node.getPtr(index + 1))
		nodeReplace2Kid(newNode, node, index, tree.new(merged), merged.getKey(0))
	case updated.nKeys() == 0:
		if !(node.nKeys() == 1 && index == 0) {
			panic("node has 1 empty child, but no sibling")
		}
		newNode.setHeader(BNodeNode, 0)
	case updated.nKeys() > 0:
		nodeReplaceKidN(tree, newNode, node, index, updated)
	}

	return newNode
}

func nodeMerge(new, left, right BNode) {
	if left.nBytes()+right.nBytes()-HEADER > BtreePageSize {
		panic("nodeMerge: Can't merge nodes, sum of sizes too big")
	}
	lNKeys := left.nKeys()
	rNKeys := right.nKeys()
	new.setHeader(left.bType(), lNKeys+rNKeys)

	nodeAppendRange(new, left, 0, 0, lNKeys)
	nodeAppendRange(new, right, lNKeys, 0, rNKeys)
}

func treeGet(tree *BTree, node BNode, key []byte) ([]byte, error) {
	index := nodeLookupLE(node, key)

	switch node.bType() {
	case BNodeLeaf:
		if bytes.Equal(key, node.getKey(index)) {
			return node.getVal(index), nil
		}
		return nil, errors.New("key not found")
	case BNodeNode:
		return nodeGet(tree, node, index, key)

	default:
		panic("unknown node type")
	}

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

func treeDelete(tree *BTree, node BNode, key []byte) BNode {
	index := nodeLookupLE(node, key)
	switch node.bType() {
	case BNodeLeaf:
		if bytes.Equal(key, node.getKey(index)) {
			newNode := BNode(make([]byte, BtreePageSize))
			leafDelete(newNode, node, index)
			return newNode
		} else {
			return BNode{}
		}
	case BNodeNode:
		return nodeDelete(tree, node, index, key)
	default:
		panic("bad node")
	}
}

func leafInsert(
	new, old BNode, index uint16, key []byte, value []byte) {
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

func leafDelete(new, old BNode, index uint16) {
	new.setHeader(BNodeLeaf, old.nKeys()-1)
	nodeAppendRange(new, old, 0, 0, index)
	nodeAppendRange(new, old, index, index+1, old.nKeys()-(index+1))

}

func shouldMerge(tree *BTree, node BNode, index uint16, updated BNode) (int, BNode) {
	if updated.nBytes() > BtreePageSize {
		return 0, BNode{}
	}

	if index > 0 {
		sibling := BNode(tree.get(node.getPtr(index - 1)))
		merged := sibling.nBytes() + updated.nBytes() - HEADER
		if merged <= BtreePageSize {
			return -1, sibling //left
		}
	}
	if index+1 < node.nKeys() {
		sibling := BNode(tree.get(node.getPtr(index + 1)))
		merged := sibling.nBytes() + updated.nBytes() - HEADER
		if merged <= BtreePageSize {
			return +1, sibling //right
		}
	}

	return 0, BNode{}
}

func (tree *BTree) Get(key []byte) ([]byte, error) {
	if tree.root == 0 {
		return nil, errors.New("key not found")
	}

	root := BNode(tree.get(tree.root))

	return treeGet(tree, root, key)

	//for i := uint16(0); i < root.nKeys(); i++ {
	//	ptr := root.getPtr(i)
	//	bNode := BNode(tree.get(ptr))
	//}

}

func (tree *BTree) Insert(key, value []byte) error {
	// check the key and value length against the node format
	if err := checkLimit(key, value); err != nil {
		return err
	}

	// create the first node
	if tree.root == 0 {
		root := BNode(make([]byte, BtreePageSize))
		root.setHeader(BNodeLeaf, 2)
		nodeAppendKV(root, 0, 0, nil, nil)
		nodeAppendKV(root, 1, 0, key, value)
		tree.root = tree.new(root)
		return nil
	}

	node := treeInsert(tree, tree.get(tree.root), key, value)

	nSplit, split := nodeSplit3(node)
	tree.del(tree.root)
	if nSplit > 1 {
		root := BNode(make([]byte, BtreePageSize))
		root.setHeader(BNodeNode, nSplit)
		for i, kNode := range split[:nSplit] {
			ptr, key := tree.new(kNode), kNode.getKey(0)
			nodeAppendKV(root, uint16(i), ptr, key, nil)
		}
		tree.root = tree.new(root)
	} else {
		tree.root = tree.new(split[0])
	}

	return nil
}

func (tree *BTree) Delete(key []byte) (bool, error) {
	if tree.root == 0 {
		return false, errors.New("Tree is empty")
	}

	updated := treeDelete(tree, tree.get(tree.root), key)

	if len(updated) == 0 {
		return false, errors.New("node not found")
	}

	tree.del(tree.root)
	if updated.bType() == BNodeNode && updated.nKeys() == 1 {
		tree.root = updated.getPtr(0)
	} else {
		tree.root = tree.new(updated)
	}
	return true, nil
}

func init() {
	node1max := HEADER + 8 + 2 + 4 + BtreeMaxKeySize + BtreeMaxValSize

	//noinspection GoBoolExpressions
	if node1max > BtreePageSize {
		panic("Node size is too big")
	}
}

func checkLimit(key, value []byte) error {
	if len(key) > BtreeMaxKeySize {
		return errors.New("key is too large")
	}

	if len(value) > BtreeMaxValSize {
		return errors.New("value is too large")
	}

	return nil
}

func (tree *BTree) String() string {
	var sb strings.Builder
	if tree.root == 0 {
		sb.WriteString("<empty tree>")
		return sb.String()
	}
	tree.stringifyNode(&sb, tree.get(tree.root), 0)
	return sb.String()
}

func (tree *BTree) stringifyNode(sb *strings.Builder, node BNode, depth int) {
	indent := strings.Repeat("  ", depth)
	kind := node.bType()
	count := node.nKeys()

	if kind == BNodeLeaf {
		sb.WriteString(fmt.Sprintf("%sLeaf:\n", indent))
		for i := uint16(1); i < count; i++ {
			k := node.getKey(i)
			v := node.getVal(i)
			sb.WriteString(fmt.Sprintf("%s  [%s] => [%s]\n", indent, string(k), string(v)))
		}
	} else if kind == BNodeNode {
		sb.WriteString(fmt.Sprintf("%sInternal:\n", indent))
		for i := uint16(0); i < count; i++ {
			ptr := node.getPtr(i)
			key := node.getKey(i)
			if i > 0 {
				sb.WriteString(fmt.Sprintf("%s  <key> [%s]\n", indent, string(key)))
			}
			child := tree.get(ptr)
			tree.stringifyNode(sb, child, depth+1)
		}
	} else {
		sb.WriteString(fmt.Sprintf("%sUnknown node type\n", indent))
	}
}

type kv struct {
	Key   []byte
	Value []byte
}

func (kv kv) String() string {
	return fmt.Sprintf("{%c: %c}", kv.Key, kv.Value)
}

func (tree *BTree) ToSlice() []kv {
	if tree.root == 0 {
		return make([]kv, 0)
	}
	var resp []kv
	tree.sliceNode(&resp, tree.get(tree.root))

	return resp
}

func (tree *BTree) sliceNode(resp *[]kv, node BNode) {
	kind := node.bType()
	count := node.nKeys()
	switch kind {
	case BNodeLeaf:
		for i := uint16(1); i < count; i++ {
			k0 := node.getKey(i)
			v0 := node.getVal(i)
			k := make([]byte, len(k0))
			copy(k, k0)
			v := make([]byte, len(v0))
			copy(v, v0)
			*resp = append(*resp, kv{k, v})
		}

	case BNodeNode:
		for i := uint16(0); i < count; i++ {
			ptr := node.getPtr(i)
			child := tree.get(ptr)
			tree.sliceNode(resp, child)
		}

	default:
		panic("Unknown node type")
	}
}
