package lsm

// 这个不是严格的b+树，如果有b+树会有一个nextleaf
type BNode struct {
	keys     []K
	values   []V
	isLeaf   bool
	numKeys  int
	children []*BNode // 非叶子节点才有子节点
	parent   *BNode   // 父节点
}

type bPlusTree struct {
	root    *BNode
	maxKeys int // 每个节点最多存储的键值对数量
}

type BPlusTreeRun2 struct {
	bTree       *bPlusTree
	numElements int
	min         K
	max         K
}

func NewBPlusTreeRun2(degree int, minKey K, maxKey K) *BPlusTreeRun2 {
	return &BPlusTreeRun2{
		bTree: newBPlusTree(degree),
		min:   minKey,
		max:   maxKey,
	}
}

func newBPlusTree(maxKeys int) *bPlusTree {
	return &bPlusTree{root: &BNode{isLeaf: true}, maxKeys: maxKeys}
}

func (r *BPlusTreeRun2) InsertKey(key K, value V) {
	r.bTree.insert(key, value)
	r.numElements++
}

func (r *BPlusTreeRun2) DeleteKey(key K) {
	// r.numElements--
}

func (r *BPlusTreeRun2) LookUp(key K) (found bool, value V) {
	return r.bTree.lookup(key)
}

func (r *BPlusTreeRun2) GetMin() (key K) {
	return r.min
}

func (r *BPlusTreeRun2) GetMax() (key K) {
	return r.max
}

func (r *BPlusTreeRun2) NumElements() int {
	return r.numElements
}

func (r *BPlusTreeRun2) GetAll() []KVPair {
	return r.bTree.getAll()
}

func (r *BPlusTreeRun2) Range(key1 K, key2 K) []KVPair {
	return r.bTree.rangeQuery(key1, key2)
}

func (t *bPlusTree) insert(key K, value V) {
	leaf := t.getLeafNode(key)

	i := findInsertionIndex(leaf.keys, key)
	if i < leaf.numKeys && leaf.keys[i] == key {
		leaf.values[i] = value // 如果已经存在，则更新值
	} else {
		leaf.keys = insertKeyElement(leaf.keys, i, key)
		leaf.values = insertValueElement(leaf.values, i, value)
		leaf.numKeys++

		if leaf.numKeys == t.maxKeys {
			t.splitLeafNode(leaf)
		}
	}
}

func (t *bPlusTree) lookup(key K) (found bool, value V) {
	leaf := t.getLeafNode(key)

	i := findInsertionIndex(leaf.keys, key)
	if i < leaf.numKeys && leaf.keys[i] == key {
		return true, leaf.values[i]
	} else {
		return false, V{}
	}
}

func (t *bPlusTree) getAll() []KVPair {
	var pairs []KVPair
	// n := t.getLeftmostLeafNode()
	// for n != nil {
	// 	for i := 0; i < n.numKeys; i++ {
	// 		pairs = append(pairs, KVPair{n.keys[i], n.values[i]})
	// 	}
	// 	n = n.children[len(n.children)-1]
	// }
	// return pairs

	pairs = traverse(t.root)
	return pairs
}

func traverse(node *BNode) []KVPair {
	var pairs []KVPair
	for i := 0; i < node.numKeys; i++ {
		pairs = append(pairs, KVPair{node.keys[i], node.values[i]})
	}
	if node.isLeaf {
		return pairs
	}

	for !node.isLeaf {
		for i := 0; i < len(node.children); i++ {
			childPairs := traverse(node.children[i])
			pairs = append(pairs, childPairs...)
		}
	}
	return pairs
}

func (t *bPlusTree) rangeQuery(key1 K, key2 K) []KVPair {
	var pairs []KVPair

	// node := t.getLeafNode(key1)
	// for node != nil {
	// 	for i := 0; i < node.numKeys; i++ {
	// 		// if node.keys[i] >= key1 && node.keys[i] <= key2 {
	// 		if !lessThan(node.keys[i], key1) && !moreThan(node.keys[i], key2) {
	// 			pairs = append(pairs, KVPair{node.keys[i], node.values[i]})
	// 		}
	// 	}
	// 	// if node.keys[node.numKeys-1] >= key2 {
	// 	if !lessThan(node.keys[node.numKeys-1], key2) {
	// 		break
	// 	}
	// 	node = node.children[len(node.children)-1]
	// }

	return pairs
}

func (t *bPlusTree) getLeafNode(key K) *BNode {
	node := t.root
	for !node.isLeaf {
		i := findInsertionIndex(node.keys, key)
		node = node.children[i]
	}
	return node
}

func (t *bPlusTree) getLeftmostLeafNode() *BNode {
	// node := t.root
	// for !node.isLeaf {
	// 	node = node.children[0]
	// }
	// return node

	return findLeftmostLeaf(t.root)
}

func findLeftmostLeaf(n *BNode) *BNode {
	if n == nil {
		return nil
	}
	if n.isLeaf {
		return n
	}
	return findLeftmostLeaf(n.children[0])
}

func (t *bPlusTree) splitLeafNode(node *BNode) {
	mid := node.numKeys / 2

	newNode := &BNode{
		keys:    make([]K, t.maxKeys),
		values:  make([]V, t.maxKeys),
		isLeaf:  true,
		numKeys: 0,
		parent:  node.parent,
	}
	newNode.keys = append(newNode.keys, node.keys[mid:]...)
	newNode.values = append(newNode.values, node.values[mid:]...)
	newNode.numKeys = len(newNode.keys)

	node.keys = node.keys[:mid]
	node.values = node.values[:mid]
	node.numKeys = len(node.keys)

	if node.parent == nil {
		t.root = &BNode{
			keys:     []K{newNode.keys[0]},
			isLeaf:   false,
			numKeys:  1,
			children: []*BNode{node, newNode},
		}
		node.parent = t.root
		newNode.parent = t.root
	} else {
		parent := node.parent
		parentKey := newNode.keys[0]

		i := findInsertionIndex(parent.keys, parentKey)
		parent.keys = insertKeyElement(parent.keys, i, parentKey)
		parent.children = insertChildElement(parent.children, i+1, newNode)
		parent.numKeys++

		newNode.parent = parent

		if parent.numKeys == t.maxKeys {
			t.splitNonLeafNode(parent)
		}
	}

}

func (t *bPlusTree) splitNonLeafNode(node *BNode) {
	mid := node.numKeys / 2

	newNode := &BNode{keys: make([]K, t.maxKeys), isLeaf: false, numKeys: 0, parent: node.parent}
	newNode.keys = append(newNode.keys, node.keys[mid+1:]...)
	newNode.numKeys = len(newNode.keys)

	node.keys = node.keys[:mid]
	node.numKeys = len(node.keys)

	for i := mid + 1; i <= node.numKeys+1; i++ {
		node.children[i].parent = newNode
		newNode.children = append(newNode.children, node.children[i])
	}

	node.children = node.children[:mid+1]
	node.numKeys = len(node.keys)

	if node.parent == nil {
		t.root = &BNode{keys: []K{node.keys[mid]}, isLeaf: false, numKeys: 1, children: []*BNode{node, newNode}}
		node.parent = t.root
		newNode.parent = t.root
	} else {
		parent := node.parent
		parentKey := node.keys[mid]

		i := findInsertionIndex(parent.keys, parentKey)
		parent.keys = insertKeyElement(parent.keys, i, parentKey)
		parent.children = insertChildElement(parent.children, i+1, newNode)
		parent.numKeys++

		newNode.parent = parent

		if parent.numKeys == t.maxKeys {
			t.splitNonLeafNode(parent)
		}
	}
}

func findInsertionIndex(keys []K, key K) int {
	i := 0
	for i < len(keys) && lessThan(keys[i], key) {
		i++
	}
	return i
}

func insertKeyElement(keys []K, index int, key K) []K {
	keys = append(keys, key)
	copy(keys[index+1:], keys[index:])
	keys[index] = key
	return keys
}

func insertValueElement(values []V, index int, value V) []V {
	values = append(values, value)
	copy(values[index+1:], values[index:])
	values[index] = value
	return values
}

func insertChildElement(children []*BNode, index int, child *BNode) []*BNode {
	children = append(children, child)
	copy(children[index+1:], children[index:])
	children[index] = child
	return children
}

func insertElement(slice []interface{}, index int, value interface{}) []interface{} {
	slice = append(slice, nil)
	copy(slice[index+1:], slice[index:])
	slice[index] = value
	return slice
}
