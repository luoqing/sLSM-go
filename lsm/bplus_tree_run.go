package lsm

/*
type K struct {
	// Data interface{}  // 如果是interface，文件存储的是指针，数据并不是一直都保留的
	Data int // 使用int也是可以的
}

type V struct {
	// Data interface{}
	Data int
}

type KVPair struct {
	Key   K
	Value V
}*/

type Node struct {
	keys     []K
	values   []V
	children []*Node
	leaf     bool
	next     *Node
	keyNum   int
}

type BPlusTree struct {
	root   *Node
	degree int
}

func NewNode(degree int, leaf bool) *Node {
	return &Node{
		keys:     make([]K, 2*degree-1, 2*degree-1),
		values:   make([]V, 2*degree-1, 2*degree-1),
		children: make([]*Node, 2*degree, 2*degree),
		leaf:     leaf,
	}
}

type BPlusTreeRun struct {
	bTree       *BPlusTree
	numElements int
	min         K
	max         K
}

func NewBPlusTreeRun(degree int, minKey K, maxKey K) *BPlusTreeRun {
	return &BPlusTreeRun{
		bTree: NewBPlusTree(degree),
		min:   minKey,
		max:   maxKey,
	}
}

func (r *BPlusTreeRun) NumElements() int {
	return r.numElements
}

func (r *BPlusTreeRun) InsertKey(key K, value V) {
	r.bTree.insertKey(key, value)
	r.numElements++
}

func (r *BPlusTreeRun) DeleteKey(key K) {
	// r.numElements--
}

func (r *BPlusTreeRun) LookUp(key K) (found bool, value V) {
	return r.bTree.LookUp(key)
}

func (r *BPlusTreeRun) GetAll() []KVPair {
	return r.bTree.GetAll()
}

func NewBPlusTree(degree int) *BPlusTree {
	return &BPlusTree{
		root:   NewNode(degree, true),
		degree: degree,
	}
}

func (t *BPlusTree) InsertKey(key K, value V) {
	t.insertKey(key, value)
}

func (t *BPlusTree) insertKey(key K, value V) {
	if t.root.keyNum == 2*t.degree-1 {
		newRoot := NewNode(t.degree, false)
		newRoot.children[0] = t.root
		newRoot.splitChild(0)

		i := 0
		if newRoot.keys[0].Data < key.Data {
			i++
		}
		newRoot.children[i].insertNonFull(key, value, t.degree)
		t.root = newRoot
	} else {
		t.root.insertNonFull(key, value, t.degree)
	}
}

func (t *BPlusTree) LookUp(key K) (found bool, value V) {
	return t.root.lookUp(key)
}

func (t *BPlusTree) GetAll() []KVPair {
	return t.root.getAllNodeItems()
}

func (n *Node) lookUp(key K) (found bool, value V) {
	i := 0
	for ; i < n.keyNum && key.Data > n.keys[i].Data; i++ {
	}

	// If key is equal to keys[i], return the value
	if i < n.keyNum && key == n.keys[i] {
		return true, n.values[i]
	}

	// If we reached the leaf, then key is not present
	if n.leaf {
		return false, V{}
	}

	// Else, go deeper
	return n.children[i].lookUp(key)
}

func (n *Node) getAllNodeItems() []KVPair {
	items := make([]KVPair, 0)
	if n.leaf {
		for i := 0; i < n.keyNum; i++ {
			items = append(items, KVPair{Key: n.keys[i], Value: n.values[i]})
		}
		return items
	}

	for i := 0; i < n.keyNum+1; i++ {
		items = append(items, n.children[i].getAllNodeItems()...)
	}

	return items
}

func (n *Node) insertNonFull(key K, value V, degree int) {
	i := n.keyNum

	if n.leaf {
		for ; i >= 1 && key.Data < n.keys[i-1].Data; i-- {
			n.keys[i] = n.keys[i-1]
			n.values[i] = n.values[i-1]
		}
		n.keys[i] = key
		n.values[i] = value
		n.keyNum++
	} else {
		for ; i >= 1 && key.Data < n.keys[i-1].Data; i-- {
		}
		if n.children[i].keyNum == 2*degree-1 {
			n.splitChild(i)
			if key.Data > n.keys[i].Data {
				i++
			}
		}
		n.children[i].insertNonFull(key, value, degree)
	}
}

func (n *Node) splitChild(i int) {
	degree := len(n.children)/2 + 1

	z := NewNode(degree, n.children[i].leaf)
	z.keyNum = degree - 1

	// Copy the last (2*t-1) keys and children to the new node
	for j := 0; j < degree-1; j++ {
		z.keys[j] = n.children[i].keys[j+degree]
		z.values[j] = n.children[i].values[j+degree]
	}
	if !n.children[i].leaf {
		for j := 0; j < degree; j++ {
			z.children[j] = n.children[i].children[j+degree]
		}
	}
	n.children[i].keyNum = degree - 1

	// Move the children to create new space
	for j := n.keyNum; j >= i+1; j-- {
		n.children[j+1] = n.children[j]
	}

	// Insert the new child in between
	n.children[i+1] = z

	// Move the keys/values to create new space
	for j := n.keyNum - 1; j >= i; j-- {
		n.keys[j+1] = n.keys[j]
		n.values[j+1] = n.values[j]
	}

	// Copy the median key/value
	n.keys[i] = n.children[i].keys[degree-1]
	n.values[i] = n.children[i].values[degree-1]
	n.keyNum++
}

/*
func main() {
	tree := NewBPlusTree(3) // Degree = 3
	tree.InsertKey(K{1}, V{2})
	tree.InsertKey(K{3}, V{4})
	tree.InsertKey(K{5}, V{6})
	tree.InsertKey(K{7}, V{8})
	tree.InsertKey(K{9}, V{10})

	found, value := tree.LookUp(K{3})
	fmt.Printf("Key 3 found: %t - Value: %d\n", found, value.Data)

	found, value = tree.LookUp(K{10})
	fmt.Printf("Key 10 found: %t - Value: %d\n", found, value.Data)

	all := tree.GetAll()
	fmt.Println("Tree items:")
	for _, kv := range all {
		fmt.Printf("%d -> %d\n", kv.Key.Data, kv.Value.Data)
	}
}*/
