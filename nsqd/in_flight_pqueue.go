package nsqd

// 下面是一个小根堆的实现(min heap)，这份代码与标准库的heap大量重复，并且项目中已经有了一个priority queue的实现（internal/pqueue/pqueue.go），
// 不知道这份代码存在的价值在哪里
type inFlightPqueue []*Message

func newInFlightPqueue(capacity int) inFlightPqueue {
	return make(inFlightPqueue, 0, capacity)
}

func (pq inFlightPqueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *inFlightPqueue) Push(x *Message) {
	n := len(*pq)
	c := cap(*pq)
	if n+1 > c {
		npq := make(inFlightPqueue, n, c*2)
		copy(npq, *pq)
		*pq = npq
	}
	*pq = (*pq)[0 : n+1]
	x.index = n
	(*pq)[n] = x
	// 新增节点被放在了最后的叶子上，所以要执行up
	pq.up(n)
}

func (pq *inFlightPqueue) Pop() *Message {
	n := len(*pq)
	c := cap(*pq)
	pq.Swap(0, n-1)
	pq.down(0, n-1)
	if n < (c/2) && c > 25 {
		npq := make(inFlightPqueue, n, c/2)
		copy(npq, *pq)
		*pq = npq
	}
	x := (*pq)[n-1]
	x.index = -1
	*pq = (*pq)[0 : n-1]
	return x
}

func (pq *inFlightPqueue) Remove(i int) *Message {
	n := len(*pq)
	if n-1 != i {
		pq.Swap(i, n-1)
		pq.down(i, n-1)
		pq.up(i)
	}
	x := (*pq)[n-1]
	x.index = -1
	*pq = (*pq)[0 : n-1]
	return x
}

func (pq *inFlightPqueue) PeekAndShift(max int64) (*Message, int64) {
	if len(*pq) == 0 {
		return nil, 0
	}

	x := (*pq)[0]
	// 小根堆，如果根节点大于max，那说明所有节点都大于max，大于说明尚未超时
	if x.pri > max {
		return nil, x.pri - max
	}
	pq.Pop()

	return x, 0
}

/*
if the node index is i, then:

parent: (i - 1) / 2
left child : 2*i  + 1
right child: 2*i + 2

有一种更方便阅读的优化，将数组的第一个元素置为-1，弃用，这样父子节点的关系为：
parent: i/2 (地板除)
left child : 2*i
right child: 2*i + 1

Pop是取出index为1的元素
*/
func (pq *inFlightPqueue) up(j int) {
	for {
		i := (j - 1) / 2 // parent
		// 到了根节点或者父节点的值小于等于子节点（满足小根堆）
		if i == j || (*pq)[j].pri >= (*pq)[i].pri {
			break
		}
		pq.Swap(i, j)
		j = i
	}
}

func (pq *inFlightPqueue) down(i, n int) {
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 { // j1 < 0 after int overflow
			break
		}
		j := j1 // left child
		// 比较left_child和right_child，找出小的那个
		if j2 := j1 + 1; j2 < n && (*pq)[j1].pri >= (*pq)[j2].pri {
			j = j2 // = 2*i + 2  // right child
		}
		if (*pq)[j].pri >= (*pq)[i].pri {
			break
		}
		pq.Swap(i, j)
		i = j
	}
}
