package main

type wordCountHeap []wordCount

func (w *wordCountHeap) Len() int {
	return len(*w)
}

func (w *wordCountHeap) Less(i int, j int) bool {
	return (*w)[i].word < (*w)[j].word
}
func (w *wordCountHeap) Swap(i int, j int) {
	(*w)[i], (*w)[j] = (*w)[j], (*w)[i]
}

func (w *wordCountHeap) Pop() any {
	v := (*w)[len(*w)-1]
	*w = (*w)[:len(*w)-1]
	return v
}

func (w *wordCountHeap) Push(x any) {
	*w = append(*w, x.(wordCount))
}
