package main

import (
	"container/heap"
	"context"
	"regexp"
	"strings"

	"golang.org/x/sync/errgroup"
)

type wordCount struct {
	word  string
	count int
}

var nonAlpha = regexp.MustCompile("[^a-zA-Z]+")

// mapFn 将输入的每一行转换成 wordCount 列表
func mapFn(line string) []wordCount {
	var result []wordCount
	for _, w := range strings.Fields(line) {
		// 通过正则替换掉掉非字母字符，并转换成小写
		w = strings.ToLower(nonAlpha.ReplaceAllString(w, ""))
		if w != "" {
			result = append(result, wordCount{word: w, count: 1})
		}
	}
	return result
}

// mapper 将输入的每一行转换成 wordCount 流
func mapper(ctx context.Context, eg *errgroup.Group, input <-chan string, fn func(string) []wordCount) <-chan wordCount {
	ch := make(chan wordCount)

	eg.Go(func() error {
		defer func() { close(ch); logger.Debug("mapper exits") }()
		for l := range input {
			for _, wc := range fn(l) {
				logger.Debug("mapFn outputs", "word", wc.word, "count", wc.count)
				select {
				case ch <- wc:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}
		return nil
	})

	return ch
}

// sorter 将 wordCount 流中数据按照 word 排序
func sorter(ctx context.Context, eg *errgroup.Group, input <-chan wordCount) <-chan wordCount {
	ch := make(chan wordCount)

	eg.Go(func() error {
		defer func() { close(ch); logger.Debug("sorter exits") }()
		wcHeap := new(wordCountHeap)
		for wc := range input {
			logger.Debug("sorter got map output", "word", wc.word, "count", wc.count)
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				heap.Push(wcHeap, wc)
			}
		}

		for wcHeap.Len() > 0 {
			select {
			case ch <- heap.Pop(wcHeap).(wordCount):
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		return nil
	})

	return ch
}

// reducer 将排序后的 wordCount 流中相同 word 的数据合并，计算出每个 word 的总数
func reducer(ctx context.Context, eg *errgroup.Group, input <-chan wordCount) <-chan wordCount {
	ch := make(chan wordCount)

	eg.Go(func() error {
		defer func() { close(ch); logger.Debug("reducer exits") }()
		var wc wordCount
		for in := range input {
			logger.Debug("reducer got sorted output", "word", in.word, "count", in.count)
			if wc.word != in.word {
				if wc.word != "" {
					select {
					case ch <- wc:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				wc = in
			}
			wc.count += in.count
		}
		return nil
	})

	return ch
}
