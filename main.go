package main

import (
	"bufio"
	"container/heap"
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"syscall"

	"golang.org/x/sync/errgroup"
)

var inputFile string

var (
	logger *slog.Logger
	debug  bool
)

func init() {
	flag.StringVar(&inputFile, "f", "article.txt", "specify the input file")
	flag.BoolVar(&debug, "debug", false, "enable debug mode")
	flag.Parse()
}

func main() {
	if inputFile == "" {
		flag.Usage()
		return
	}

	logger = slog.New(slog.NewTextHandler(os.Stderr, getLoggerOptions()))

	f, err := os.Open(inputFile)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "failed to open file: %s\n", err.Error())
		os.Exit(1)
	}
	defer f.Close()

	// 监听系统信号，当收到 SIGTERM 或 SIGINT 信号时，取消程序执行
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	eg, ctx := errgroup.WithContext(ctx)
	input := getInputStream(ctx, eg, f)
	mapped := mapper(ctx, eg, input, mapFn)
	sorted := sorter(ctx, eg, mapped)
	reduced := reducer(ctx, eg, sorted)

	eg.Go(func() error {
		for wc := range reduced {
			_, _ = fmt.Printf("%-15s%4d\n", wc.word, wc.count)
		}
		return nil
	})

	if err := eg.Wait(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "failed to process file: %s\n", err.Error())
		os.Exit(1)
	}
}

func getLoggerOptions() *slog.HandlerOptions {
	logOpts := &slog.HandlerOptions{
		Level:     slog.LevelInfo,
		AddSource: true,
	}

	if debug {
		logOpts.Level = slog.LevelDebug
	}

	return logOpts
}

// getInputStream 启动一个 goroutine 来读取 r 中的数据，将所读到的数据发送到返回的 channel 中。
func getInputStream(ctx context.Context, eg *errgroup.Group, r io.Reader) <-chan string {
	ch := make(chan string)

	eg.Go(func() error {
		defer func() { close(ch); slog.Debug("all text hash been read") }()
		sc := bufio.NewScanner(r)
		for sc.Scan() {
			line := sc.Text()
			logger.Debug("read line", "line", line)
			select {
			case ch <- line:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return sc.Err()
	})

	return ch
}

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
