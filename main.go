package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"golang.org/x/sync/errgroup"
)

var inputFile string

var (
	logger *slog.Logger
	debug  bool
)

func init() {
	flag.StringVar(&inputFile, "f", "", "specify the input file")
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
	eg.SetLimit(runtime.GOMAXPROCS(0)) // 设置 goroutine 数量为 CPU 核心数
	input := getInputStream(ctx, eg, f)
	mapped := mapper(ctx, eg, input, mapFn)
	sorted := sorter(ctx, eg, mapped)
	reduced := reducer(ctx, eg, sorted)

	eg.Go(func() error {
		for wc := range reduced {
			if _, err := fmt.Printf("%-15s%4d\n", wc.word, wc.count); err != nil {
				return err
			}
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
