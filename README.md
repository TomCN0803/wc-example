# wc-example

A sample word count (wc) implementation utilizing map-reduce paradigm and Golang errgroup.Group primitive.

## Build and Run

``` shell
go build -o wc
./wc -f article.txt
```

## Usage

```shell
Usage of ./wc:
  -debug
        enable debug mode
  -f string
        specify the input file

```
