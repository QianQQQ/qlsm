package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"qlsm"
	"qlsm/config"
	"time"
)

type TestValue struct {
	A int64
	B int64
	C int64
	D string
}

func main() {
	log.SetFlags(log.LstdFlags | log.Llongfile)
	defer func() {
		r := recover()
		if r != nil {
			fmt.Println(r)
			inputReader := bufio.NewReader(os.Stdin)
			_, _ = inputReader.ReadString('\n')
		}
	}()
	lsm.Start(config.Config{
		DataDir:       `D:\项目\lsm数据测试目录`,
		Level0Size:    100,
		PartSize:      4,
		Threshold:     3000,
		CheckInterval: 3,
	})
	insert()
	queryAll()
}

func query() {
	start := time.Now()
	v, _ := lsm.Get[TestValue]("aaaa")
	elapse := time.Since(start)
	fmt.Println("查找 aaaa 完成，消耗时间：", elapse)
	fmt.Println(v)

	start = time.Now()
	v, _ = lsm.Get[TestValue]("zzzz")
	elapse = time.Since(start)
	fmt.Println("查找 zzzz 完成，消耗时间：", elapse)
	fmt.Println(v)
}

func queryAll() {
	start := time.Now()
	key := []byte{'a', 'a', 'a', 'a'}
	for a := 0; a < 26; a++ {
		for b := 0; b < 26; b++ {
			for c := 0; c < 26; c++ {
				for d := 0; d < 26; d++ {
					key[0] = 'a' + byte(a)
					key[1] = 'a' + byte(b)
					key[2] = 'a' + byte(c)
					key[3] = 'a' + byte(d)
					lsm.Get[TestValue](string(key))
				}
			}
		}
	}
	elapse := time.Since(start)
	fmt.Println("查找完成, 消耗时间：", elapse)
}

func insert() {
	testV := TestValue{1, 2, 3, "abcdefghijklmnopqrstuvwxyz"}
	count := 0
	start := time.Now()
	key := []byte{'a', 'a', 'a', 'a'}
	for a := 0; a < 26; a++ {
		for b := 0; b < 26; b++ {
			for c := 0; c < 26; c++ {
				for d := 0; d < 26; d++ {
					key[0] = 'a' + byte(a)
					key[1] = 'a' + byte(b)
					key[2] = 'a' + byte(c)
					key[3] = 'a' + byte(d)
					lsm.Set(string(key), testV)
					count++
				}
			}
		}
	}
	elapse := time.Since(start)
	fmt.Println("插入完成, 数据量", count, ", 消耗时间", elapse)
}
