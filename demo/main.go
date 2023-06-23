package main

import (
	"log"
	lsm "qlsm"
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
	//defer func() {
	//	if r := recover(); r != nil {
	//		log.Println(r)
	//		inputReader := bufio.NewReader(os.Stdin)
	//		_, _ = inputReader.ReadString('\n')
	//	}
	//}()
	lsm.Start(config.Config{
		DataDir:       `D:\项目\lsm数据测试目录`,
		Level0Size:    100,
		PartSize:      4,
		Threshold:     10000,
		CheckInterval: 1,
	})
	insert()
	queryAll()
	//queryAbsent()
	deleteAll()
	//deleteAbsent()
	//queryAll()
}

func insert() {
	testV := TestValue{1, 2, 3, "abcdefghijklmnopqrstuvwxyz"}
	count := 0
	start := time.Now()
	defer func() { log.Println("finish insert, data count:", count, ", time consumption", time.Since(start)) }()
	key := []byte{'a', 'a', 'a', 'a', 'a'}
	for a := 0; a < 26; a++ {
		for b := 0; b < 26; b++ {
			for c := 0; c < 26; c++ {
				for d := 0; d < 26; d++ {
					//for e := 0; e < 26; e++ {
					key[0] = 'a' + byte(a)
					key[1] = 'a' + byte(b)
					key[2] = 'a' + byte(c)
					key[3] = 'a' + byte(d)
					//key[4] = 'a' + byte(e)
					lsm.Set[TestValue](string(key), testV)
					count++
					//}
				}
			}
		}
	}
}

func queryAll() {
	start := time.Now()
	defer func() { log.Println("finish queryAll, time consumption", time.Since(start)) }()
	key := []byte{'a', 'a', 'a', 'a', 'a'}
	for a := 0; a < 26; a++ {
		for b := 0; b < 26; b++ {
			for c := 0; c < 26; c++ {
				for d := 0; d < 26; d++ {
					//for e := 0; e < 26; e++ {
					key[0] = 'a' + byte(a)
					key[1] = 'a' + byte(b)
					key[2] = 'a' + byte(c)
					key[3] = 'a' + byte(d)
					//key[4] = 'a' + byte(e)
					lsm.Get[TestValue](string(key))
					//}
				}
			}
		}
	}
}

func queryAbsent() {
	start := time.Now()
	defer func() { log.Println("finish queryAbsent, time consumption", time.Since(start)) }()
	v, ok := lsm.Get[TestValue]("abcdefg")
	log.Println("data is exist?", ok, ", the default value is", v)
}

func deleteAll() {
	start := time.Now()
	defer func() { log.Println("finish deleteAll, time consumption", time.Since(start)) }()
	key := []byte{'a', 'a', 'a', 'a', 'a'}
	for a := 0; a < 26; a++ {
		for b := 0; b < 26; b++ {
			for c := 0; c < 26; c++ {
				for d := 0; d < 26; d++ {
					//for e := 0; e < 26; e++ {
					key[0] = 'a' + byte(a)
					key[1] = 'a' + byte(b)
					key[2] = 'a' + byte(c)
					key[3] = 'a' + byte(d)
					//key[4] = 'a' + byte(e)
					lsm.Delete(string(key))
					//}
				}
			}
		}
	}
}

func deleteAbsent() {
	start := time.Now()
	defer func() { log.Println("finish deleteAbsent, time consumption", time.Since(start)) }()
	lsm.Delete("abcdefg")
}
