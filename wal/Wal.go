package wal

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"log"
	"os"
	"path"
	"qlsm/kv"
	"qlsm/memTable"
	"qlsm/memTable/skiplist"
	"sync"
	"time"
)

type Wal struct {
	f    *os.File
	path string
	sync.Mutex
}

// Load 通过 wal.log 文件初始化 Wal, 加载文件中的 WalF 到内存
func (w *Wal) Load(dir string) memTable.MemTable {
	log.Println("start loading wal.log...")
	start := time.Now()
	defer func() {
		log.Println("finish Loading wal.log, consumption of time:", time.Since(start))
	}()

	walPath := path.Join(dir, "wal.log")
	f, err := os.OpenFile(walPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Panicln("can not open The wal.log file:", err)
	}
	w.f = f
	w.path = walPath

	w.Lock()
	defer w.Unlock()

	info, _ := os.Stat(w.path)
	size := info.Size()
	t := skiplist.New()

	if size == 0 {
		return t
	}

	if _, err = w.f.Seek(0, 0); err != nil {
		log.Panicln("can not open the wal.log:", err)
	}

	// 文件指针移动到最后，以便追加
	defer func(f *os.File, offset int64, whence int) {
		_, err = f.Seek(offset, whence)
		if err != nil {
			log.Println("failed to open the wal.log")
			panic(err)
		}
	}(w.f, size-1, 0)

	// 将文件内容全部读取到内存
	data := make([]byte, size)
	if _, err = w.f.Read(data); err != nil {
		log.Panicln("can not read the wal.log:", err)
	}

	dataLen := int64(0) // 元素的字节数量
	index := int64(0)   // 当前索引
	for index < size {
		// 获取元素的字节长度
		dataLenArea := data[index : index+8]
		buf := bytes.NewBuffer(dataLenArea)
		if err = binary.Read(buf, binary.LittleEndian, &dataLen); err != nil {
			log.Panicln("can not read for dataLen:", err)
		}
		// 将元素的所有字节读取出来，并还原为 kv.Data
		index += 8
		var value kv.Data
		dataArea := data[index:(index + dataLen)]
		if err = json.Unmarshal(dataArea, &value); err != nil {
			log.Panicln("can not unmarshal the data:", err)
		}
		if value.Deleted {
			t.Delete(value.Key)
		} else {
			t.Set(value.Key, value.Value)
		}
		index += dataLen
	}
	return t
}

func (w *Wal) Write(value kv.Data) {
	w.Lock()
	defer w.Unlock()

	if value.Deleted {
		//log.Println("wal.log: delete", value.Key)
	} else {
		//log.Println("wal.log: insert", value.Key)
	}

	data, _ := json.Marshal(value)
	if err := binary.Write(w.f, binary.LittleEndian, int64(len(data))); err != nil {
		log.Panicln("failed to write the wal.log: " + err.Error())
	}

	if err := binary.Write(w.f, binary.LittleEndian, data); err != nil {
		log.Panicln("failed to write the wal.log: " + err.Error())
	}
}

func (w *Wal) Reset() {
	w.Lock()
	defer w.Unlock()

	log.Println("start resetting the wal.log...")
	if err := w.f.Close(); err != nil {
		log.Panicln(err)
	}
	w.f = nil

	if err := os.Remove(w.path); err != nil {
		log.Panicln(err)
	}

	f, err := os.OpenFile(w.path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Panicln(err)
	}
	w.f = f
}
