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
	"sync"
	"time"
)

type Wal struct {
	f    *os.File
	path string
	sync.Mutex
}

// Load 通过 wal.log 文件初始化 Wal, 加载文件中的 WalF 到内存
func (w *Wal) Load(dir string) *memTable.SL {
	log.Println("Start loading wal.log...")
	start := time.Now()
	defer func() {
		log.Println("Finish Loading wal.log, consumption of time:", time.Since(start))
	}()

	walPath := path.Join(dir, "wal.log")
	f, err := os.OpenFile(walPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Panicln("Can not open The wal.log file:", err)
	}
	w.f = f
	w.path = walPath

	w.Lock()
	defer w.Unlock()

	info, _ := os.Stat(w.path)
	size := info.Size()
	t := memTable.NewSL()

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
			log.Println("Failed to open the wal.log")
			panic(err)
		}
	}(w.f, size-1, 0)

	// 将文件内容全部读取到内存
	data := make([]byte, size)
	if _, err = w.f.Read(data); err != nil {
		log.Panicln("Can not read the wal.log:", err)
	}

	dataLen := int64(0) // 元素的字节数量
	index := int64(0)   // 当前索引
	for index < size {
		// 前面的 8 个字节表示元素的长度
		indexData := data[index : index+8]
		// 获取元素的字节长度
		buf := bytes.NewBuffer(indexData)
		if err = binary.Read(buf, binary.LittleEndian, &dataLen); err != nil {
			log.Panicln("Can not read for dataLen:", err)
		}
		// 将元素的所有字节读取出来，并还原为 kv.Value
		index += 8
		var value kv.Value
		dataArea := data[index:(index + dataLen)]
		if err = json.Unmarshal(dataArea, &value); err != nil {
			log.Panicln("Can not unmarshal the data:", err)
		}
		if value.Deleted {
			t.Delete(value.Key)
		} else {
			t.Set(value.Key, value.Value)
		}
		index = index + dataLen
	}
	return t
}

func (w *Wal) Write(value kv.Value) {
	w.Lock()
	defer w.Unlock()

	if value.Deleted {
		log.Println("wal.log: delete ", value.Key)
	} else {
		log.Println("wal.log: insert ", value.Key)
	}

	data, _ := json.Marshal(value)
	err := binary.Write(w.f, binary.LittleEndian, int64(len(data)))
	if err != nil {
		log.Panicln("Failed to write the wal.log: " + err.Error())
	}

	err = binary.Write(w.f, binary.LittleEndian, data)
	if err != nil {
		log.Panicln("Failed to write the wal.log: " + err.Error())
	}
}

func (w *Wal) Reset() {
	w.Lock()
	defer w.Unlock()

	log.Println("Start resetting the wal.log file")

	if err := w.f.Close(); err != nil {
		log.Panicln(err)
	}
	w.f = nil

	err := os.Remove(w.path)
	if err != nil {
		log.Panicln(err)
	}

	f, err := os.OpenFile(w.path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Panicln(err)
	}
	w.f = f
}
