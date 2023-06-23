package ssTable

import (
	"encoding/json"
	"log"
	"os"
	"qlsm/config"
	"qlsm/kv"
	"strconv"
)

func (tt *TablesTree) CreateNewTable(values []kv.Data) {
	tt.createTable(values, 0)
}

func (tt *TablesTree) createTable(values []kv.Data, level int) *SsTable {
	// 生成数据区
	positions := map[string]Position{}
	var dataArea []byte
	for _, value := range values {
		data, err := json.Marshal(value)
		if err != nil {
			log.Println("failed to Insert Key:", value.Key, err)
			continue
		}
		positions[value.Key] = Position{
			Start:   int64(len(dataArea)),
			Len:     int64(len(data)),
			Deleted: value.Deleted,
		}
		dataArea = append(dataArea, data...)
	}

	// 生成稀疏索引区
	indexArea, err := json.Marshal(positions)
	if err != nil {
		log.Fatal("an SsTable file cannot be created,", err)
	}

	meta := MetaInfo{
		version:    0,
		dataStart:  0,
		dataLen:    int64(len(dataArea)),
		indexStart: int64(len(dataArea)),
		indexLen:   int64(len(indexArea)),
	}

	table := &SsTable{
		metaInfo:    meta,
		sparseIndex: positions,
	}

	index := tt.Insert(table, level)
	log.Printf("create a new SsTable, level: %d, index: %d\n", level, index)
	con := config.GetConfig()
	filePath := con.DataDir + "/" + strconv.Itoa(level) + "." + strconv.Itoa(index) + ".db"
	table.filepath = filePath

	writeDataToFile(filePath, dataArea, indexArea, meta)
	// 以只读的形式打开文件
	f, err := os.OpenFile(table.filepath, os.O_RDONLY, 0666)
	if err != nil {
		log.Println("can not open file", table.filepath)
		panic(err)
	}
	table.f = f

	return table
}
