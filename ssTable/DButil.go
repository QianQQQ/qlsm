package ssTable

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
)

// 获取 db 对应的 level 和 index 信息
func getSsTableInfo(filename string) (level int, index int, err error) {
	n, err := fmt.Sscanf(filename, "%d.%d.db", &level, &index)
	if n != 2 || err != nil {
		return 0, 0, fmt.Errorf("incorrect data file filename for SsTable: %q", filename)
	}
	return level, index, nil
}

// 获取 db 数据文件大小
func (t *SsTable) getDBSize() int64 {
	info, err := os.Stat(t.filepath)
	if err != nil {
		log.Fatal(err)
	}
	return info.Size()
}

// 获取指定层的 SsTable 总文件大小
func (tt *TablesTree) getLevelSize(level int) (size int64) {
	curr := tt.levels[level]
	for curr != nil {
		size += curr.table.getDBSize()
		curr = curr.next
	}
	return size
}

// 将数据按顺序 <data, sparseIndex, metaInfo> 写入 db 文件
func writeDataToFile(filepath string, dataArea []byte, indexArea []byte, metaInfo MetaInfo) {
	f, err := os.OpenFile(filepath, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		log.Fatal("can not create file:", err)
	}
	if _, err = f.Write(dataArea); err != nil {
		log.Fatal("can not write dataArea:", err)
	}
	if _, err = f.Write(indexArea); err != nil {
		log.Fatal("can not write indexArea:", err)
	}
	_ = binary.Write(f, binary.LittleEndian, &metaInfo.version)
	_ = binary.Write(f, binary.LittleEndian, &metaInfo.dataStart)
	_ = binary.Write(f, binary.LittleEndian, &metaInfo.dataLen)
	_ = binary.Write(f, binary.LittleEndian, &metaInfo.indexStart)
	_ = binary.Write(f, binary.LittleEndian, &metaInfo.indexLen)
	if err = f.Sync(); err != nil {
		log.Fatal("can not write metaInfo:", err)
	}
	if err = f.Close(); err != nil {
		log.Fatal("can not close .db:", err)
	}
}
