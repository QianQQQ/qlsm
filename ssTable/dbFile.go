package ssTable

import (
	"encoding/binary"
	"log"
	"os"
)

// GetDBSize 获取 .db 数据文件大小
func (t *SsTable) GetDBSize() int64 {
	info, err := os.Stat(t.filepath)
	if err != nil {
		log.Fatal(err)
	}
	return info.Size()
}

// GetLevelSize 获取指定层的 SsTable 总大小
func (tt *TablesTree) GetLevelSize(level int) (size int64) {
	curr := tt.levels[level]
	for curr != nil {
		size += curr.table.GetDBSize()
		curr = curr.next
	}
	return size
}

// 将数据按顺序 <data, sparseIndex, metaInfo> 写入文件
func writeDataToFile(filepath string, dataArea []byte, indexArea []byte, metaInfo MetaInfo) {
	f, err := os.OpenFile(filepath, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		log.Fatal("Can not create file:", err)
	}
	if _, err = f.Write(dataArea); err != nil {
		log.Fatal("Can not write dataArea:", err)
	}
	if _, err = f.Write(indexArea); err != nil {
		log.Fatal("Can not write indexArea:", err)
	}
	_ = binary.Write(f, binary.LittleEndian, &metaInfo.version)
	_ = binary.Write(f, binary.LittleEndian, &metaInfo.dataStart)
	_ = binary.Write(f, binary.LittleEndian, &metaInfo.dataLen)
	_ = binary.Write(f, binary.LittleEndian, &metaInfo.indexStart)
	_ = binary.Write(f, binary.LittleEndian, &metaInfo.indexLen)
	if err = f.Sync(); err != nil {
		log.Fatal("Can not write metaInfo:", err)
	}
	if err = f.Close(); err != nil {
		log.Fatal("Can not close .db:", err)
	}
}
