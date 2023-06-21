package kv

type SearchResult int

const (
	None = iota
	Deleted
	Success
)

// Data 表示一个 KV
type Data struct {
	Key     string
	Value   []byte
	Deleted bool
}

func (d *Data) Copy() *Data {
	return &Data{
		Key:     d.Key,
		Value:   d.Value,
		Deleted: d.Deleted,
	}
}
