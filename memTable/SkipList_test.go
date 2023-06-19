package memTable

import (
	"qlsm/kv"
	"reflect"
	"testing"
)

func Test_SkipList(t *testing.T) {
	sl := NewSL()
	_, hasOld := sl.Set("a", []byte{1, 2, 3})
	if hasOld == true {
		t.Error(hasOld)
	}
	oldKV, hasOld := sl.Set("a", []byte{2, 3, 4})
	if !hasOld {
		t.Error("fail to test the set function, the 'hasOld' should be true")
	}
	if !reflect.DeepEqual(oldKV.Value, []byte{1, 2, 3}) {
		t.Error("fail to test the set function, the 'oldKV' is invalid")
	}

	count := sl.GetCount()
	if count != 1 {
		t.Error(count)
	}

	sl.Set("b", []byte{1, 2, 3})
	sl.Set("c", []byte{1, 2, 3})

	count = sl.GetCount()
	if count != 3 {
		t.Error(count)
	}

	sl.Delete("a")
	sl.Delete("a")

	count = sl.GetCount()
	if count != 3 {
		t.Error(count)
	}

	data, success := sl.Search("a")
	if success != kv.Deleted {
		t.Error(success)
	}

	data, success = sl.Search("b")
	if success != kv.Success {
		t.Error(success)
	}

	if data.Value[0] != 1 {
		t.Error(data)
	}
}
