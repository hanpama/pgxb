package pgxb_test

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func testJSONSnapshot(t *testing.T, name string, res interface{}) {
	resB, err := json.Marshal(res)
	if err != nil {
		panic(err)
	}
	path := filepath.Join("__snapshots__", name+".json")
	_, err = os.Stat(path)
	if os.IsNotExist(err) {
		ioutil.WriteFile(path, resB, os.ModePerm)
		t.Logf("Wrote snapshot: %s(%s)", name, path)
		return
	}
	snapshotB, err := ioutil.ReadFile(path)

	if string(resB) != string(snapshotB) {
		println(string(resB))
		t.Fail()
	}
}

func assertDeepEqual(t *testing.T, val interface{}, expected interface{}) {
	if !reflect.DeepEqual(val, expected) {
		t.Fatalf("Expected value to equal to %+v but got %+v", expected, val)
	}
}

func assert(t *testing.T, name string, shouldBeTrue bool) {
	if !shouldBeTrue {
		t.Fatalf("Expected value %s to equal to be true but it was not", name)
	}
}
