package pgxb_test

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/jackc/pgx/v4"
	"github.com/kr/pretty"
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
		pretty.Log(string(resB), string(snapshotB))
		t.Fail()
	}
}

func withTx(fn func(ctx context.Context, tx pgx.Tx)) {
	ctx := context.Background()
	db, err := pgx.Connect(ctx, "user=postgres dbname=pgmg sslmode=disable")
	if err != nil {
		panic(err)
	}
	tx, err := db.Begin(ctx)
	if err != nil {
		panic(err)
	}
	defer tx.Rollback(ctx)
	fn(ctx, tx)
}

func assertDeepEqual(t *testing.T, val interface{}, expected interface{}) {
	if !reflect.DeepEqual(val, expected) {
		t.Fatalf("Expected value to equal to %+v but got %+v", expected, val)
	}
}

func assert(t *testing.T, shouldBeTrue bool) {
	if !shouldBeTrue {
		t.Fatalf("Expected value to equal to be true but it was not")
	}
}
