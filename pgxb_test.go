package pgxb_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/hanpama/pgxb"
	"github.com/jackc/pgx/v4"
)

func TestPGXB(t *testing.T) {
	withTx(func(ctx context.Context, tx pgx.Tx) {
		_, err := tx.Exec(ctx, `
		  CREATE TEMPORARY TABLE todo (
				id SERIAL PRIMARY KEY,
				content TEXT NOT NULL,
				done TIMESTAMPTZ
			)
		`)
		if err != nil {
			panic(err)
		}
		s := &testSender{tx, 0}

		b := pgxb.NewBatchWorker(ctx, s, 50, 16*time.Millisecond)

		var wg sync.WaitGroup
		for i := 0; i < 120; i++ {
			wg.Add(1)

			args := []interface{}{fmt.Sprintf("My todo %d", i)}
			go func() {
				b.BatchExec(`INSERT INTO todo (content, done) VALUES ($1, null)`, args,
					func(res pgxb.ExecResult, err error) {
						if err != nil {
							t.Fatal(err)
							panic(err)
						}
						wg.Done()
					},
				)
			}()
		}
		wg.Wait()

		assert(t, 3 <= s.sendCount)
		assert(t, s.sendCount < 120)
		t.Logf("Sent %d batches", s.sendCount)

		for i := 0; i < 120; i++ {
			wg.Add(1)
			id := i + 1
			args := []interface{}{id}
			go func() {
				b.BatchQueryRow(`SELECT id FROM todo WHERE id = $1`, args, func(row pgx.Row, err error) {
					wg.Done()
					if err != nil {
						panic(err)
					}
					var resultID int
					err = row.Scan(&resultID)

					if err != nil {
						t.Fatal(err)
					}
					assertDeepEqual(t, resultID, id)
				})
			}()
		}
		wg.Wait()
		t.Logf("Sent %d batches", s.sendCount)
	})
}

type testSender struct {
	tx        pgx.Tx
	sendCount int
}

func (s *testSender) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	s.sendCount = s.sendCount + 1
	return s.tx.SendBatch(ctx, b)
}
