package pgxb_test

import (
	"context"
	"testing"
	"time"

	"github.com/hanpama/pgxb"
	"github.com/jackc/pgx/v4"
)

func BenchmarkBase(b *testing.B) {
	ctx := context.Background()
	pool := getPool(ctx)
	b.ResetTimer()
	b.ReportAllocs()
	b.SetParallelism(500)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			rows, err := pool.Query(ctx, `SELECT version()`)
			if err != nil {
				panic(err)
			}
			rows.Close()
		}
	})
}

func BenchmarkBatchForPool(b *testing.B) {
	ctx := context.Background()
	pool := getPool(ctx)
	sender := &testSender{sender: pool}
	bw := pgxb.NewBatcher(ctx, sender, 150, 5*time.Microsecond)
	b.ResetTimer()
	b.ReportAllocs()
	b.SetParallelism(500)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			done := make(chan struct{})
			bw.BatchQuery(`SELECT version()`, nil, func(rows pgx.Rows, err error) {
				if err != nil {
					panic(err)
				}
				rows.Close()
				close(done)
			})
			<-done
		}
	})
	b.Log(b.N, sender.sendCount, b.N/sender.sendCount)
}
