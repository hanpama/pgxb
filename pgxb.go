package pgxb

import (
	"context"
	"time"

	"github.com/jackc/pgx/v4"
)

// BatchSender 인터페이스는 배치를 전송할 수 있다
type BatchSender interface {
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
}

// Batcher 인터페이스는 쿼리를 큐잉하고 일정 갯수나 시간 프레임으로 묶어서 처리한 후 콜백을 호출한다.
type Batcher interface {
	BatchQuery(query string, args []interface{}, callback func(pgx.Rows, error))
	BatchQueryRow(query string, args []interface{}, callback func(pgx.Row, error))
	BatchExec(query string, args []interface{}, callback func(ExecResult, error))
}

// ExecResult represents PostgreSQL execution result
type ExecResult interface {
	RowsAffected() int64
	String() string
}

type batchWorker struct {
	sender   BatchSender
	wait     time.Duration
	maxBatch int
	items    chan batchItem
	done     chan struct{}
	err      error
}

type batchType int8
type batchItem struct {
	query    string
	args     []interface{}
	callBack interface{}
}

type callbackQuery func(pgx.Rows, error)
type callbackQueryRow func(pgx.Row, error)
type callbackExec func(ExecResult, error)

// NewBatchWorker 함수는 새로운 배치 워커를 만든다.
func NewBatchWorker(ctx context.Context, sender BatchSender, maxBatch int, wait time.Duration) Batcher {
	b := &batchWorker{
		sender:   sender,
		wait:     wait,
		maxBatch: maxBatch,
		items:    make(chan batchItem),
		done:     make(chan struct{}),
	}
	errc := make(chan error)
	go func() {
		for {
			select {
			case b.err = <-errc:
				close(b.done)
				return
			default:
				b.work(ctx, errc)
			}
		}
	}()
	return b
}

func (b *batchWorker) work(ctx context.Context, errc chan<- error) {
	timerDone := make(chan struct{})
	var currentItems []batchItem
	batch := &pgx.Batch{}

loop:
	for {
		select {
		case item := <-b.items: // 배치 아이템 채널에서 항목을 전달받음.
			if len(currentItems) == 0 { // 처음 요소가 들어올 때 타이머 작동
				go func() {
					time.Sleep(b.wait)
					close(timerDone)
				}()
			}
			currentItems = append(currentItems, item)
			batch.Queue(item.query, item.args...)
			if len(currentItems) >= b.maxBatch {
				break loop
			}
		case <-timerDone: // 시간이 지나면 실행되고 종료.
			break loop
		case <-ctx.Done(): // 컨텍스트에 의해 종료
			errc <- ctx.Err()
			return
		}
	}

	go func() {
		res := b.sender.SendBatch(ctx, batch)
		for _, item := range currentItems {
			switch callback := item.callBack.(type) {
			case callbackQuery:
				callback(res.Query())
			case callbackQueryRow:
				callback(res.QueryRow(), nil)
			case callbackExec:
				c, err := res.Exec()
				callback(c, err)
			}
		}
		err := res.Close()
		if err != nil {
			select {
			case <-b.done:
			case errc <- err:
			}
		}
	}()
}

func (b *batchWorker) BatchQuery(query string, args []interface{}, callback func(pgx.Rows, error)) {
	select {
	case b.items <- batchItem{query, args, callbackQuery(callback)}:
	case <-b.done:
		callback(nil, b.err)
	}
}
func (b *batchWorker) BatchQueryRow(query string, args []interface{}, callback func(pgx.Row, error)) {
	select {
	case b.items <- batchItem{query, args, callbackQueryRow(callback)}:
	case <-b.done:
		callback(nil, b.err)
	}
}
func (b *batchWorker) BatchExec(query string, args []interface{}, callback func(ExecResult, error)) {
	select {
	case b.items <- batchItem{query, args, callbackExec(callback)}:
	case <-b.done:
		callback(nil, b.err)
	}
}
