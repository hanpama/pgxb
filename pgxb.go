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

// Batchable 인터페이스는 배치 처리될 수 있는 작업이다.
type Batchable interface {
	// Queue 함수는 주어진 배치에 1개의 쿼리를 Queue 한다.
	Queue(*pgx.Batch)
	// Done 함수에서는 주어진 결과를 1회 읽는다.
	Done(pgx.BatchResults, error)
}

// Batcher 인터페이스는 쿼리를 큐잉하고 일정 갯수나 시간 프레임으로 묶어서 처리한 후
// Batchable의 Done 메서드를 결과와 함께 호출한다.
type Batcher interface {
	// Batch 메서드는 주어진 작업을 큐에 넣는다.
	Batch(Batchable)
}

type batchWorker struct {
	sender   BatchSender
	wait     time.Duration
	maxBatch int
	items    chan Batchable
	closed   chan struct{}
	err      error
}

// NewBatchWorker 함수는 새로운 배치 워커를 만든다.
func NewBatchWorker(ctx context.Context, sender BatchSender, maxBatch int, wait time.Duration) Batcher {
	b := &batchWorker{
		sender:   sender,
		wait:     wait,
		maxBatch: maxBatch,
		// 버퍼드 채널의 길이는 최대 배치 갯수만큼 되어야 한다.
		// 버퍼가 꽉 차있는 경우에는 현재 배치 처리 이후에 push 가능하다.
		// 기다리던 중 배처가 닫히는 경우에는 push 단계에서 오류.
		items: make(chan Batchable, maxBatch),
		// 이 배치 워커가 종료(오류임)되면 닫힌다.
		closed: make(chan struct{}),
	}
	go func() {
		for {
			// 고루틴 하나가 큐를 비우면서 작업한다.
			b.err = b.work(ctx)
			if b.err != nil {
				close(b.closed) // Push 기다리던 고루틴들을 unblock
				return          // 이 고루틴도 종료
			}
		}
	}()
	return b
}

func (b *batchWorker) work(ctx context.Context) error {
	timerDone := make(chan struct{})
	var currentItems []Batchable
	batch := &pgx.Batch{}

loop:
	for {
		select {
		case item := <-b.items: // 배치 아이템 채널에서 항목을 전달받음.
			if len(currentItems) == 0 { // 처음 요소가 들어올 때 타이머 작동
				go func() {
					time.Sleep(b.wait)
					timerDone <- struct{}{}
				}()
			}
			currentItems = append(currentItems, item)
			item.Queue(batch)
			if len(currentItems) >= b.maxBatch {
				break loop
			}
		case <-timerDone: // 시간이 지나면 실행되고 종료.
			break loop
		case <-ctx.Done(): // 컨텍스트에 의해 종료
			return ctx.Err()
		}
	}

	res := b.sender.SendBatch(ctx, batch)
	for _, item := range currentItems {
		item.Done(res, nil)
	}

	return res.Close()
}

func (b *batchWorker) Batch(item Batchable) {
	select {
	case b.items <- item:
	case <-b.closed:
		item.Done(nil, b.err)
	}
}
