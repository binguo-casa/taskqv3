package taskq_test

import (
	"context"
	"sync"
	"testing"

	"github.com/vmihailenco/taskq/v3"
	"github.com/vmihailenco/taskq/v3/memqueue"
	"github.com/vmihailenco/taskq/v3/redisq"
)

func BenchmarkConsumerMemq(b *testing.B) {
	benchmarkConsumer(b, memqueue.NewFactory(), false)
}

func BenchmarkConsumerRedisqScript(b *testing.B) {
	benchmarkConsumer(b, redisq.NewFactory(), true)
}
func BenchmarkConsumerRedisqNoScript(b *testing.B) {
	benchmarkConsumer(b, redisq.NewFactory(), false)
}

var (
	once sync.Once
	q    taskq.Queue
	task *taskq.Task
	wg   sync.WaitGroup
)

func benchmarkConsumer(b *testing.B, factory taskq.Factory, script bool) {
	c := context.Background()

	once.Do(func() {
		q = factory.RegisterQueue(&taskq.QueueOptions{
			Name:                      "bench",
			Redis:                     redisRing(),
			SchedulerDelayedUseScript: &script,
		})

		task = taskq.RegisterTask(&taskq.TaskOptions{
			Name: "bench",
			Handler: func() {
				wg.Done()
			},
		})

		_ = q.Consumer().Start(c)
	})

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for j := 0; j < 100; j++ {
			wg.Add(1)
			_ = q.Add(task.WithArgs(c))
		}
		wg.Wait()
	}
}
