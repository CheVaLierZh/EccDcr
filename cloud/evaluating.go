package main

import (
	"EccDcr/pb"
	"context"
	"google.golang.org/protobuf/types/known/emptypb"
	"log"
	"math"
	"sync"
)

type EvaluateManager struct {
	epsilon float64
	nSampling int
}

func (c *Coordinator) MakeEvaluateManager(epsilon float64) *EvaluateManager {
	return &EvaluateManager{epsilon: epsilon, nSampling: int(math.Ceil(8.0 / (epsilon * epsilon)))}
}

func (c *Coordinator) Evaluate(epsilon float64) {
	c.em = c.MakeEvaluateManager(epsilon)

	var wg sync.WaitGroup

	partSize := make([]int, len(c.followers))
	for i := 0; i < len(c.followers); i++ {
		wg.Add(1)
		go func(j int) {
			reply, _ := c.followers[j].TableSize(context.Background(), &emptypb.Empty{})
			partSize[j] = int(reply.Size)
			wg.Done()
		} (i)
	}
	wg.Wait()
	log.Println("finish collecting table size...")

	sampleSize := make([]int, len(c.followers))
	total := 0
	for _, v := range partSize {
		total += v
	}
	acc := 0
	for i := 0; i < len(c.followers) - 1; i++ {
		sampleSize[i] = int(math.Ceil(float64(partSize[i]) / float64(total) * float64(c.em.nSampling)))
		acc += sampleSize[i]
	}
	sampleSize[len(sampleSize) - 1] = total - acc

	for i := 0; i < len(c.followers); i++ {
		wg.Add(1)
		go func(j int) {
			c.followers[j].Sampling(context.Background(), &pb.SizeMessage{Size: int32(sampleSize[j])})
			wg.Done()
		} (i)
	}
	wg.Wait()
	log.Println("finish table sampling...")

	c.ConflictDetect()
	c.Repair(false)

	cnt := c.rm.vc.Size()
	log.Println("inconsistency degree:", float64(cnt) / float64(c.em.nSampling) + c.em.epsilon / 2)
}
