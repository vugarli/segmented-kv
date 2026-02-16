package store

import (
	"context"
	"testing"
	"time"
)

type Fake struct {
	calledOn time.Time
}

func (f *Fake) Merge(strat MergeCandidateRetrievalStrat) error {
	f.calledOn = time.Now()
	return nil
}

func fakeStrat(kd KeyDir) map[int][]MergeEntryRecord {
	return nil
}

func TestPeriodicMergeSchedule(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	start := time.Now()
	fake := Fake{}
	duration := 2 * time.Second

	c := PeriodicMerge(ctx, duration)
	c(&fake, fakeStrat)

	time.Sleep(2500 * time.Millisecond)

	actualDuration := fake.calledOn.Sub(start)
	delta := 100 * time.Millisecond

	if actualDuration < duration || actualDuration > duration+delta {
		t.Errorf("expected call  %v, but got %v", duration, actualDuration)
	}
}
