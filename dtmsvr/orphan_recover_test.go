/*
 * Copyright (c) 2022 yedf. All rights reserved.
 * Use of this source code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */

package dtmsvr

import (
	"testing"
	"time"

	"github.com/dtm-labs/dtm/client/dtmcli"
	"github.com/dtm-labs/dtm/dtmsvr/storage"
	"github.com/stretchr/testify/assert"
)

// setupOrphanTestStore makes the dtmsvr package tests run against an
// embedded boltdb file, no external service needed.
func setupOrphanTestStore() {
	conf.Store.Driver = "boltdb"
	conf.Store.DataExpire = 604800
	conf.RetryInterval = 10
}

func saveUnfinishedTrans(t *testing.T, gid string, status string, updateTime time.Time, nextCronTime time.Time) {
	g := &storage.TransGlobalStore{
		Gid:          gid,
		TransType:    "saga",
		Status:       status,
		NextCronTime: &nextCronTime,
	}
	g.CreateTime = &updateTime
	g.UpdateTime = &updateTime
	err := GetStore().MaySaveNewTrans(g, []storage.TransBranchStore{{Gid: gid, BranchID: "01"}})
	assert.Nil(t, err)
}

func TestOrphanTransRecovery(t *testing.T) {
	setupOrphanTestStore()
	oldSweep, oldIdle := OrphanSweepInterval, OrphanTransMinIdle
	OrphanSweepInterval = 0
	OrphanTransMinIdle = time.Minute
	defer func() { OrphanSweepInterval, OrphanTransMinIdle = oldSweep, oldIdle }()

	stale := time.Now().Add(-time.Hour)
	orphan := "orphan-recover-" + GenGid()
	saveUnfinishedTrans(t, orphan, dtmcli.StatusSubmitted, stale, stale)

	// a delayed trans scheduled in the future must not be recovered
	delayed := "orphan-delayed-" + GenGid()
	future := time.Now().Add(time.Hour)
	saveUnfinishedTrans(t, delayed, dtmcli.StatusSubmitted, stale, future)

	recovered := CronOrphanTransOnce()
	assert.Contains(t, recovered, orphan)
	assert.NotContains(t, recovered, delayed)

	// the orphan cron time is reset to ~now, so the cron loop can lock it
	g := GetStore().FindTransGlobalStore(orphan)
	assert.NotNil(t, g)
	assert.True(t, g.NextCronTime.Before(time.Now().Add(time.Minute)),
		"orphan next_cron_time should be reset to now, got: %v", g.NextCronTime)

	// the delayed trans keeps its future cron time
	g2 := GetStore().FindTransGlobalStore(delayed)
	assert.NotNil(t, g2)
	assert.True(t, g2.NextCronTime.After(time.Now().Add(30*time.Minute)),
		"delayed trans must not be touched, got: %v", g2.NextCronTime)
}

func TestCronOrphanTransOnceRateLimited(t *testing.T) {
	setupOrphanTestStore()
	oldSweep, oldIdle, oldLast := OrphanSweepInterval, OrphanTransMinIdle, lastOrphanSweep
	OrphanSweepInterval = time.Hour
	OrphanTransMinIdle = time.Minute
	lastOrphanSweep = time.Time{}
	defer func() { OrphanSweepInterval, OrphanTransMinIdle, lastOrphanSweep = oldSweep, oldIdle, oldLast }()

	stale := time.Now().Add(-time.Hour)
	gid1 := "orphan-rl1-" + GenGid()
	saveUnfinishedTrans(t, gid1, dtmcli.StatusSubmitted, stale, stale)
	recovered := CronOrphanTransOnce()
	assert.Contains(t, recovered, gid1)

	// the second call within OrphanSweepInterval does nothing
	gid2 := "orphan-rl2-" + GenGid()
	saveUnfinishedTrans(t, gid2, dtmcli.StatusSubmitted, stale, stale)
	recovered = CronOrphanTransOnce()
	assert.Empty(t, recovered)
	g := GetStore().FindTransGlobalStore(gid2)
	assert.NotNil(t, g)
	assert.WithinDuration(t, stale, *g.NextCronTime, time.Second,
		"rate limited sweep must not reset cron time")
}

func TestRescheduleAfterPanic(t *testing.T) {
	setupOrphanTestStore()
	gid := "orphan-panic-" + GenGid()
	now := time.Now()
	future := now.Add(time.Hour) // cron would not pick it up for an hour
	g := &storage.TransGlobalStore{
		Gid:          gid,
		TransType:    "unknown-type", // no processor registered: ProcessOnce panics
		Status:       dtmcli.StatusSubmitted,
		NextCronTime: &future,
	}
	g.CreateTime = &now
	g.UpdateTime = &now
	err := GetStore().MaySaveNewTrans(g, []storage.TransBranchStore{{Gid: gid, BranchID: "01"}})
	assert.Nil(t, err)

	tg := &TransGlobal{TransGlobalStore: *g}
	tg.WaitResult = false
	err = tg.process(nil) // async: goroutine panics, handlePanic recovers, trans rescheduled
	assert.Nil(t, err)

	// wait until the panic handler reschedules the trans (next_cron_time ~= now)
	deadline := time.Now().Add(5 * time.Second)
	for {
		stored := GetStore().FindTransGlobalStore(gid)
		if stored != nil && stored.NextCronTime != nil && stored.NextCronTime.Before(time.Now().Add(time.Minute)) {
			break
		}
		assert.True(t, time.Now().Before(deadline), "trans was not rescheduled after async panic")
		time.Sleep(20 * time.Millisecond)
	}
	stored := GetStore().FindTransGlobalStore(gid)
	assert.Equal(t, dtmcli.StatusSubmitted, stored.Status)
}
