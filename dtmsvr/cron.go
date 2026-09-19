/*
 * Copyright (c) 2021 yedf. All rights reserved.
 * Use of this source code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */

package dtmsvr

import (
	"errors"
	"fmt"
	"math/rand"
	"runtime/debug"
	"sync"
	"time"

	"github.com/dtm-labs/dtm/client/dtmcli"
	"github.com/dtm-labs/dtm/client/dtmcli/dtmimp"
	"github.com/dtm-labs/dtm/dtmsvr/storage"
	"github.com/dtm-labs/logger"
)

// NowForwardDuration will be set in test, trans may be timeout
var NowForwardDuration = time.Duration(0)

// CronForwardDuration will be set in test. cron will fetch trans which expire in CronForwardDuration
var CronForwardDuration = time.Duration(0)

// CronTransOnce cron expired trans. use expireIn as expire time
func CronTransOnce() (gid string) {
	defer handlePanic(nil)
	trans := lockOneTrans(CronForwardDuration)
	if trans == nil {
		return
	}
	gid = trans.Gid
	trans.WaitResult = true
	branches := GetStore().FindBranches(gid)
	err := trans.Process(branches)
	dtmimp.PanicIf(err != nil && !errors.Is(err, dtmcli.ErrFailure) && !errors.Is(err, dtmcli.ErrOngoing), err)
	return
}

// CronExpiredTrans cron expired trans, num == -1 indicate for ever
func CronExpiredTrans(num int) {
	for i := 0; i < num || num == -1; i++ {
		gid := CronTransOnce()
		if gid == "" && num != 1 {
			CronOrphanTransOnce()
			sleepCronTime()
		}
	}
}

// OrphanSweepInterval is the minimal interval between two orphan trans sweeps.
// It is a var so tests can shrink it.
var OrphanSweepInterval = 10 * time.Minute

// OrphanTransMinIdle: an unfinished trans whose update_time is older than
// this and whose next_cron_time is already due is treated as an orphan
// (e.g. its async processor panicked) and gets recovered by the sweep.
var OrphanTransMinIdle = 2 * time.Minute

var (
	orphanSweepMu   sync.Mutex
	lastOrphanSweep time.Time
)

// CronOrphanTransOnce detects orphan trans (unfinished, stale update_time,
// overdue next_cron_time) and resets their cron time so the normal cron loop
// can pick them up again. It is rate limited by OrphanSweepInterval to avoid
// scanning the storage too often. Returns the gids of recovered trans.
func CronOrphanTransOnce() (recovered []string) {
	defer handlePanic(nil)
	orphanSweepMu.Lock()
	if time.Since(lastOrphanSweep) < OrphanSweepInterval {
		orphanSweepMu.Unlock()
		return nil
	}
	lastOrphanSweep = time.Now()
	orphanSweepMu.Unlock()

	st := GetStore()
	now := time.Now()
	orphanBefore := now.Add(-OrphanTransMinIdle)
	for _, status := range []string{dtmcli.StatusPrepared, dtmcli.StatusAborting, dtmcli.StatusSubmitted} {
		position := ""
		for {
			globals := st.ScanTransGlobalStores(&position, 100, storage.TransGlobalScanCondition{Status: status})
			for i := range globals {
				g := globals[i]
				if g.UpdateTime == nil || g.NextCronTime == nil ||
					g.UpdateTime.After(orphanBefore) || g.NextCronTime.After(now) {
					continue // still active, or intentionally scheduled in the future (e.g. delay msg)
				}
				err := st.ResetTransGlobalCronTime(&g)
				if err != nil {
					logger.Errorf("recover orphan trans err. gid: %s err: %v", g.Gid, err)
					continue
				}
				logger.Infof("recovered orphan trans. gid: %s status: %s", g.Gid, g.Status)
				recovered = append(recovered, g.Gid)
			}
			if position == "" {
				break
			}
		}
	}
	return
}

// CronUpdateTopicsMap cron updates topics map
func CronUpdateTopicsMap() {
	for {
		time.Sleep(time.Duration(conf.ConfigUpdateInterval) * time.Second)
		CronUpdateTopicsMapOnce()
	}
}

// CronUpdateTopicsMapOnce cron updates topics map once
func CronUpdateTopicsMapOnce() {
	defer handlePanic(nil)
	updateTopicsMap()
}

func lockOneTrans(expireIn time.Duration) *TransGlobal {
	global := GetStore().LockOneGlobalTrans(expireIn)
	if global == nil {
		return nil
	}
	logger.Infof("cron job return a trans: %s", global.String())
	return &TransGlobal{TransGlobalStore: *global}
}

func handlePanic(perr *error) {
	if err := recover(); err != nil {
		logger.Errorf("----recovered panic %v\n%s", err, string(debug.Stack()))
		if perr != nil {
			*perr = fmt.Errorf("dtm panic: %v", err)
		}
	}
}

func sleepCronTime() {
	normal := time.Duration((float64(conf.TransCronInterval) - rand.Float64()) * float64(time.Second))
	interval := dtmimp.If(CronForwardDuration > 0, 1*time.Millisecond, normal).(time.Duration)
	logger.Debugf("sleeping for %v", interval)
	time.Sleep(interval)
}
