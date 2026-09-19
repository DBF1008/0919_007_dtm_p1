package test

import (
	"fmt"
	"testing"
	"time"

	"github.com/dtm-labs/dtm/client/dtmcli/dtmimp"
	"github.com/dtm-labs/dtm/dtmsvr/storage"
	"github.com/dtm-labs/dtm/dtmsvr/storage/registry"
	"github.com/dtm-labs/dtm/dtmutil"
	"github.com/stretchr/testify/assert"
)

func initTransGlobal(gid string) (*storage.TransGlobalStore, storage.Store) {
	next := time.Now().Add(10 * time.Second)
	return initTransGlobalByNextCronTime(gid, next)
}

func initTransGlobalByNextCronTime(gid string, next time.Time) (*storage.TransGlobalStore, storage.Store) {
	g := &storage.TransGlobalStore{Gid: gid, Status: "prepared", NextCronTime: &next}
	bs := []storage.TransBranchStore{
		{Gid: gid, BranchID: "01"},
	}
	s := registry.GetStore()
	err := s.MaySaveNewTrans(g, bs)
	dtmimp.E2P(err)
	return g, s
}

func TestStoreSave(t *testing.T) {
	gid := dtmimp.GetFuncName()
	bs := []storage.TransBranchStore{
		{Gid: gid, BranchID: "01"},
		{Gid: gid, BranchID: "02"},
	}
	g, s := initTransGlobal(gid)
	g2 := s.FindTransGlobalStore(gid)
	assert.NotNil(t, g2)
	assert.Equal(t, gid, g2.Gid)

	bs2 := s.FindBranches(gid)
	assert.Equal(t, len(bs2), int(1))
	assert.Equal(t, "01", bs2[0].BranchID)

	s.LockGlobalSaveBranches(gid, g.Status, []storage.TransBranchStore{bs[1]}, -1)
	bs3 := s.FindBranches(gid)
	assert.Equal(t, 2, len(bs3))
	assert.Equal(t, "02", bs3[1].BranchID)
	assert.Equal(t, "01", bs3[0].BranchID)

	err := dtmimp.CatchP(func() {
		s.LockGlobalSaveBranches(g.Gid, "submitted", []storage.TransBranchStore{bs[1]}, 1)
	})
	assert.Equal(t, storage.ErrNotFound, err)

	s.ChangeGlobalStatus(g, "succeed", []string{}, true)
}

func TestStoreChangeStatus(t *testing.T) {
	gid := dtmimp.GetFuncName()
	g, s := initTransGlobal(gid)
	g.Status = "no"
	err := dtmimp.CatchP(func() {
		s.ChangeGlobalStatus(g, "submitted", []string{}, false)
	})
	assert.Equal(t, storage.ErrNotFound, err)
	g.Status = "prepared"
	s.ChangeGlobalStatus(g, "submitted", []string{}, false)
	s.ChangeGlobalStatus(g, "succeed", []string{}, true)
}

func TestStoreLockTrans(t *testing.T) {
	// lock trans will only lock unfinished trans. ensure all other trans are finished
	gid := dtmimp.GetFuncName()
	g, s := initTransGlobal(gid)

	g2 := s.LockOneGlobalTrans(2 * time.Duration(conf.RetryInterval) * time.Second)
	assert.NotNil(t, g2)
	assert.Equal(t, gid, g2.Gid)

	s.TouchCronTime(g, 3*conf.RetryInterval, dtmutil.GetNextTime(3*conf.RetryInterval))
	g2 = s.LockOneGlobalTrans(2 * time.Duration(conf.RetryInterval) * time.Second)
	assert.Nil(t, g2)

	s.TouchCronTime(g, 1*conf.RetryInterval, dtmutil.GetNextTime(1*conf.RetryInterval))
	g2 = s.LockOneGlobalTrans(2 * time.Duration(conf.RetryInterval) * time.Second)
	assert.NotNil(t, g2)
	assert.Equal(t, gid, g2.Gid)

	s.ChangeGlobalStatus(g, "succeed", []string{}, true)
	g2 = s.LockOneGlobalTrans(2 * time.Duration(conf.RetryInterval) * time.Second)
	assert.Nil(t, g2)
}

func TestStoreResetCronTime(t *testing.T) {
	s := registry.GetStore()
	testStoreResetCronTime(t, dtmimp.GetFuncName(), func(timeout int64, limit int64) (int64, bool, error) {
		return s.ResetCronTime(time.Duration(timeout)*time.Second, limit)
	})
}

func testStoreResetCronTime(t *testing.T, funcName string, resetCronHandler func(expire int64, limit int64) (int64, bool, error)) {
	s := registry.GetStore()
	var afterSeconds, lockExpireIn, limit, i int64
	afterSeconds = 100
	lockExpireIn = 2
	limit = 10

	// Will be reset
	for i = 0; i < limit; i++ {
		gid := funcName + fmt.Sprintf("%d", i)
		_, _ = initTransGlobalByNextCronTime(gid, time.Now().Add(time.Duration(afterSeconds+10)*time.Second))
	}

	// Will not be reset
	gid := funcName + fmt.Sprintf("%d", 10)
	_, _ = initTransGlobalByNextCronTime(gid, time.Now().Add(time.Duration(afterSeconds-10)*time.Second))

	// Not Found
	g := s.LockOneGlobalTrans(time.Duration(lockExpireIn) * time.Second)
	assert.Nil(t, g)

	// Reset limit-1 count
	succeedCount, hasRemaining, err := resetCronHandler(afterSeconds, limit-1)
	assert.Equal(t, hasRemaining, true)
	assert.Equal(t, succeedCount, limit-1)
	assert.Nil(t, err)
	// Found limit-1 count
	for i = 0; i < limit-1; i++ {
		g = s.LockOneGlobalTrans(time.Duration(lockExpireIn) * time.Second)
		assert.NotNil(t, g)
		s.ChangeGlobalStatus(g, "succeed", []string{}, true)
	}

	// Not Found
	g = s.LockOneGlobalTrans(time.Duration(lockExpireIn) * time.Second)
	assert.Nil(t, g)

	// Reset 1 count
	succeedCount, hasRemaining, err = resetCronHandler(afterSeconds, limit)
	assert.Equal(t, hasRemaining, false)
	assert.Equal(t, succeedCount, int64(1))
	assert.Nil(t, err)
	// Found 1 count
	g = s.LockOneGlobalTrans(time.Duration(lockExpireIn) * time.Second)
	assert.NotNil(t, g)
	s.ChangeGlobalStatus(g, "succeed", []string{}, true)

	// Not Found
	g = s.LockOneGlobalTrans(time.Duration(lockExpireIn) * time.Second)
	assert.Nil(t, g)

	// reduce the resetTimeTimeout, Reset 1 count
	succeedCount, hasRemaining, err = resetCronHandler(afterSeconds-12, limit)
	assert.Equal(t, hasRemaining, false)
	assert.Equal(t, succeedCount, int64(1))
	assert.Nil(t, err)
	// Found 1 count
	g = s.LockOneGlobalTrans(time.Duration(lockExpireIn) * time.Second)
	assert.NotNil(t, g)
	s.ChangeGlobalStatus(g, "succeed", []string{}, true)

	// Not Found
	g = s.LockOneGlobalTrans(time.Duration(lockExpireIn) * time.Second)
	assert.Nil(t, g)

	// Not Found
	succeedCount, hasRemaining, err = resetCronHandler(afterSeconds-12, limit)
	assert.Equal(t, hasRemaining, false)
	assert.Equal(t, succeedCount, int64(0))
	assert.Nil(t, err)
}

func TestUpdateBranches(t *testing.T) {
	if !conf.Store.IsDB() {
		_, err := registry.GetStore().UpdateBranches(nil, nil)
		assert.Nil(t, err)
	}
}

func TestResetTransGlobalCronTime(t *testing.T) {
	gid := dtmimp.GetFuncName()
	g, _ := initTransGlobal(gid)

	s := registry.GetStore()
	g2 := s.FindTransGlobalStore(gid)
	assert.NotNil(t, g2)
	assert.Equal(t, gid, g2.Gid)

	s.ResetTransGlobalCronTime(g2)

	g2 = s.FindTransGlobalStore(gid)
	assert.NotNil(t, g2)
	assert.Equal(t, gid, g2.Gid)
	assert.Greater(t, time.Now().Add(3*time.Second), *g2.NextCronTime)
	assert.Equal(t, g2.UpdateTime, g2.NextCronTime)
	assert.NotEqual(t, g.UpdateTime, g2.UpdateTime)
	assert.NotEqual(t, g.NextCronTime, g2.NextCronTime)
	s.ChangeGlobalStatus(g, "succeed", []string{}, true)
}

func TestScanTransGlobalStores(t *testing.T) {
	s := registry.GetStore()
	prefix := dtmimp.GetFuncName()
	transType := prefix // unique trans type isolates this test's data
	now := time.Now().Truncate(time.Second)
	next := now.Add(time.Hour)
	mkTrans := func(i int, status string) {
		gid := fmt.Sprintf("%s-%02d", prefix, i)
		created := now.Add(time.Duration(i) * time.Second)
		g := &storage.TransGlobalStore{
			Gid:          gid,
			Status:       status,
			TransType:    transType,
			NextCronTime: &next,
		}
		g.CreateTime = &created
		g.UpdateTime = &created
		err := s.MaySaveNewTrans(g, []storage.TransBranchStore{{Gid: gid, BranchID: "01"}})
		assert.Nil(t, err)
	}
	// 5 submitted (i=0..4) + 5 succeed (i=5..9)
	for i := 0; i < 5; i++ {
		mkTrans(i, "submitted")
		mkTrans(i+5, "succeed")
	}

	// scan all of this test's trans
	position := ""
	globals := s.ScanTransGlobalStores(&position, 100, storage.TransGlobalScanCondition{TransType: transType})
	assert.Equal(t, 10, len(globals))
	assert.Equal(t, "", position)

	// filter by status on the storage side
	position = ""
	globals = s.ScanTransGlobalStores(&position, 100, storage.TransGlobalScanCondition{
		TransType: transType, Status: "submitted",
	})
	assert.Equal(t, 5, len(globals))
	for _, g := range globals {
		assert.Equal(t, "submitted", g.Status)
		assert.Equal(t, transType, g.TransType)
	}

	// limit is respected; paginate until the position is exhausted
	position = ""
	seen := map[string]bool{}
	pages := 0
	for {
		globals = s.ScanTransGlobalStores(&position, 4, storage.TransGlobalScanCondition{TransType: transType})
		assert.LessOrEqual(t, len(globals), 4)
		for _, g := range globals {
			assert.False(t, seen[g.Gid], "duplicate gid: %s", g.Gid)
			seen[g.Gid] = true
		}
		pages++
		if position == "" {
			break
		}
	}
	assert.Equal(t, 10, len(seen))
	assert.Equal(t, 3, pages) // 4 + 4 + 2

	// filter by create time range: i in 3..7 (boundaries at half seconds)
	position = ""
	globals = s.ScanTransGlobalStores(&position, 100, storage.TransGlobalScanCondition{
		TransType:       transType,
		CreateTimeStart: now.Add(2*time.Second + 500*time.Millisecond),
		CreateTimeEnd:   now.Add(7*time.Second + 500*time.Millisecond),
	})
	assert.Equal(t, 5, len(globals))
	for _, g := range globals {
		assert.True(t, g.CreateTime.After(now.Add(2*time.Second+500*time.Millisecond)))
		assert.True(t, g.CreateTime.Before(now.Add(7*time.Second+500*time.Millisecond)))
	}

	// no match
	position = ""
	globals = s.ScanTransGlobalStores(&position, 10, storage.TransGlobalScanCondition{
		TransType: transType, Status: "aborting",
	})
	assert.Equal(t, 0, len(globals))
	assert.Equal(t, "", position)
}
