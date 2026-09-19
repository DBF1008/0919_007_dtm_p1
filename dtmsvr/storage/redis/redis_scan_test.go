/*
 * Copyright (c) 2022 yedf. All rights reserved.
 * Use of this source code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */

package redis

import (
	"fmt"
	"testing"
	"time"

	"github.com/dtm-labs/dtm/dtmsvr/storage"
	"github.com/stretchr/testify/assert"
)

// TestScanTransGlobalStoresServerFilter requires a redis at 127.0.0.1:6379.
// It verifies that ScanTransGlobalStores filters on the server side and
// respects the limit, instead of MGET-ing every key to the client.
func TestScanTransGlobalStoresServerFilter(t *testing.T) {
	conf.Store.Host = "127.0.0.1"
	conf.Store.Port = 6379
	conf.Store.User = ""
	conf.Store.Password = ""
	conf.Store.RedisPrefix = "{scan_test}"
	conf.Store.DataExpire = 3600
	conf.Store.FinishedDataExpire = 3600

	s := &Store{}
	s.PopulateData(false) // flushall, the test redis is dedicated

	now := time.Now().Truncate(time.Second)
	next := now.Add(time.Hour)
	mk := func(gid string, status string, transType string, created time.Time) {
		g := &storage.TransGlobalStore{
			Gid:          gid,
			Status:       status,
			TransType:    transType,
			NextCronTime: &next,
		}
		g.CreateTime = &created
		g.UpdateTime = &created
		err := s.MaySaveNewTrans(g, nil)
		assert.Nil(t, err)
	}
	// 30 saga/submitted + 30 msg/submitted + 30 saga/succeed
	for i := 0; i < 30; i++ {
		created := now.Add(time.Duration(i) * time.Second)
		mk(fmt.Sprintf("st-saga-sub-%02d", i), "submitted", "saga", created)
		mk(fmt.Sprintf("st-msg-sub-%02d", i), "submitted", "msg", created)
		mk(fmt.Sprintf("st-saga-ok-%02d", i), "succeed", "saga", created)
	}

	// limit is respected and more pages are available
	position := ""
	globals := s.ScanTransGlobalStores(&position, 10, storage.TransGlobalScanCondition{Status: "submitted"})
	assert.Equal(t, 10, len(globals))
	assert.NotEqual(t, "", position)
	for _, g := range globals {
		assert.Equal(t, "submitted", g.Status)
	}

	// paginate through all submitted trans, even though limit < matching count
	position = ""
	seen := map[string]bool{}
	for {
		globals = s.ScanTransGlobalStores(&position, 7, storage.TransGlobalScanCondition{Status: "submitted"})
		for _, g := range globals {
			assert.Equal(t, "submitted", g.Status)
			assert.False(t, seen[g.Gid], "duplicate gid: %s", g.Gid)
			seen[g.Gid] = true
		}
		if position == "" {
			break
		}
	}
	assert.Equal(t, 60, len(seen))

	// trans_type filter is applied on the server
	position = ""
	globals = s.ScanTransGlobalStores(&position, 100, storage.TransGlobalScanCondition{
		Status: "submitted", TransType: "saga",
	})
	assert.Equal(t, 30, len(globals))
	assert.Equal(t, "", position)
	for _, g := range globals {
		assert.Equal(t, "saga", g.TransType)
	}

	// create time range filter is applied on the server (strictly between)
	position = ""
	globals = s.ScanTransGlobalStores(&position, 100, storage.TransGlobalScanCondition{
		Status:          "submitted",
		TransType:       "saga",
		CreateTimeStart: now.Add(10*time.Second + 500*time.Millisecond),
		CreateTimeEnd:   now.Add(20*time.Second + 500*time.Millisecond),
	})
	assert.Equal(t, 10, len(globals)) // i = 11..20
	for _, g := range globals {
		assert.True(t, g.CreateTime.After(now.Add(10*time.Second+500*time.Millisecond)))
		assert.True(t, g.CreateTime.Before(now.Add(20*time.Second+500*time.Millisecond)))
	}

	// no match: empty result and exhausted position
	position = ""
	globals = s.ScanTransGlobalStores(&position, 10, storage.TransGlobalScanCondition{Status: "aborting"})
	assert.Equal(t, 0, len(globals))
	assert.Equal(t, "", position)
}

// TestTransScanPositionCodec needs no redis: it checks the opaque pagination
// cursor round-trips, including pending keys and the exhausted marker.
func TestTransScanPositionCodec(t *testing.T) {
	pos := transScanPosition{}
	assert.Equal(t, "", pos.String()) // zero value means exhausted/fresh start

	pos.Cursor = 123
	parsed := parseTransScanPosition(pos.String())
	assert.Equal(t, uint64(123), parsed.Cursor)
	assert.Empty(t, parsed.Pending)

	pos.Cursor = 0
	pos.Pending = []string{"{a}_g_gid1", "{a}_g_gid2"}
	s := pos.String()
	assert.NotEqual(t, "", s) // pending keys keep the position non-empty
	parsed = parseTransScanPosition(s)
	assert.Equal(t, uint64(0), parsed.Cursor)
	assert.Equal(t, []string{"{a}_g_gid1", "{a}_g_gid2"}, parsed.Pending)

	parsed = parseTransScanPosition("")
	assert.Equal(t, uint64(0), parsed.Cursor)
	assert.Empty(t, parsed.Pending)
}
