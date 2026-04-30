/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sql

import (
	"context"
	"database/sql/driver"
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"seata.apache.org/seata-go/v2/pkg/datasource/sql/mock"
	"seata.apache.org/seata-go/v2/pkg/datasource/sql/types"
	xares "seata.apache.org/seata-go/v2/pkg/datasource/sql/xa"
	"seata.apache.org/seata-go/v2/pkg/protocol/branch"
	"seata.apache.org/seata-go/v2/pkg/rm"
)

type fakeXAResource struct {
	commitXIDs   []string
	rollbackXIDs []string
	commitErr    error
	rollbackErr  error
}

func (f *fakeXAResource) Commit(_ context.Context, xid string, _ bool) error {
	f.commitXIDs = append(f.commitXIDs, xid)
	return f.commitErr
}

func (f *fakeXAResource) End(context.Context, string, int) error {
	return nil
}

func (f *fakeXAResource) Forget(context.Context, string) error {
	return nil
}

func (f *fakeXAResource) GetTransactionTimeout() time.Duration {
	return 0
}

func (f *fakeXAResource) IsSameRM(context.Context, xares.XAResource) bool {
	return false
}

func (f *fakeXAResource) XAPrepare(context.Context, string) error {
	return nil
}

func (f *fakeXAResource) Recover(context.Context, int) ([]string, error) {
	return nil, nil
}

func (f *fakeXAResource) Rollback(_ context.Context, xid string) error {
	f.rollbackXIDs = append(f.rollbackXIDs, xid)
	return f.rollbackErr
}

func (f *fakeXAResource) SetTransactionTimeout(time.Duration) bool {
	return false
}

func (f *fakeXAResource) Start(context.Context, string, int) error {
	return nil
}

type closeTrackingConn struct {
	closeCalls int
}

func (c *closeTrackingConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("not implemented")
}

func (c *closeTrackingConn) Close() error {
	c.closeCalls++
	return nil
}

func (c *closeTrackingConn) Begin() (driver.Tx, error) {
	return nil, errors.New("not implemented")
}

func TestDBResource_ConnectionForXA_UsesResourceDBType(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockConn := mock.NewMockTestDriverConn(ctrl)
	mockConn.EXPECT().Close().AnyTimes().Return(nil)

	mockConnector := mock.NewMockTestDriverConnector(ctrl)
	mockConnector.EXPECT().Connect(gomock.Any()).Return(mockConn, nil)

	resource := &DBResource{
		connector: mockConnector,
		dbType:    types.DBTypeOracle,
	}

	xaConn, err := resource.ConnectionForXA(context.Background(), XaIdBuild("global-xid", 9))
	require.NoError(t, err)
	require.NotNil(t, xaConn)
	assert.IsType(t, &xares.OracleXAConn{}, xaConn.xaResource)
	assert.Equal(t, "global-xid-9", xaConn.xaBranchXid.String())
}

func TestDBResource_ConnectionForXA_ReturnsHeldConnectionForRetry(t *testing.T) {
	resource := &DBResource{}
	held := &XAConn{}
	xaID := XaIdBuild("held-xid", 7)

	require.NoError(t, resource.Hold(xaID.String(), held))

	got, err := resource.ConnectionForXA(context.Background(), xaID)
	require.NoError(t, err)
	assert.Same(t, held, got)
}

func TestXAResourceManager_BranchCommit_UsesMappedXidAndReleasesHeldConnection(t *testing.T) {
	xaID := XaIdBuild("commit-xid", 21)
	fakeResource := &fakeXAResource{}
	targetConn := &closeTrackingConn{}
	dbResource := &DBResource{
		resourceID:   "resource-commit",
		dbType:       types.DBTypeMySQL,
		shouldBeHeld: true,
	}

	heldConn := &XAConn{
		Conn: &Conn{
			targetConn: targetConn,
			res:        dbResource,
		},
		xaBranchXid: xaID,
		xaResource:  fakeResource,
		isConnKept:  true,
	}
	require.NoError(t, dbResource.Hold(xaID.String(), heldConn))

	manager := &XAResourceManager{}
	manager.resourceCache.Store(dbResource.resourceID, dbResource)

	status, err := manager.BranchCommit(context.Background(), rm.BranchResource{
		ResourceId: dbResource.resourceID,
		Xid:        xaID.GetGlobalXid(),
		BranchId:   int64(xaID.GetBranchId()),
	})
	require.NoError(t, err)
	assert.EqualValues(t, branch.BranchStatusPhasetwoCommitted, status)
	assert.Equal(t, []string{xaID.String()}, fakeResource.commitXIDs)
	_, ok := dbResource.Lookup(xaID.String())
	assert.False(t, ok)
	assert.Equal(t, 1, targetConn.closeCalls)
}

func TestXAResourceManager_BranchRollback_UsesMappedXidAndReleasesHeldConnection(t *testing.T) {
	xaID := XaIdBuild("rollback-xid", 22)
	fakeResource := &fakeXAResource{}
	targetConn := &closeTrackingConn{}
	dbResource := &DBResource{
		resourceID:   "resource-rollback",
		dbType:       types.DBTypeMySQL,
		shouldBeHeld: true,
	}

	heldConn := &XAConn{
		Conn: &Conn{
			targetConn: targetConn,
			res:        dbResource,
		},
		xaBranchXid: xaID,
		xaResource:  fakeResource,
		isConnKept:  true,
	}
	require.NoError(t, dbResource.Hold(xaID.String(), heldConn))

	manager := &XAResourceManager{}
	manager.resourceCache.Store(dbResource.resourceID, dbResource)

	status, err := manager.BranchRollback(context.Background(), rm.BranchResource{
		ResourceId: dbResource.resourceID,
		Xid:        xaID.GetGlobalXid(),
		BranchId:   int64(xaID.GetBranchId()),
	})
	require.NoError(t, err)
	assert.EqualValues(t, branch.BranchStatusPhasetwoRollbacked, status)
	assert.Equal(t, []string{xaID.String()}, fakeResource.rollbackXIDs)
	_, ok := dbResource.Lookup(xaID.String())
	assert.False(t, ok)
	assert.Equal(t, 1, targetConn.closeCalls)
}
