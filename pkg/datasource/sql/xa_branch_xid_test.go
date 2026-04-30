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
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestXABranchXidBuild(t *testing.T) {
	xid := "111"
	branchId := uint64(222)
	x := XaIdBuild(xid, branchId)
	assert.Equal(t, x.GetGlobalXid(), xid)
	assert.Equal(t, x.GetBranchId(), branchId)

	assert.Equal(t, x.GetGlobalTransactionId(), []byte(xid))
	assert.Equal(t, x.GetBranchQualifier(), []byte("-222"))
}

func TestXABranchXidBuildWithByte(t *testing.T) {
	xid := []byte("111")
	branchId := []byte(branchIdPrefix + "222")
	x := XaIdBuildWithByte(xid, branchId)
	assert.Equal(t, x.GetGlobalTransactionId(), xid)
	assert.Equal(t, x.GetBranchQualifier(), branchId)

	assert.Equal(t, x.GetGlobalXid(), "111")
	assert.Equal(t, x.GetBranchId(), uint64(222))
}

func TestXABranchXid_RoundTripCompositeXID(t *testing.T) {
	globalXid := "127.0.0.1:8091:202604280001"
	branchID := uint64(9876543210)

	built := XaIdBuild(globalXid, branchID)
	rebuilt := XaIdBuildWithByte(built.GetGlobalTransactionId(), built.GetBranchQualifier())

	assert.Equal(t, globalXid, rebuilt.GetGlobalXid())
	assert.Equal(t, branchID, rebuilt.GetBranchId())
	assert.Equal(t, globalXid+"-"+strconv.FormatUint(branchID, 10), rebuilt.String())
}

func TestParseXABranchXid(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantXID   string
		wantBID   uint64
		expectErr bool
	}{
		{
			name:    "valid composite xid",
			input:   "oracle-global-xid-42",
			wantXID: "oracle-global-xid",
			wantBID: 42,
		},
		{
			name:      "missing branch id",
			input:     "oracle-global-xid",
			expectErr: true,
		},
		{
			name:      "invalid branch id",
			input:     "oracle-global-xid-not-a-number",
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			xaXID, err := ParseXABranchXid(tt.input)
			if tt.expectErr {
				assert.Error(t, err)
				assert.Nil(t, xaXID)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.wantXID, xaXID.GetGlobalXid())
			assert.Equal(t, tt.wantBID, xaXID.GetBranchId())
		})
	}
}
