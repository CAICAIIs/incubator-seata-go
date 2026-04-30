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

package xa

import (
	"context"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const oracleBranchXID = "oracle-global-xid-9"

func TestOracleXAConn_Commit(t *testing.T) {
	tests := []struct {
		name     string
		onePhase bool
		wantPart string
	}{
		{
			name:     "two phase",
			wantPart: "DBMS_XA.XA_COMMIT(l_xid, FALSE)",
		},
		{
			name:     "one phase",
			onePhase: true,
			wantPart: "DBMS_XA.XA_COMMIT(l_xid, TRUE)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn := &recordingXAConn{}
			c := NewOracleXaConn(conn)

			require.NoError(t, c.Commit(context.Background(), oracleBranchXID, tt.onePhase))
			require.Len(t, conn.execQueries, 1)
			assert.Contains(t, conn.execQueries[0], tt.wantPart)
			assert.Contains(t, conn.execQueries[0], "DBMS_XA_XID("+strconv.Itoa(oracleXAFormatID)+",")
			assert.Contains(t, conn.execQueries[0], "DBMS_XA.XA_COMMIT:ret=")
		})
	}
}

func TestOracleXAConn_Start(t *testing.T) {
	tests := []struct {
		name     string
		flag     int
		wantPart string
		wantErr  string
	}{
		{
			name:     "no flags",
			flag:     TMNoFlags,
			wantPart: "DBMS_XA.XA_START(l_xid, DBMS_XA.TMNOFLAGS)",
		},
		{
			name:     "join",
			flag:     TMJoin,
			wantPart: "DBMS_XA.XA_START(l_xid, DBMS_XA.TMJOIN)",
		},
		{
			name:     "resume",
			flag:     TMResume,
			wantPart: "DBMS_XA.XA_START(l_xid, DBMS_XA.TMRESUME)",
		},
		{
			name:    "invalid flag",
			flag:    TMSuspend,
			wantErr: "invalid arguments",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn := &recordingXAConn{}
			c := NewOracleXaConn(conn)

			err := c.Start(context.Background(), oracleBranchXID, tt.flag)
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				assert.Empty(t, conn.execQueries)
				return
			}

			require.NoError(t, err)
			require.Len(t, conn.execQueries, 1)
			assert.Contains(t, conn.execQueries[0], tt.wantPart)
		})
	}
}

func TestOracleXAConn_End(t *testing.T) {
	tests := []struct {
		name     string
		flag     int
		wantPart string
		wantErr  string
	}{
		{
			name:     "success",
			flag:     TMSuccess,
			wantPart: "DBMS_XA.XA_END(l_xid, DBMS_XA.TMSUCCESS)",
		},
		{
			name:     "suspend",
			flag:     TMSuspend,
			wantPart: "DBMS_XA.XA_END(l_xid, DBMS_XA.TMSUSPEND)",
		},
		{
			name:     "tmfail maps to success",
			flag:     TMFail,
			wantPart: "DBMS_XA.XA_END(l_xid, DBMS_XA.TMSUCCESS)",
		},
		{
			name:    "invalid flag",
			flag:    TMJoin,
			wantErr: "invalid arguments",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn := &recordingXAConn{}
			c := NewOracleXaConn(conn)

			err := c.End(context.Background(), oracleBranchXID, tt.flag)
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				assert.Empty(t, conn.execQueries)
				return
			}

			require.NoError(t, err)
			require.Len(t, conn.execQueries, 1)
			assert.Contains(t, conn.execQueries[0], tt.wantPart)
		})
	}
}

func TestOracleXAConn_PrepareRollbackForgetRecover(t *testing.T) {
	t.Run("prepare uses readonly success mapping", func(t *testing.T) {
		conn := &recordingXAConn{}
		c := NewOracleXaConn(conn)

		require.NoError(t, c.XAPrepare(context.Background(), oracleBranchXID))
		require.Len(t, conn.execQueries, 1)
		assert.Contains(t, conn.execQueries[0], "DBMS_XA.XA_PREPARE(l_xid)")
		assert.Contains(t, conn.execQueries[0], "IF l_ret NOT IN (0, 3)")
	})

	t.Run("rollback treats nota as idempotent", func(t *testing.T) {
		conn := &recordingXAConn{
			execErr: errors.New("ORA-20000: DBMS_XA.XA_ROLLBACK:ret=-4,oer=24756"),
		}
		c := NewOracleXaConn(conn)

		require.NoError(t, c.Rollback(context.Background(), oracleBranchXID))
		require.Len(t, conn.execQueries, 1)
		assert.Contains(t, conn.execQueries[0], "DBMS_XA.XA_ROLLBACK(l_xid)")
	})

	t.Run("forget treats nota as idempotent", func(t *testing.T) {
		conn := &recordingXAConn{
			execErr: errors.New("ORA-20000: DBMS_XA.XA_FORGET:ret=-4,oer=24756"),
		}
		c := NewOracleXaConn(conn)

		require.NoError(t, c.Forget(context.Background(), oracleBranchXID))
		require.Len(t, conn.execQueries, 1)
		assert.Contains(t, conn.execQueries[0], "DBMS_XA.XA_FORGET(l_xid)")
	})

	t.Run("recover decodes xid", func(t *testing.T) {
		globalXidRaw := []byte("oracle-global-xid")
		branchQualifierRaw := []byte("-88")
		conn := &recordingXAConn{
			queryRows: &stubXARows{
				data: [][]driver.Value{
					{
						oracleXAFormatID,
						strings.ToUpper(hex.EncodeToString(globalXidRaw)),
						strings.ToUpper(hex.EncodeToString(branchQualifierRaw)),
					},
				},
			},
		}
		c := NewOracleXaConn(conn)

		xids, err := c.Recover(context.Background(), TMStartRScan|TMEndRScan)
		require.NoError(t, err)
		assert.Equal(t, []string{"oracle-global-xid-88"}, xids)
		assert.Equal(t, []string{"SELECT x.formatid, RAWTOHEX(x.gtrid), RAWTOHEX(x.bqual) FROM TABLE(DBMS_XA.XA_RECOVER()) x"}, conn.queryQueries)
	})

	t.Run("recover skips foreign format id", func(t *testing.T) {
		conn := &recordingXAConn{
			queryRows: &stubXARows{
				data: [][]driver.Value{
					{
						oracleXAFormatID + 1,
						strings.ToUpper(hex.EncodeToString([]byte("foreign-xid"))),
						strings.ToUpper(hex.EncodeToString([]byte("not-a-seata-branch"))),
					},
					{
						oracleXAFormatID,
						strings.ToUpper(hex.EncodeToString([]byte("oracle-global-xid"))),
						strings.ToUpper(hex.EncodeToString([]byte("-89"))),
					},
				},
			},
		}
		c := NewOracleXaConn(conn)

		xids, err := c.Recover(context.Background(), TMStartRScan)
		require.NoError(t, err)
		assert.Equal(t, []string{"oracle-global-xid-89"}, xids)
	})

	t.Run("recover rejects invalid seata branch qualifier", func(t *testing.T) {
		conn := &recordingXAConn{
			queryRows: &stubXARows{
				data: [][]driver.Value{
					{
						oracleXAFormatID,
						strings.ToUpper(hex.EncodeToString([]byte("oracle-global-xid"))),
						strings.ToUpper(hex.EncodeToString([]byte("not-a-branch-id"))),
					},
				},
			},
		}
		c := NewOracleXaConn(conn)

		xids, err := c.Recover(context.Background(), TMStartRScan)
		require.EqualError(t, err, "the protocol of oracle XA RECOVER statement is error")
		assert.Nil(t, xids)
	})

	t.Run("recover invalid flag", func(t *testing.T) {
		conn := &recordingXAConn{}
		c := NewOracleXaConn(conn)

		xids, err := c.Recover(context.Background(), TMFail)
		require.EqualError(t, err, "invalid arguments")
		assert.Nil(t, xids)
		assert.Empty(t, conn.queryQueries)
	})

	t.Run("recover end scan is no-op", func(t *testing.T) {
		conn := &recordingXAConn{}
		c := NewOracleXaConn(conn)

		xids, err := c.Recover(context.Background(), TMEndRScan)
		require.NoError(t, err)
		assert.Nil(t, xids)
		assert.Empty(t, conn.queryQueries)
	})
}

func TestOracleXAConn_TimeoutAndValidation(t *testing.T) {
	t.Run("default timeout", func(t *testing.T) {
		assert.Equal(t, oracleXADefaultTimeout, NewOracleXaConn(&recordingXAConn{}).GetTransactionTimeout())
	})

	t.Run("set timeout", func(t *testing.T) {
		conn := &recordingXAConn{}
		c := NewOracleXaConn(conn)

		assert.True(t, c.SetTransactionTimeout(1500*time.Millisecond))
		assert.Equal(t, 2*time.Second, c.GetTransactionTimeout())
		require.Len(t, conn.execQueries, 1)
		assert.Contains(t, conn.execQueries[0], "DBMS_XA.XA_SETTIMEOUT(2)")
	})

	t.Run("negative timeout rejected", func(t *testing.T) {
		conn := &recordingXAConn{}
		c := NewOracleXaConn(conn)

		assert.False(t, c.SetTransactionTimeout(-time.Second))
		assert.Empty(t, conn.execQueries)
	})

	t.Run("invalid xid format", func(t *testing.T) {
		c := NewOracleXaConn(&recordingXAConn{})
		err := c.Commit(context.Background(), "invalid-xid", false)
		require.EqualError(t, err, "invalid xa branch xid: invalid-xid")
	})

	t.Run("xid part too long", func(t *testing.T) {
		c := NewOracleXaConn(&recordingXAConn{})
		globalXid := strings.Repeat("g", oracleXAMaxXIDPartSize+1)
		err := c.Start(context.Background(), globalXid+"-1", TMNoFlags)
		require.EqualError(t, err, "oracle xa gtrid exceeds 64 bytes")
	})
}

func TestOracleXAConn_PropagatesErrors(t *testing.T) {
	boom := errors.New("boom")
	conn := &recordingXAConn{execErr: boom}
	c := NewOracleXaConn(conn)

	assert.ErrorIs(t, c.Commit(context.Background(), oracleBranchXID, false), boom)
	require.Len(t, conn.execQueries, 1)
	assert.Contains(t, conn.execQueries[0], "DBMS_XA.XA_COMMIT(l_xid, FALSE)")
}
