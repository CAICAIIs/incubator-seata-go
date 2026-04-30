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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"seata.apache.org/seata-go/v2/pkg/datasource/sql/types"
)

func TestCreateXAResource_ByDBType(t *testing.T) {
	tests := []struct {
		name      string
		dbType    types.DBType
		wantType  interface{}
		wantError bool
	}{
		{
			name:     "mysql",
			dbType:   types.DBTypeMySQL,
			wantType: &MysqlXAConn{},
		},
		{
			name:     "oracle",
			dbType:   types.DBTypeOracle,
			wantType: &OracleXAConn{},
		},
		{
			name:      "postgresql unsupported",
			dbType:    types.DBTypePostgreSQL,
			wantError: true,
		},
		{
			name:      "unknown",
			dbType:    types.DBTypeUnknown,
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resource, err := CreateXAResource(&recordingXAConn{}, tt.dbType)
			if tt.wantError {
				require.Error(t, err)
				assert.Nil(t, resource)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, resource)
			assert.IsType(t, tt.wantType, resource)
		})
	}
}
