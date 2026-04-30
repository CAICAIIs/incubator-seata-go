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
	"errors"
	"io"
)

type recordingXAConn struct {
	execQueries  []string
	queryQueries []string
	execErr      error
	queryErr     error
	queryRows    driver.Rows
}

func (c *recordingXAConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("not implemented")
}

func (c *recordingXAConn) Close() error {
	return nil
}

func (c *recordingXAConn) Begin() (driver.Tx, error) {
	return nil, errors.New("not implemented")
}

func (c *recordingXAConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	c.execQueries = append(c.execQueries, query)
	if c.execErr != nil {
		return nil, c.execErr
	}
	return driver.ResultNoRows, nil
}

func (c *recordingXAConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	c.queryQueries = append(c.queryQueries, query)
	if c.queryErr != nil {
		return nil, c.queryErr
	}
	if c.queryRows != nil {
		return c.queryRows, nil
	}
	return &stubXARows{}, nil
}

type stubXARows struct {
	idx     int
	data    [][]driver.Value
	nextErr error
}

func (r *stubXARows) Columns() []string {
	return nil
}

func (r *stubXARows) Close() error {
	return nil
}

func (r *stubXARows) Next(dest []driver.Value) error {
	if r.nextErr != nil {
		err := r.nextErr
		r.nextErr = nil
		return err
	}
	if r.idx >= len(r.data) {
		return io.EOF
	}
	copy(dest, r.data[r.idx])
	r.idx++
	return nil
}
