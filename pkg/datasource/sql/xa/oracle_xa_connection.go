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
	"fmt"
	"io"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"seata.apache.org/seata-go/v2/pkg/util/log"
)

const (
	oracleXADefaultTimeout = 60 * time.Second
	oracleXAMaxXIDPartSize = 64
	oracleXAFormatID       = 0x53474F

	oracleXAOK       = 0
	oracleXAReadonly = 3
	oracleXARetry    = 4
	oracleXAHeurMix  = 5
	oracleXAHeurRB   = 6
	oracleXAHeurCom  = 7
	oracleXAHeurHaz  = 8

	oracleXAERRMErr   = -3
	oracleXAERNota    = -4
	oracleXAERInval   = -5
	oracleXAERProto   = -6
	oracleXAERRMFail  = -7
	oracleXAERDupID   = -8
	oracleXAEROutside = -9
)

var (
	_ XAResource = (*OracleXAConn)(nil)

	oracleXAResultPattern      = regexp.MustCompile(`DBMS_XA\.(XA_[A-Z_]+):ret=(-?\d+),oer=(-?\d+)`)
	errOracleXARecoverProtocol = errors.New("the protocol of oracle XA RECOVER statement is error")
)

type OracleXAConn struct {
	driver.Conn
	timeout time.Duration
}

type oracleXID struct {
	formatID            int
	globalTransactionID []byte
	branchQualifier     []byte
}

type oracleXAError struct {
	operation  string
	resultCode int
	oracleCode int
	cause      error
}

func (e *oracleXAError) Error() string {
	return fmt.Sprintf("oracle %s returned %d (oer=%d): %v", e.operation, e.resultCode, e.oracleCode, e.cause)
}

func (e *oracleXAError) Unwrap() error {
	return e.cause
}

func NewOracleXaConn(conn driver.Conn) *OracleXAConn {
	return &OracleXAConn{
		Conn:    conn,
		timeout: oracleXADefaultTimeout,
	}
}

func (c *OracleXAConn) Commit(ctx context.Context, xid string, onePhase bool) error {
	xaXID, err := oracleXABranchXid(xid)
	if err != nil {
		return err
	}

	plsql := oracleXAWithXidBlock(
		"XA_COMMIT",
		xaXID,
		fmt.Sprintf("DBMS_XA.XA_COMMIT(l_xid, %s)", oracleBooleanLiteral(onePhase)),
		oracleXAOK,
	)
	return c.exec(ctx, plsql)
}

func (c *OracleXAConn) End(ctx context.Context, xid string, flags int) error {
	xaXID, err := oracleXABranchXid(xid)
	if err != nil {
		return err
	}

	flagExpr, err := oracleXAEndFlag(flags)
	if err != nil {
		return err
	}

	plsql := oracleXAWithXidBlock(
		"XA_END",
		xaXID,
		fmt.Sprintf("DBMS_XA.XA_END(l_xid, %s)", flagExpr),
		oracleXAOK,
	)
	return c.exec(ctx, plsql)
}

func (c *OracleXAConn) Forget(ctx context.Context, xid string) error {
	xaXID, err := oracleXABranchXid(xid)
	if err != nil {
		return err
	}

	plsql := oracleXAWithXidBlock("XA_FORGET", xaXID, "DBMS_XA.XA_FORGET(l_xid)", oracleXAOK)
	err = c.exec(ctx, plsql)
	if isOracleXAResultCode(err, oracleXAERNota) {
		return nil
	}
	return err
}

func (c *OracleXAConn) GetTransactionTimeout() time.Duration {
	return c.timeout
}

func (c *OracleXAConn) IsSameRM(ctx context.Context, resource XAResource) bool {
	other, ok := resource.(*OracleXAConn)
	if !ok {
		return false
	}
	return c.Conn == other.Conn
}

func (c *OracleXAConn) XAPrepare(ctx context.Context, xid string) error {
	xaXID, err := oracleXABranchXid(xid)
	if err != nil {
		return err
	}

	plsql := oracleXAWithXidBlock("XA_PREPARE", xaXID, "DBMS_XA.XA_PREPARE(l_xid)", oracleXAOK, oracleXAReadonly)
	return c.exec(ctx, plsql)
}

func (c *OracleXAConn) Recover(ctx context.Context, flag int) ([]string, error) {
	if err := oracleXARecoverFlag(flag); err != nil {
		return nil, err
	}
	startRscan := (flag & TMStartRScan) > 0
	if !startRscan {
		return nil, nil
	}

	queryer, ok := c.Conn.(driver.QueryerContext)
	if !ok {
		return nil, driver.ErrSkip
	}

	rows, err := queryer.QueryContext(ctx,
		"SELECT x.formatid, RAWTOHEX(x.gtrid), RAWTOHEX(x.bqual) FROM TABLE(DBMS_XA.XA_RECOVER()) x", nil)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var xids []string
	dest := make([]driver.Value, 3)
	for {
		if err = rows.Next(dest); err != nil {
			if errors.Is(err, io.EOF) {
				return xids, nil
			}
			return nil, err
		}

		formatID, err := oracleInt(dest[0])
		if err != nil {
			return nil, err
		}
		if formatID != oracleXAFormatID {
			continue
		}

		globalXidRawHex, err := oracleRawHex(dest[1])
		if err != nil {
			return nil, err
		}
		branchQualifierRawHex, err := oracleRawHex(dest[2])
		if err != nil {
			return nil, err
		}

		globalXidRaw, err := hex.DecodeString(globalXidRawHex)
		if err != nil {
			return nil, fmt.Errorf("decode oracle recovered gtrid failed: %w", err)
		}
		branchQualifierRaw, err := hex.DecodeString(branchQualifierRawHex)
		if err != nil {
			return nil, fmt.Errorf("decode oracle recovered bqual failed: %w", err)
		}

		recoveredXID, err := oracleRecoveredXID(globalXidRaw, branchQualifierRaw)
		if err != nil {
			return nil, err
		}
		xids = append(xids, recoveredXID)
	}
}

func (c *OracleXAConn) Rollback(ctx context.Context, xid string) error {
	xaXID, err := oracleXABranchXid(xid)
	if err != nil {
		return err
	}

	plsql := oracleXAWithXidBlock("XA_ROLLBACK", xaXID, "DBMS_XA.XA_ROLLBACK(l_xid)", oracleXAOK)
	err = c.exec(ctx, plsql)
	if isOracleXAResultCode(err, oracleXAERNota) {
		return nil
	}
	return err
}

func (c *OracleXAConn) SetTransactionTimeout(duration time.Duration) bool {
	if duration < 0 {
		return false
	}

	timeoutSeconds := int(math.Ceil(duration.Seconds()))
	if duration > 0 && timeoutSeconds == 0 {
		timeoutSeconds = 1
	}

	plsql := oracleXAWithoutXidBlock("XA_SETTIMEOUT",
		fmt.Sprintf("DBMS_XA.XA_SETTIMEOUT(%d)", timeoutSeconds), oracleXAOK)
	if err := c.exec(context.Background(), plsql); err != nil {
		return false
	}

	c.timeout = time.Duration(timeoutSeconds) * time.Second
	return true
}

func (c *OracleXAConn) Start(ctx context.Context, xid string, flags int) error {
	xaXID, err := oracleXABranchXid(xid)
	if err != nil {
		return err
	}

	flagExpr, err := oracleXAStartFlag(flags)
	if err != nil {
		return err
	}

	plsql := oracleXAWithXidBlock(
		"XA_START",
		xaXID,
		fmt.Sprintf("DBMS_XA.XA_START(l_xid, %s)", flagExpr),
		oracleXAOK,
	)
	return c.exec(ctx, plsql)
}

func (c *OracleXAConn) exec(ctx context.Context, query string) error {
	conn, ok := c.Conn.(driver.ExecerContext)
	if !ok {
		return driver.ErrSkip
	}

	_, err := conn.ExecContext(ctx, query, nil)
	if err == nil {
		return nil
	}

	if xaErr := parseOracleXAError(err); xaErr != nil {
		log.Errorf("oracle xa %s failed, result=%d, oer=%d, err=%v", xaErr.operation, xaErr.resultCode, xaErr.oracleCode, err)
		return xaErr
	}

	return err
}

func oracleXABranchXid(xid string) (*oracleXID, error) {
	branchSplitIdx := strings.LastIndex(xid, "-")
	if branchSplitIdx <= 0 || branchSplitIdx == len(xid)-1 {
		return nil, fmt.Errorf("invalid xa branch xid: %s", xid)
	}

	if _, err := strconv.ParseUint(xid[branchSplitIdx+1:], 10, 64); err != nil {
		return nil, fmt.Errorf("invalid xa branch xid: %s", xid)
	}

	xaXID := &oracleXID{
		formatID:            oracleXAFormatID,
		globalTransactionID: []byte(xid[:branchSplitIdx]),
		branchQualifier:     []byte(xid[branchSplitIdx:]),
	}

	if len(xaXID.globalTransactionID) == 0 {
		return nil, errors.New("invalid arguments")
	}
	if len(xaXID.globalTransactionID) > oracleXAMaxXIDPartSize {
		return nil, fmt.Errorf("oracle xa gtrid exceeds %d bytes", oracleXAMaxXIDPartSize)
	}
	if len(xaXID.branchQualifier) == 0 {
		return nil, errors.New("invalid arguments")
	}
	if len(xaXID.branchQualifier) > oracleXAMaxXIDPartSize {
		return nil, fmt.Errorf("oracle xa bqual exceeds %d bytes", oracleXAMaxXIDPartSize)
	}
	return xaXID, nil
}

func oracleXAStartFlag(flags int) (string, error) {
	switch flags {
	case TMNoFlags:
		return "DBMS_XA.TMNOFLAGS", nil
	case TMJoin:
		return "DBMS_XA.TMJOIN", nil
	case TMResume:
		return "DBMS_XA.TMRESUME", nil
	default:
		return "", errors.New("invalid arguments")
	}
}

func oracleXAEndFlag(flags int) (string, error) {
	switch flags {
	case TMSuccess, TMFail:
		// Oracle DBMS_XA does not support TMFAIL. We detach with TMSUCCESS and
		// let the following XA_ROLLBACK complete the rollback-only path.
		return "DBMS_XA.TMSUCCESS", nil
	case TMSuspend:
		return "DBMS_XA.TMSUSPEND", nil
	default:
		return "", errors.New("invalid arguments")
	}
}

func oracleXARecoverFlag(flags int) error {
	allowed := TMStartRScan | TMEndRScan
	if flags&^allowed != 0 {
		return errors.New("invalid arguments")
	}
	return nil
}

func oracleXAWithXidBlock(operation string, xid *oracleXID, statement string, successCodes ...int) string {
	return fmt.Sprintf(`DECLARE
	l_xid DBMS_XA_XID := DBMS_XA_XID(%d, HEXTORAW('%s'), HEXTORAW('%s'));
	l_ret PLS_INTEGER;
BEGIN
	l_ret := %s;
	IF l_ret NOT IN (%s) THEN
		RAISE_APPLICATION_ERROR(-20000, 'DBMS_XA.%s:ret=' || TO_CHAR(l_ret) || ',oer=' || TO_CHAR(DBMS_XA.XA_GETLASTOER()));
	END IF;
END;`,
		xid.formatID,
		hex.EncodeToString(xid.globalTransactionID),
		hex.EncodeToString(xid.branchQualifier),
		statement,
		oracleXAResultList(successCodes...),
		operation,
	)
}

func oracleXAWithoutXidBlock(operation string, statement string, successCodes ...int) string {
	return fmt.Sprintf(`DECLARE
	l_ret PLS_INTEGER;
BEGIN
	l_ret := %s;
	IF l_ret NOT IN (%s) THEN
		RAISE_APPLICATION_ERROR(-20000, 'DBMS_XA.%s:ret=' || TO_CHAR(l_ret) || ',oer=' || TO_CHAR(DBMS_XA.XA_GETLASTOER()));
	END IF;
END;`,
		statement,
		oracleXAResultList(successCodes...),
		operation,
	)
}

func oracleXAResultList(resultCodes ...int) string {
	values := make([]string, 0, len(resultCodes))
	for _, resultCode := range resultCodes {
		values = append(values, strconv.Itoa(resultCode))
	}
	return strings.Join(values, ", ")
}

func oracleRawHex(raw driver.Value) (string, error) {
	switch value := raw.(type) {
	case string:
		return value, nil
	case []byte:
		return string(value), nil
	default:
		return "", errOracleXARecoverProtocol
	}
}

func oracleInt(raw driver.Value) (int, error) {
	switch value := raw.(type) {
	case int:
		return value, nil
	case int32:
		return int(value), nil
	case int64:
		return int(value), nil
	case float64:
		if value != math.Trunc(value) {
			return 0, errOracleXARecoverProtocol
		}
		return int(value), nil
	case string:
		return oracleParseInt(value)
	case []byte:
		return oracleParseInt(string(value))
	default:
		return 0, errOracleXARecoverProtocol
	}
}

func oracleParseInt(value string) (int, error) {
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, errOracleXARecoverProtocol
	}
	return parsed, nil
}

func oracleRecoveredXID(globalTransactionID []byte, branchQualifier []byte) (string, error) {
	branchQualifierValue := string(branchQualifier)
	if !strings.HasPrefix(branchQualifierValue, "-") {
		return "", errOracleXARecoverProtocol
	}
	if _, err := strconv.ParseUint(strings.TrimPrefix(branchQualifierValue, "-"), 10, 64); err != nil {
		return "", errOracleXARecoverProtocol
	}
	return string(globalTransactionID) + branchQualifierValue, nil
}

func oracleBooleanLiteral(value bool) string {
	if value {
		return "TRUE"
	}
	return "FALSE"
}

func parseOracleXAError(err error) *oracleXAError {
	if err == nil {
		return nil
	}

	matches := oracleXAResultPattern.FindStringSubmatch(err.Error())
	if len(matches) != 4 {
		return nil
	}

	resultCode, convErr := strconv.Atoi(matches[2])
	if convErr != nil {
		return nil
	}
	oracleCode, convErr := strconv.Atoi(matches[3])
	if convErr != nil {
		return nil
	}

	return &oracleXAError{
		operation:  matches[1],
		resultCode: resultCode,
		oracleCode: oracleCode,
		cause:      err,
	}
}

func isOracleXAResultCode(err error, resultCode int) bool {
	var xaErr *oracleXAError
	if !errors.As(err, &xaErr) {
		return false
	}
	return xaErr.resultCode == resultCode
}
