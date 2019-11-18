/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cluster

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path"
	"strings"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/log"
)

// MysqlctlProcess is a generic handle for a running mysqlctl command .
// It can be spawned manually
type MysqlctlProcess struct {
	Name         string
	Binary       string
	LogDirectory string
	TabletUID    int
	MySQLPort    int
	InitDBFile   string
}

// InitDb executes mysqlctl command to add cell info
func (mysqlctl *MysqlctlProcess) InitDb() (err error) {
	tmpProcess := exec.Command(
		mysqlctl.Binary,
		"-log_dir", mysqlctl.LogDirectory,
		"-tablet_uid", fmt.Sprintf("%d", mysqlctl.TabletUID),
		"-mysql_port", fmt.Sprintf("%d", mysqlctl.MySQLPort),
		"init",
		"-init_db_sql_file", mysqlctl.InitDBFile,
	)
	return tmpProcess.Run()
}

// Start executes mysqlctl command to start mysql instance
func (mysqlctl *MysqlctlProcess) Start() (err error) {
	tmpProcess := exec.Command(
		mysqlctl.Binary,
		"-log_dir", mysqlctl.LogDirectory,
		"-tablet_uid", fmt.Sprintf("%d", mysqlctl.TabletUID),
		"-mysql_port", fmt.Sprintf("%d", mysqlctl.MySQLPort),
		"init",
		"-init_db_sql_file", mysqlctl.InitDBFile,
	)
	return tmpProcess.Run()
}

// Stop executes mysqlctl command to stop mysql instance
func (mysqlctl *MysqlctlProcess) Stop() (err error) {
	tmpProcess := exec.Command(
		mysqlctl.Binary,
		"-tablet_uid", fmt.Sprintf("%d", mysqlctl.TabletUID),
		"shutdown",
	)
	return tmpProcess.Run()
}

// CleanupFiles clean the mysql files to make sure we can start the same process again
func (mysqlctl *MysqlctlProcess) CleanupFiles(tabletUID int) {
	os.RemoveAll(path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("/vt_%010d/data", tabletUID)))
	os.RemoveAll(path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("/vt_%010d/relay-logs", tabletUID)))
	os.RemoveAll(path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("/vt_%010d/tmp", tabletUID)))
	os.RemoveAll(path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("/vt_%010d/bin-logs", tabletUID)))
	os.RemoveAll(path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("/vt_%010d/innodb", tabletUID)))
}

// MysqlCtlProcessInstance returns a Mysqlctl handle for mysqlctl process
// configured with the given Config.
func MysqlCtlProcessInstance(tabletUID int, mySQLPort int, tmpDirectory string) *MysqlctlProcess {
	mysqlctl := &MysqlctlProcess{
		Name:         "mysqlctl",
		Binary:       "mysqlctl",
		LogDirectory: tmpDirectory,
		InitDBFile:   path.Join(os.Getenv("VTROOT"), "/config/init_db.sql"),
	}
	mysqlctl.MySQLPort = mySQLPort
	mysqlctl.TabletUID = tabletUID
	return mysqlctl
}

// StartMySQL create a connection to tablet mysql
func StartMySQL(ctx context.Context, tablet *Vttablet, username string, tmpDirectory string) (*mysql.Conn, error) {
	tablet.MysqlctlProcess = *MysqlCtlProcessInstance(tablet.TabletUID, tablet.MySQLPort, tmpDirectory)
	err := tablet.MysqlctlProcess.Start()
	if err != nil {
		return nil, err
	}
	params := mysql.ConnParams{
		Uname:      username,
		UnixSocket: fmt.Sprintf(path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("/vt_%010d", tablet.TabletUID), "/mysql.sock")),
	}

	conn, err := mysql.Connect(ctx, &params)
	return conn, err
}

// ExecuteCommandWithOutput executes any sqlctl command and returns output
func (mysqlctl *MysqlctlProcess) ExecuteCommandWithOutput(args ...string) (result string, err error) {
	tmpProcess := exec.Command(
		mysqlctl.Binary,
		args...,
	)
	println(fmt.Sprintf("Executing sqlctl with arguments %v", strings.Join(tmpProcess.Args, " ")))
	log.Info(fmt.Sprintf("Executing sqlctl with arguments %v", strings.Join(tmpProcess.Args, " ")))
	resultByte, err := tmpProcess.CombinedOutput()
	return string(resultByte), err
}
