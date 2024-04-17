/*
 * benchyou
 * xelabs.org
 *
 * Copyright (c) XeLabs
 * GPL License
 *
 */

package xworker

import (
	"database/sql"
	"fmt"
	"log"
	"xcommon"

	_ "github.com/go-sql-driver/mysql"
)

// Metric tuple.
type Metric struct {
	WNums  uint64
	WCosts uint64
	WMax   uint64
	WMin   uint64
	QNums  uint64
	QCosts uint64
	QMax   uint64
	QMin   uint64
}

// Worker tuple.
type Worker struct {
	// session
	S *sql.DB

	// mertric
	M *Metric

	// engine
	E string

	// xid
	XID string

	// table number
	N int
}

// CreateWorkers creates the new workers.
func CreateWorkers(conf *xcommon.Conf, threads int) []Worker {
	var workers []Worker
	var conn *sql.DB
	var err error

	// utf8 := "utf8"
	// dsn := fmt.Sprintf("%s:%d", conf.MysqlHost, conf.MysqlPort)
	dataSourceName := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", conf.MysqlUser, conf.MysqlPassword, conf.MysqlHost, conf.MysqlPort, conf.MysqlDb)
	log.Printf("dataSourceName: %s", dataSourceName)

	for i := 0; i < threads; i++ {

		// if conn, err := driver.NewConn(conf.MysqlUser, conf.MysqlPassword, dsn, conf.MysqlDb, utf8); err != nil {
		// TODO: rm hard code
		if conn, err = sql.Open("mysql", "root:@tcp(127.0.0.1:3307)/sbtest"); err != nil {
			log.Panicf("create.worker.error:%v", err)
		}
		workers = append(workers, Worker{
			S: conn,
			M: &Metric{},
			E: conf.MysqlTableEngine,
			N: conf.OltpTablesCount,
		},
		)
	}
	return workers
}

// AllWorkersMetric returns all the worker's metric.
func AllWorkersMetric(workers []Worker) *Metric {
	all := &Metric{}
	for _, worker := range workers {
		all.WNums += worker.M.WNums
		all.WCosts += worker.M.WCosts

		if all.WMax < worker.M.WMax {
			all.WMax = worker.M.WMax
		}

		if all.WMin > worker.M.WMin {
			all.WMin = worker.M.WMin
		}

		all.QNums += worker.M.QNums
		all.QCosts += worker.M.QCosts

		if all.QMax < worker.M.QMax {
			all.QMax = worker.M.QMax
		}

		if all.QMin > worker.M.QMin {
			all.QMin = worker.M.QMin
		}
	}

	return all
}

// StopWorkers used to stop all the worker.
func StopWorkers(workers []Worker) {
	for _, worker := range workers {
		worker.S.Close()
	}
}
