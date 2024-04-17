/*
 * benchyou
 * xelabs.org
 *
 * Copyright (c) XeLabs
 * GPL License
 *
 */

package sysbench

import (
	"fmt"
	"log"
	"xworker"
)

// Table tuple.
type Table struct {
	workers []xworker.Worker
}

// NewTable creates the new table.
func NewTable(workers []xworker.Worker) *Table {
	return &Table{workers}
}

// Prepare used to prepare the tables.
func (t *Table) Prepare() {
	session := t.workers[0].S
	count := t.workers[0].N
	engine := t.workers[0].E
	for i := 0; i < count; i++ {
		sql := fmt.Sprintf(`create table benchyou%d (
							id bigint(20) unsigned not null auto_increment,
							k bigint(20) unsigned not null default '0',
							c char(120) not null default '',
							pad char(60) not null default '',
							primary key (id),
							key k_1 (k)
							) engine=%s`, i, engine)

		if _, err := session.Exec(sql); err != nil {
			log.Panicf("creata.table.error[%v]", err)
		}
		log.Printf("create table benchyou%d(engine=%v) finished...\n", i, engine)
	}
}

// Cleanup used to cleanup the tables.
func (t *Table) Cleanup() {
	session := t.workers[0].S
	count := t.workers[0].N
	for i := 0; i < count; i++ {
		sql := fmt.Sprintf(`drop table benchyou%d;`, i)

		if _, err := session.Exec(sql); err != nil {
			log.Panicf("drop.table.error[%v]", err)
		}
		log.Printf("drop table benchyou%d finished...\n", i)
	}
}
