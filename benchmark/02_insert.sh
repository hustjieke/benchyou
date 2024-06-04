#!/bin/sh

../bin/benchyou  --mysql-host=127.0.0.1 --mysql-user=root --mysql-password= --oltp-tables-count=16  --mysql-port=15306 --oltp-tables-count=16  --write-threads=8  --read-threads=0 --rows-per-insert=100 --mysql-db=commerce --max-time=1000 seq
