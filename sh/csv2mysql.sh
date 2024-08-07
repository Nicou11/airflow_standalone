#!/bin/bash

CSV_PATH=$1
DEL_DT=$2

user="root"
password="qwer123"
database="history_db"

mysql --local-infile=1 -u root -pqwer123 <<EOF
DELETE FROM history_db.tmp_cmd_usage WHERE dt='${DEL_DT}';

-- LOAD DATA INFILE '/var/lib/mysql-files/csv.csv'
LOAD DATA LOCAL INFILE '$CSV_PATH'
INTO TABLE history_db.tmp_cmd_usage
FIELDS TERMINATED BY ',' ENCLOSED BY '^' ESCAPED BY '\b'
LINES TERMINATED BY '\n';
EOF
