#!/usr/bin/env bash

db=$1
usr=$2

if [ -z "$db" ]
then
  echo "Error: usage $0 <db> [ <usr> ]"
  exit 1
fi

if [[ -z "$MONETDB" ]]
then
  echo "Error: variable MONETDB is not set. run the env file in bin directory first"
  exit 1
fi


if [ -z "$usr" ]
then
    $MONETDB/bin/mclient -d $db < ../sqls/startup_monetdb.sql
else
    $MONETDB/bin/mclient -u $usr -d $db < ../sqls/startup_monetdb.sql
fi
