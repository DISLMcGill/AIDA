#!/usr/bin/env bash

db=$1
usr=$2

if [ -z "$db" ]
then
  echo "Error: usage $0 <db> [ <usr> ]"
  exit 1
fi

if [[ -z "$POSTGRESQL" ]]
then
  echo "Error: variable POSTGRESQL is not set. run the env file in bin directory first"
  exit 1
fi


if [ -z "$usr" ]
then
    $POSTGRESQL/bin/psql -d $db < ../sqls/startup_postgresql.sql &
else
    $POSTGRESQL/bin/psql -U $usr -d $db < ../sqls/startup_postgresql.sql &
fi
