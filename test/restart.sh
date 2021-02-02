#!/usr/bin/env bash

dbuser=$1

$MONETDB/bin/monetdbd stop $DBFARMBASE
$MONETDB/bin/monetdbd start $DBFARMBASE

cur=$(pwd)
cd $home/AIDA/aidaMonetDB/scripts

export DOTMONETDBFILE=$MONETDB/sec/.${dbuser}
./startup_monetdb.sh $dbuser

cd $cur
