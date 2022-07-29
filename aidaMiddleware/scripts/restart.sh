#!/usr/bin/env bash

$MONETDB/bin/monetdbd stop /home/build/monet/dbfarm
$MONETDB/bin/monetdbd start /home/build/monet/dbfarm

./startup_monetdb.sh bixi bixi