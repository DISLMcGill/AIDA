#!/bin/bash

$MONETDB/bin/monetdbd start /home/monet/dbfarm/
$MONETDB/bin/monetdb  create courses01
$MONETDB/bin/monetdb  set embedpy3=true courses01
$MONETDB/bin/monetdb  release courses01
$MONETDB/bin/monetdb  create courses02
$MONETDB/bin/monetdb  set embedpy3=true courses02
$MONETDB/bin/monetdb  release courses02
$MONETDB/bin/monetdb  create courses03
$MONETDB/bin/monetdb  set embedpy3=true courses03
$MONETDB/bin/monetdb  release courses03
$MONETDB/bin/monetdbd stop /home/monet/dbfarm/
./setup_user.sh
for sf in "01" "02" "03"
do
	./load.sh $sf
done
