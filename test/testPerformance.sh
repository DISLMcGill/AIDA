#!/usr/bin/env bash

iter=$2
warmup=$1
output=$3

for query in {1..22};
do
  #./restart.sh sf01

  for ((i=0; i<warmup; i++));
  do
	echo "run warmup $query"
	python3 runTPCH-AIDA.py $query;
  done;
  for ((i=0; i<iter; i++));
  do
    #printf "restart server\n";
    #./restart.sh sf01;
    echo "run test $query"; 
    python3 runTPCH-AIDA.py --output=$output $query;
  done;
done;
