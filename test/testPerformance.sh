#!/usr/bin/env bash

iter=$1

for query in {1..22};
do
  for ((i=0; i<iter; i++));
  do
    printf "restart server\n";
    ./restart.sh sf01;
    echo "run test $query";
    python3 runTPCH-AIDA.py $query;
  done;
done;
