#!/usr/bin/env bash

for i in init-*.sql; do echo $i; trino --progress=true -f=$i; done
sleep 30
for i in optimize-*.sql; do echo $i; trino --progress=true -f=$i; done
sleep 10
echo 'precision,scale,time' > decimals.csv
for i in query-*.sh; do echo $i; bash $i >> 'decimals.csv'; done
