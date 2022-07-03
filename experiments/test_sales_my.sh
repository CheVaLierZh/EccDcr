#!/bin/bash

#trap "exec 1000>&-;exec 1000<&-;exit 0" 2

start=`date +%s`
{
    ./cloud/cloud.exe --metaJson=./cloud/data/sales_meta.json --query="DECLARE s,t IN sales, s.Product number = t.Product number AND s.Retailer code = t.Retailer code AND s.Unit sale price > t.Unit sale price AND s.Unit price < t.Unit price"
    
    end=`date +%s`

    echo "exec time {$((end-start - 30))}s"
} > cloud.txt 2>&1 &

{
    ./edge1/edge.exe --coordinatorIp=127.0.0.1 --port=4958 --dataDir=./edge1/data
} > edge1.txt 2>&1 &

{
    ./edge2/edge.exe --coordinatorIp=127.0.0.1 --port=4959 --dataDir=./edge2/data
} > edge2.txt 2>&1 &

wait

