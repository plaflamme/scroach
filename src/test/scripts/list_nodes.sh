#!/bin/bash

cd "$(dirname $0)"

CONTAINERS=$(docker ps | egrep -e '-roachnode' | awk '{print $1}')

for CID in $CONTAINERS
do
  PORT=$(docker port $CID 8080)
  echo $PORT
done

