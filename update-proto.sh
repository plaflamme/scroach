#!/bin/bash

CLONE_DIR="target/cockroachdb-proto"

if [ ! -e target/cockroachdb-proto ];
then
  git clone https://github.com/cockroachdb/cockroach-proto.git $CLONE_DIR
else
  cd $CLONE_DIR && git fetch origin && cd -
fi

cd $CLONE_DIR && git reset --hard $1 && cd -

rsync -vv -a --delete $CLONE_DIR/ src/main/protobuf/
echo $1 > src/main/protobuf/VERSION
