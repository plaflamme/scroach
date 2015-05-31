#!/bin/bash

CLONE_DIR="target/cockroachdb-proto"

if [ ! -e target/cockroachdb-proto ];
then
  git clone https://github.com/cockroachdb/cockroach-proto.git $CLONE_DIR
fi

cd $CLONE_DIR && git reset --hard 943eeb7534a66d5a2d6ace238738699ab9b5264e && cd -

rsync -vv -a --delete $CLONE_DIR/ src/main/protobuf/
