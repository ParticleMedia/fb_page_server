#!/bin/bash -e
cd $(dirname $0)
mkdir -p output/bin output/conf output/log

set -e

GIT_SHA=`git rev-parse --short HEAD || echo "NotGitVersion"`
WHEN=`date '+%Y-%m-%d_%H:%M:%S'`

GO111MODULE=on
go mod tidy
CGO_ENABLED=0 go build -installsuffix -a -v -o nonlocal_indexer -ldflags "-s -X main.GitSHA=${GIT_SHA} -X main.BuildTime=${WHEN}" .

cp run.sh output/bin/
cp clean_log.sh output/bin/
mv nonlocal_indexer output/bin/nonlocal_indexer
chmod 755 output/bin/*
cp -r conf/* output/conf/
