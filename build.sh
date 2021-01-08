#!/bin/bash -e
cd $(dirname $0)
mkdir -p output/bin output/conf output/log

set -e

GIT_SHA=`git rev-parse --short HEAD || echo "NotGitVersion"`
WHEN=`date '+%Y-%m-%d_%H:%M:%S'`

GO111MODULE=on
go mod tidy
CGO_ENABLED=0 go build -installsuffix -a -v -o fb_page_tcat -ldflags "-s -X main.GitSHA=${GIT_SHA} -X main.BuildTime=${WHEN}" .

cp run.sh output/bin/
cp clean_log.sh output/bin/
mv fb_page_tcat output/bin/fb_page_tcat
chmod 755 output/bin/*
cp -r conf/* output/conf/
