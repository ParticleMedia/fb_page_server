#!/bin/bash -e

pem_file=$1
if [ -z ${pem_file} ]; then
    pem_file=~/services.pem
fi

hosts=("172.31.20.243")
tar -zcvf output.tar.gz output/

for host in ${hosts[@]}; do
    scp -i ${pem_file} output.tar.gz services@${host}:~/
    ssh -i ${pem_file} services@${host} "tar -zxvf output.tar.gz && cp -f output/conf/* nonlocal_indexer/conf && sudo supervisorctl stop nonlocal_indexer && cp output/bin/* nonlocal_indexer/bin && sudo supervisorctl start nonlocal_indexer && rm -rf output && rm -f output.tar.gz"
    ret=$?
    if [ $ret -ne 0 ]; then
        exit $ret
    fi
done
rm -f output.tar.gz
