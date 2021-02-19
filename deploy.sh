#!/bin/bash -e

pem_file=$1
if [ -z ${pem_file} ]; then
    pem_file=~/services.pem
fi

hosts=("172.31.20.243")
tar -zcvf output.tar.gz output/

for host in ${hosts[@]}; do
    scp -i ${pem_file} output.tar.gz services@${host}:~/
    ssh -i ${pem_file} services@${host} "tar -zxvf output.tar.gz && cp -f output/conf/* fb_page_server/conf && sudo supervisorctl stop fb_page_server && cp output/bin/* fb_page_server/bin && sudo supervisorctl start fb_page_server && rm -rf output && rm -f output.tar.gz"
    ret=$?
    if [ $ret -ne 0 ]; then
        exit $ret
    fi
done
rm -f output.tar.gz
