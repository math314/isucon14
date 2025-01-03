#!/bin/bash -xe

set -eux
cd $(dirname $0)
source ~/.local.env

# # go service
cd ../webapp/go
echo `pwd`
echo `which go`
go build -o isuride
cd ../../deploy
sudo systemctl stop isuride-go.service
cp ../env2.sh /home/isucon/env.sh
sudo systemctl restart isuride-go.service

# # nginx service
# sudo cp ../webapp/config/nginx02.conf /etc/nginx/nginx.conf
# sudo cp ../webapp/config/nginx_site02.conf /etc/nginx/sites-enabled/isuride.conf
# sudo systemctl restart nginx.service
