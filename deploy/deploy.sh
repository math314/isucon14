#!/bin/bash -xe


## usage
## 1. login to isucon1, isucon2, isucon3
## 2. git clone on each instance
## 3. run this script

ssh isucon@isucon1 "git checkout master && git pull && ~/deploy/deploy1.sh"
ssh isucon@isucon2 "git checkout master && git pull && ~/deploy/deploy2.sh"
ssh isucon@isucon3 "git checkout master && git pull && ~/deploy/deploy3.sh"