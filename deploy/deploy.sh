#!/bin/bash -xe

branchName=${1:-master}

## usage
## 1. login to isucon1, isucon2, isucon3
## 2. git clone on each instance
## 3. run this script
ssh isucon@isucon3 "git pull && git checkout ${branchName} && ~/deploy/deploy3.sh"
ssh isucon@isucon1 "git pull && git checkout ${branchName} && ~/deploy/deploy1.sh"
ssh isucon@isucon2 "git pull && git checkout ${branchName} && ~/deploy/deploy2.sh"