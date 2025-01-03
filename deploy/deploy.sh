#!/bin/bash -xe

branchName=${1:-master}
hash=$(git rev-parse --short $branchName)

echo "deploying ${hash} to isucon1, isucon2, isucon3"

## usage
## 1. login to isucon1, isucon2, isucon3
## 2. git clone on each instance
## 3. run this script
ssh isucon@isucon3 "git fetch && git checkout ${hash} && ~/deploy/deploy3.sh"
ssh isucon@isucon1 "git fetch && git checkout ${hash} && ~/deploy/deploy1.sh"
ssh isucon@isucon2 "git fetch && git checkout ${hash} && ~/deploy/deploy2.sh"