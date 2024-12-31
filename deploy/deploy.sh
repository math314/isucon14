#!/bin/bash -xe


## usage
## 1. login to isucon1, isucon2, isucon3
## 2. git clone on each instance
## 3. run this script

ssh isucon@isucon1 "cd ~/isucon14/deploy && ./deploy1.sh"
ssh isucon@isucon2 "cd ~/isucon14/deploy && ./deploy2.sh"
ssh isucon@isucon3 "cd ~/isucon14/deploy && ./deploy3.sh"