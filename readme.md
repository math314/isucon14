## How to initialize

Run this command on the user directory in isucon to sync with git
Note: generate or copy ssh key to pull the remote directly if it's private repo "AT YOUR OWN RISK".

```sh
git init
git remote add origin https://github.com/math314/isucon14.git
git pull
git checkout master -f
git branch --set-upstream-to origin/master
```

