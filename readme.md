## How to initialize

### install alp

https://github.com/tkuchiki/alp/blob/main/README.ja.md

```sh
wget https://github.com/tkuchiki/alp/releases/download/v1.0.21/alp_linux_amd64.tar.gz
tar -zxvf alp_linux_amd64.tar.gz 
sudo install alp /usr/local/bin/alp
```

### initialize instances with git

Run this command on the user directory in isucon to sync with git
Note: generate or copy ssh key to pull the remote directly if it's private repo "AT YOUR OWN RISK".

```sh
git init
git remote add origin https://github.com/math314/isucon14.git
git pull
git checkout master -f
git branch --set-upstream-to origin/master
```

