FILENAME=/home/isucon/logs/mysql-slow-$(date +%Y%m%d_%H%M%S).log

sudo cp /var/log/mysql/mysql-slow.log $FILENAME
sudo chown isucon:isucon $FILENAME
