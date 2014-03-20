/home/super/harstorage/bin/uwsgi --stop $HOME/var/run/harstorage.pid
export LD_LIBRARY_PATH=:/opt/rh/python27/root/usr/lib64/
/home/super/harstorage/bin/uwsgi --ini-paste $HOME/git/harstorage/production.ini
tail -f $HOME/var/log/harstorage_uwsgi.log
