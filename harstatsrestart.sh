/home/super/harstorage-stats-env/bin/uwsgi --stop /home/super/var/run/harstorage-stats.pid
export LD_LIBRARY_PATH=:/opt/rh/python27/root/usr/lib64/
/home/super/harstorage-stats-env/bin/uwsgi --ini /home/super/uwsgi/hss.ini
tail -f /home/super/var/log/harstorage-stats_uwsgi.log
