#!/bin/sh

LD_LIBRARY_PATH=/opt/uptime/pgxl
PATH=/opt/uptime/pgxl/bin:$PATH

export LD_LIBRARY_PATH PATH

PGXC_CTL_HOME=/tmp/pgxl/pgxc_ctl_home
export PGXC_CTL_HOME

killall -9 postgres gtm
sleep 3
rm -rf /tmp/pgxl/nodes/*

pgxc_ctl init all

