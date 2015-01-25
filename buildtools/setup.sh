#!/bin/sh

LD_LIBRARY_PATH=/opt/uptime/pgxl
PATH=/opt/uptime/pgxl/bin:$PATH
PGXC_CTL_HOME=/var/lib/pgxl/pgxc_ctl
export LD_LIBRARY_PATH PATH PGXC_CTL_HOME

function stop_and_cleanup()
{
    killall -9 postgres gtm
    sleep 3

    d=`dirname $1`/gtm
    echo deleting in $d
    rm -rf $d/*

    d=`dirname $1`/coord
    echo deleting in $d
    rm -rf $d/*

    for d in "$@"; do
	echo deleting in $d;
	rm -rf $d/*
    done;
}

function create_directories()
{
    d=`dirname $1`/gtm
    echo creating $d
    mkdir -p $d

    d=`dirname $1`/coord
    echo creating $d
    mkdir -p $d

    for d in "$@"; do
	echo creating $d;
	mkdir -p $d
    done;

    mkdir -p $PGXC_CTL_HOME
}

function init_all()
{
    cp -v $1 $PGXC_CTL_HOME/pgxc_ctl.conf
    pgxc_ctl init all
}

function create_pgxc_ctl_conf()
{
    TEMPLATE=$1
    shift
    OUTPUT=$1
    shift

    # Directories for GTM and coordinator nodes
    firstdatadir=$1

    i=1
    while [ $i -le $NUM_NODES ]; do
	name="data$i"
	port=`expr 20007 + $i`
	poolerport=`expr 21001 + $i`
	
	datanodeNames="$datanodeNames $name"
	datanodePorts="$datanodePorts $port"
	datanodePoolerPorts="$datanodePoolerPorts $poolerport"
	datanodeMasterServers="$datanodeMasterServers 127.0.0.1"
	
	dir=$1
	datanodeMasterDirs="$datanodeMasterDirs $dir"
	shift
	
	datanodeMaxWALSenders="\$datanodeMaxWALSenders $datanodeMaxWALSenders"

	i=`expr $i + 1`
    done;
    
    datanodeNames=`echo $datanodeNames | sed 's/^ //'`
    datanodePorts=`echo $datanodePorts | sed 's/^ //'`
    datanodePoolerPorts=`echo $datanodePoolerPorts | sed 's/^ //'`
    datanodeMasterServers=`echo $datanodeMasterServers | sed 's/^ //'`
    datanodeMasterDirs=`echo $datanodeMasterDirs | sed 's/^ //'`
    datanodeMaxWALSenders=`echo $datanodeMaxWALSenders | sed 's/^ //'`
    
    # Directories for GTM and coordinator nodes
    gtmMasterDir=`dirname $firstdatadir`/gtm
    coordMasterDir=`dirname $firstdatadir`/coord

    cat $TEMPLATE \
	| sed "s,__gtmMasterDir__,$gtmMasterDir," \
	| sed "s,__coordMasterDir__,$coordMasterDir," \
	| sed "s,__datanodeNames__,$datanodeNames," \
	| sed "s,__datanodePorts__,$datanodePorts," \
	| sed "s,__datanodePoolerPorts__,$datanodePoolerPorts," \
	| sed "s,__datanodeMasterServers__,$datanodeMasterServers," \
	| sed "s,__datanodeMasterDirs__,$datanodeMasterDirs," \
	| sed "s,__datanodeMaxWALSenders__,$datanodeMaxWALSenders," \
	> $OUTPUT

    diff -rc $TEMPLATE $OUTPUT
}

NUM_NODES=$#
echo "Number of nodes: $NUM_NODES"

stop_and_cleanup $@

create_directories $@

create_pgxc_ctl_conf buildtools/pgxc_ctl.conf.template /tmp/pgxc_ctl.conf $@

init_all /tmp/pgxc_ctl.conf
