#!/bin/sh

CONTRIB_MODULES="pgxc_clean pgxc_ctl pgxc_ddl pgxc_monitor"

function _build_all()
{
    ./configure --prefix=/opt/uptime/pgxl --enable-debug
    make
    for c in $CONTRIB_MODULES; do
	pushd contrib/$c; make; popd
    done;
}

function _install_all()
{
    sudo rm -rf /opt/uptime/pgxl
    sudo make install
    for c in $CONTRIB_MODULES; do
	pushd contrib/$c; sudo make install; popd
    done;
}

_build_all
_install_all
