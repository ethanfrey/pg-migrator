#!/bin/bash

# how to set up postgres-9.4 on an ubuntu system
apt-key adv --keyserver pgpkeys.co.uk --recv-keys B97B0AFCAA1A47F044F244A07FCC7D46ACCC4CF8
echo 'deb http://apt.postgresql.org/pub/repos/apt/ wheezy-pgdg main' $PG_MAJOR > /etc/apt/sources.list.d/pgdg.list
apt-get update
apt-get install -y postgresql-9.4 postgresql-contrib-9.4 postgresql-server-dev-9.4
apt-get install -y python-dev
