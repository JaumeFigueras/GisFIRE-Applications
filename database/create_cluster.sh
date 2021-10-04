#!/bin/sh

# Get root privileges
[ "$(whoami)" != "root" ] && exec sudo -- "$0" "$@"

# Get call arguments
CLUSTER_NAME=$1
FOLDER=$2
PORT=$3
POSTGRESQL_VERSION=$4

pg_createcluster -d "$FOLDER"/"$CLUSTER_NAME" -l "$FOLDER"/"$CLUSTER_NAME"/"$CLUSTER_NAME".log -p "$PORT" --start --start-conf auto "$POSTGRESQL_VERSION" "$CLUSTER_NAME"
sudo -u postgres -- createuser -p "$PORT" -d -S -P -r "$CLUSTER_NAME"user
sudo -u postgres -- createdb -p "$PORT" -E UTF8 -O "$CLUSTER_NAME"user "$CLUSTER_NAME"db
