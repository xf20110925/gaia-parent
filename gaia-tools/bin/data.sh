#!/usr/bin/env bash


docker-control-container "create" "gaia-tools-tmp" "sh /opt/gaia-tools/bin/tools-mc.sh -FilePathToMongoDB -startTime 20161001 -endTime 20161003 -startHour 0 -endHour 23"

sleep 15s

docker-control-container "create" "gaia-tools-tmp" "sh /opt/gaia-tools/bin/tools-mc.sh -FilePathToMongoDB -startTime 20161004 -endTime 20161006 -startHour 0 -endHour 23"

sleep 15s

docker-control-container "create" "gaia-tools-tmp" "sh /opt/gaia-tools/bin/tools-mc.sh -FilePathToMongoDB -startTime 20161007 -endTime 20161009 -startHour 0 -endHour 23"

sleep 15s

docker-control-container "create" "gaia-tools-tmp" "sh /opt/gaia-tools/bin/tools-mc.sh -FilePathToMongoDB -startTime 20161010 -endTime 20161012 -startHour 0 -endHour 23"

sleep 15s

docker-control-container "create" "gaia-tools-tmp" "sh /opt/gaia-tools/bin/tools-mc.sh -FilePathToMongoDB -startTime 20161013 -endTime 20161015 -startHour 0 -endHour 23"

sleep 15s

docker-control-container "create" "gaia-tools-tmp" "sh /opt/gaia-tools/bin/tools-mc.sh -FilePathToMongoDB -startTime 20161016 -endTime 20161017 -startHour 0 -endHour 23"

