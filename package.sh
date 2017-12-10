#!/usr/bin/env bash
mvn clean package -DskipTests
rm ./dist/
HOME=$(cd "$(dirname "$0")/"; pwd)
cd $HOME
rm -rf ./dist
sh gaia-etl-flume/script/release.sh
sh gaia-tools/script/release.sh
sh gaia-spark-analysis/script/release.sh
sh gaia-sparketl/script/release.sh