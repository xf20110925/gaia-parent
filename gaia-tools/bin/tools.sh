 #!/usr/bin/env bash
 source /etc/profile
 HOME=$(cd "$(dirname "$0")/../"; pwd)
 cd $HOME
 jars=$(ls $HOME/|grep .jar)
 libjar=`ls libs | grep .jar | awk '{jar=jar"'"libs/"'"$1":"} END {print jar}'`
 echo $jars

 echo $HOME/config:$jars:$libjar
 java -cp /opt/ptbconf/gaia-tools:$HOME/config:$jars:$libjar  com.ptb.gaia.tool.Tool $*
