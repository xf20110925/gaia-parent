if [ $# -lt 1 ]
then
	echo "[ERROR]  Too few parameters"
	echo "[ADVICE] ./\${command} agent_name"
	exit -1
fi

source /etc/profile

HOME=$(cd "$(dirname "$0")/../"; pwd)
cd $HOME

jar_page=`ls ./libs`
class_path=""
for jar in $jar_page 
do
	class_path="${class_path}./libs/${jar}:"
done

logdir="./logs"
confdir="./config"
agent=$1

nohup /opt/flume/bin/flume-ng agent --name $agent -c ./ -f /opt/ptbconf/gaia-flume-es/gaia-flume.conf -C /opt/ptbconf/gaia-flume-es:${class_path%?}  -Xmx400m  -Dproperty=${agent}> ${logdir}/run_${agent}.log 2>&1 &
#test
