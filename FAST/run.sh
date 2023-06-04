# shell script
# for different datasets, we have written different class to run

# Main class: including SyntheticDatasetExperiment, CrimesDatasetExperiment,
# ClusterDatasetExperiment, and TradeDatasetExperiment
# we only upload a 2M synthetic dataset in open source code
# if you want to run more experiments, please download others datasets
# our readMe.md has given the download URL, or you can choose send email to us to obtain the dataset
MAIN_CLASS=Experiment/SyntheticDatasetExperiment

JAVAHOME=/usr

MAVEN_HOME=$MAVEN_HOME
BASEPATH=$(cd `dirname $0`; pwd)

JAR_PATH=$BASEPATH/target/FAST-1.0-SNAPSHOT-jar-with-dependencies.jar

psid=0

initPsid(){
    javaps=`$JAVAHOME/bin/jps -l | grep $MAIN_CLASS`
    
    if [ -n "$javaps" ]; then
        psid=`echo $javaps | awk '{ print $1}'`
    else
        psid=0
    fi
}

start(){
    outfile=log.log
    if [ $# -ge 2 ] ; then
      outfile=$2
    fi
    initPsid

    if [ $psid -ne 0 ]; then
        echo "=================================================="
        echo "|         Program has already started          |"
        echo "=================================================="
    else
        echo "starting compile"
        ${MAVEN_HOME}/bin/mvn package
        echo "starting $MAIN_CLASS ..."
        echo "java -cp .:${JAR_PATH} $MAIN_CLASS > ${outfile} 2>&1 &"
        nohup java -cp .:${JAR_PATH} $MAIN_CLASS > ${outfile} 2>&1 &
        initPsid
        if [ $psid -ne 0 ]; then
            echo "start [OK] pid=$psid"
        else
            echo "start [FAILED], $?"
        fi
    fi
}

stop(){
    initPsid

    if [ $psid -ne 0 ]; then
        echo -n "Stopping $MAIN_CLASS pid=$psid ..."
        kill $psid
        if [ $? -eq 0 ]; then
            echo "Stop [OK]"
        else
            echo "Stop [FAILED]"
        fi
    else
        echo "=================================================="
        echo "|      WARN: $MAIN_CLASS is not running!      |"
        echo "=================================================="
    fi
}

status(){
    initPsid
    if [ $psid -ne 0 ]; then
        echo "Running"
    else
        echo "not Running"
    fi
}

info(){
    echo "********* System Information ***********"
    echo `head -n 1 /etc/issue`
    echo `uname -a`
    echo
    echo "JAVAHOME=$JAVAHOME"
    echo `$JAVAHOME/bin/java -version`
    echo
    echo "BASEPATH=$BASEPATH"
    echo "JAR_PATH=$JAR_PATH"
    echo "MAIN_CLASS=$MAIN_CLASS"
    echo "*****************************************"
}

case "$1" in
    'start')
        start $@
        ;;
    'stop')
        stop
        ;;
    'restart')
        stop
        start $2
        ;;
    'status')
        status
        ;;
    'info')
        info
        ;;
    *)

    echo "Usage $0 { start | stop | status | restart | info }"
    exit
esac
exit 0
