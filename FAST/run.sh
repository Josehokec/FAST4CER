#mainclass=Experiemnt/TradeDatasetExperiment
#mvn package
#cd target

#echo "java -cp .;FAST-1.0-SNAPSHOT-jar-with-dependencies.jar ${mainclass}"
#java -cp .;FAST-1.0-jar-with-dependencies.jar ${mainclass}

# Main class: including CrimesDatasetExperiment, SyntheticDatasetExperiment, and ClusterDatasetExperiment
MAIN_CLASS=Experiment/CrimesDatasetExperiment

JAVAHOME=/usr
MAVEN_HOME=/Users/liushizhe/Downloads/apache-maven-3.9.2
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