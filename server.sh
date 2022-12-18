#!/bin/sh

VIRTUOSO_HOME="/GDD/virtuoso"
BLAZEGRAPH_HOME="/GDD/blazegraph"

function start_virtuoso {
  if [[ -z "$(lsof -t -i:8890)" ]]
  then
    echo "running virtuoso..."
    nohup $VIRTUOSO_HOME/bin/virtuoso-t +configfile $VIRTUOSO_HOME/var/lib/virtuoso/db/tuned-virtuoso.ini +foreground > virtuoso.out 2>&1 &
    while true; do
      curl --data-urlencode "query=ASK { ?s ?p ?o }" http://localhost:8890/sparql > /dev/null 2>&1
      if [ $? -eq 0 ]; then
        echo "exiting loop"
        break
      fi
      sleep 1
    done
    echo "virtuoso is up..."
  else
    echo "virtuoso is already running..."
  fi
}

function stop_virtuoso {
  if [[ -z "$(lsof -t -i:8890)" ]]
  then
    echo "virtuoso isn't running..."
  else
    echo "stopping virtuoso..."
    kill $(lsof -t -i:8890) 2>/dev/null
  fi
}

function start_blazegraph {
  if [[ -z "$(lsof -t -i:9999)" ]]
  then
    echo "running blazegraph..."
    nohup java -server -Xms16g -Xmx16g -XX:MaxDirectMemorySize=32g -Dbigdata.propertyFile=$BLAZEGRAPH_HOME/blazegraph.properties -jar $BLAZEGRAPH_HOME/blazegraph.jar > blazegraph.out 2>&1 &
    while true; do
      curl --data-urlencode "query=ASK { ?s ?p ?o }" http://localhost:9999/sparql > /dev/null 2>&1
      if [ $? -eq 0 ]; then
        echo "exiting loop"
        break
      fi
      sleep 1
    done
    echo "blazegraph is up..."
  else
    echo "blazegraph is already running..."
  fi
}

function stop_blazegraph {
  if [[ -z "$(lsof -t -i:9999)" ]]
  then
    echo "blazegraph isn't running..."
  else
    echo "stopping blazegraph..."
    kill $(lsof -t -i:9999) 2>/dev/null
  fi
}

if [[ "$1" == "start" ]]
then
  if [[ "$2" == "virtuoso" ]]
  then
    start_virtuoso
  fi
  if [[ "$2" == "blazegraph" ]]
  then
    start_blazegraph
  fi
fi

if [[ "$1" == "stop" ]]
then
  if [[ "$2" == "virtuoso" ]]
  then
    stop_virtuoso
  fi
  if [[ "$2" == "blazegraph" ]]
  then
    stop_blazegraph
  fi
fi