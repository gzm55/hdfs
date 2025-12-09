#!/bin/sh

HADOOP_HOME=${HADOOP_HOME-"$(pwd)/test-hadoop-home"}
NN_PORT=${NN_PORT-"9000"}
HADOOP_NAMENODE="localhost:$NN_PORT"

if [ ! -d "$HADOOP_HOME" ]; then
  mkdir -p $HADOOP_HOME

  echo "Downloading 3.4.2 apache dist to ${HADOOP_HOME}/hadoop.tar.gz"
  if [ -r hadoop.tar.gz ]; then
    # local cache
    cp hadoop.tar.gz ${HADOOP_HOME}/
  else
    # use apache version 3.4.2
    curl -o ${HADOOP_HOME}/hadoop.tar.gz -L 'https://archive.apache.org/dist/hadoop/core/hadoop-3.4.2/hadoop-3.4.2-lean.tar.gz'
  fi

  echo "Extracting ${HADOOP_HOME}/hadoop.tar.gz into $HADOOP_HOME"
  tar zxf ${HADOOP_HOME}/hadoop.tar.gz --strip-components 1 -C $HADOOP_HOME
fi

MINICLUSTER_JAR=$(find $HADOOP_HOME -name "hadoop-mapreduce-client-jobclient*.jar" | grep -v tests | grep -v sources | head -1)
if [ ! -f "$MINICLUSTER_JAR" ]; then
  echo "Couldn't find minicluster jar!"
  exit 1
fi

echo "Starting minicluster..."
( cd "$HADOOP_HOME" && exec bin/mapred minicluster -nnport $NN_PORT -datanodes 3 -nomr -format "$@" > minicluster.log 2>&1 ) &


export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
cat > $HADOOP_CONF_DIR/core-site.xml <<EOF
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://$HADOOP_NAMENODE</value>
  </property>
</configuration>
EOF

echo "Waiting for namenode to start up..."
$HADOOP_HOME/bin/hdfs dfsadmin -safemode wait

export HADOOP_FS="$HADOOP_HOME/bin/hdfs dfs"

( echo "export HADOOP_CONF_DIR='$HADOOP_CONF_DIR'"
  echo "export HADOOP_FS='$HADOOP_HOME/bin/hadoop fs'"
) | tee "$HADOOP_HOME/activate.sh"

./fixtures.sh

echo "Please run the following commands to activate:"
echo "source '$HADOOP_HOME/activate.sh'"
