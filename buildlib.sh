#!/bin/bash
if [ "$1" = "mkl" ] ; then
        echo 'Bulding for mkl ...'
        sudo mv /opt/intel-nope /opt/intel
else
        echo 'Bulding for openblas ...'
        sudo mv /opt/intel /opt/intel-nope
fi
cd ~/dl4j/libnd4j/ && ./buildnativeoperations.sh cpu
cd ~/dl4j/nd4j && mvn clean install -DskipTests -Dmaven.javadoc.skip=true -pl '!:nd4j-cuda-7.5,!org.nd4j:nd4j-tests'
cd git pull && ~/corpus/ && gradle ai:clean ai:jar ai:shadowJar -PscalaBinary=2.11 -PscalaVersion=2.11.6
