## Install Scala
sudo wget www.scala-lang.org/files/archive/scala-2.11.7.deb
sudo dpkg -i scala-2.11.7.deb

## Install SBT
# Required
sudo apt-get install apt-transport-https
# SBT
echo "deb https://dl.bintray.com/sbt/debian /" | sudo tee -a /etc/apt/sources.list.d/sbt.list
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 642AC823
sudo apt-get update
sudo apt-get install sbt

## Install Java 8
sudo add-apt-repository ppa:webupd8team/java
sudo apt-get update
sudo apt-get install oracle-java8-installer

## Install git
sudo apt-get install git

## Install mvn
... just remove everything with mvn and use spark/build/mvn

## Install Spark
wget http://apache.uib.no/spark/spark-1.6.0/spark-1.6.0.tgz
tar xzf spark-1.6.0.tgz
mv spark-1.6.0/ spark
cd spark
./dev/change-scala-version.sh 2.11
build/mvn -Pyarn -Phadoop-2.4 -Dscala-2.11 -DskipTests clean package
