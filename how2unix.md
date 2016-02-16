## Install Scala
sudo wget www.scala-lang.org/files/archive/scala-2.10.6.deb
sudo dpkg -i scala-2.10.6.deb

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

## Install gradle
mkdir -p ~/opt/packages/gradle && cd $_ && wget https://services.gradle.org/distributions/gradle-2.10-bin.zip && unzip gradle-2.10-bin.zip && ln -s ~/opt/packages/gradle/gradle-2.10/ ~/opt/gradle && vim ~/.profile
```
# Gradle
	if [ -d "$HOME/opt/gradle" ]; then
	    export GRADLE_HOME="$HOME/opt/gradle"
	    PATH="$PATH:$GRADLE_HOME/bin"
	fi
```
source ~/.profile && gradle -version

## CPU Monitor
sudo apt-get install htop

## RAR
sudo apt-get install unrar-free


## Add SSH key 
# Generate key, name keys id_rsa for automatic ssh
ssh-keygen -t rsa

# On Ubuntu
ssh-copy-id -i user@hostname

# Manual for cygwin
cat ~/.ssh/id_rsa.pub | ssh user@hostname 'cat >> .ssh/authorized_keys'

# Add to ssh-agent
eval $(ssh-agent -s) && ssh-add ~/.ssh/id_rsa

# Permissions on .ssh
chown -R mail:users ~/.ssh/
chmod -R 600 ~/.ssh/

############################
### Install Google Cloud ###
############################

scp -oStrictHostKeyChecking=no -i ~/.ssh/google_compute_engine ~/.ssh/github_rsa habispam@spark-m:~/.ssh/
ssh -oStrictHostKeyChecking=no -i ~/.ssh/google_compute_engine habispam@spark-m

## Apps
sudo apt-get update
sudo apt-get install htop

## .bashrc 
echo "alias submit2='gradle -p ~/corpus/ shadowJar && spark-submit --class no.habitats.corpus.spark.SparkUtil ~/corpus/build/libs/corpus-all.jar local=false rdd=local '" >> ~/.bashrc
echo "alias submit='spark-submit --class no.habitats.corpus.spark.SparkUtil --jars ~/corpus/build/libs/corpus-all.jar ~/corpus.jar local=false rdd=json '" >> ~/.bashrc

# Git
yes | git clone git@github.com:Habitats/corpus.git
wget https://www.dropbox.com/s/n0750x98l0hsrhp/corpus-archive.zip
unzip corpus-archive.zip
mv models corpus/

# Gradle
mkdir -p ~/opt/packages/gradle && cd $_ && wget https://services.gradle.org/distributions/gradle-2.10-bin.zip && unzip gradle-2.10-bin.zip && ln -s ~/opt/packages/gradle/gradle-2.10/ ~/opt/gradle 
echo "if [ -d \"$HOME/opt/gradle\" ]; then\"
    export GRADLE_HOME=\"$HOME/opt/gradle\"
    PATH=\"$PATH:$GRADLE_HOME/bin\"
fi" >> ~/.profile

source ~/.profile && gradle -version
source ~/.bashrc

cd ~/corpus/ 
gradle clean jar shadowJar