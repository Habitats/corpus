#!/usr/bin/env bash
############################
### Install Google Cloud ###
############################

## Apps
sudo apt-get update
sudo apt-get install -y htop

## .bashrc 
echo "alias submit2='cd ~/corpus/ai/ && git pull && gradle shadowJar && spark-submit --class no.habitats.corpus.spark.SparkUtil build/libs/ai-all.jar local=false '" >> ~/.bashrc
echo "alias submit='spark-submit --class no.habitats.corpus.spark.SparkUtil --jars ~/corpus/ai/build/libs/ai-all.jar ~/ai.jar local=false '" >> ~/.bashrc

# Git
mv ~/.ssh/github_rsa ~/.ssh/id_rsa
chmod 600 ~/.ssh/id_rsa
chmod 600 ~/.ssh/google_compute_engine
eval $(ssh-agent -s) && ssh-agent bash -c 'ssh-add ~/.ssh/id_rsa; yes | git clone http://github.com/Habitats/corpus.git'
git config --global user.email "mail@habitats.no"
git config --global user.name "Patrick Skjennum"
cd corpus && git remote set-url origin git@github.com:Habitats/corpus.git && cd ..

# Downlaod corpus
cd ~
rm corpus-archive.*
if [ -f ~/corpus-archive.zip ]; then
	echo "Downloading ..."
    wget --progress=bar:force:noscroll https://www.dropbox.com/s/n0750x98l0hsrhp/corpus-archive.zip
fi
if [ -d data ]; then
	echo "Unzipping ..."
	unzip -o corpus-archive.zip
	mv models ~/corpus/
fi

# DL4J
mkdir ~/dl4j
cd ~/dl4j
yes | git clone git@github.com:deeplearning4j/libnd4j.git
cd libnd4j
bash buildnativeoperations.sh cpu 
#bash buildnativeoperations.sh -c cuda
export LIBND4J_HOME=`pwd`
cd ..
yes | git clone git@github.com:deeplearning4j/nd4j.git
cd nd4j
mvn clean install -DskipTests -Dmaven.javadoc.skip=true -pl '!:nd4j-cuda-7.5,!org.nd4j:nd4j-tests'
cd ..
git clone git@github.com:deeplearning4j/deeplearning4j.git
cd deeplearning4j
mvn clean install -DskipTests -Dmaven.javadoc.skip=true

# Gradle
mkdir -p ~/opt/packages/gradle 
cd $_ 
wget --progress=bar:force:noscroll https://services.gradle.org/distributions/gradle-2.10-bin.zip 
unzip -o gradle-2.10-bin.zip 
ln -s ~/opt/packages/gradle/gradle-2.10/ ~/opt/gradle 
echo "if [ -d \"\$HOME/opt/gradle\" ]; then
    export GRADLE_HOME=\"\$HOME/opt/gradle\"
    PATH=\"\$PATH:\$GRADLE_HOME/bin\"
fi" >> ~/.profile
source ~/.profile
source ~/.bashrc

cd ~/corpus/ 
~/opt/gradle/bin/gradle clean jar shadowJar

# Copy over the raw data for easy access. A better solution would be to use HDFS, but whatever. edit: Actually HDFS was slow as hell.
scp -oStrictHostKeyChecking=no -i ~/.ssh/google_compute_engine -r ~/data habispam@corpus-w-0:~/
scp -oStrictHostKeyChecking=no -i ~/.ssh/google_compute_engine -r ~/data habispam@corpus-w-1:~/
scp -oStrictHostKeyChecking=no -i ~/.ssh/google_compute_engine -r ~/data habispam@corpus-w-2:~/
scp -oStrictHostKeyChecking=no -i ~/.ssh/google_compute_engine -r ~/data habispam@corpus-w-3:~/

