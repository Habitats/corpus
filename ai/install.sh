#!/usr/bin/env bash
############################
### Install Google Cloud ###
############################

## Apps
sudo apt-get update
sudo apt-get install -y htop

## .bashrc 
echo "alias submit2='cd ~/corpus/ai/ && git pull && gradle shadowJar && spark-submit --class no.habitats.corpus.spark.SparkUtil build/libs/ai-all.jar local=false '" >> ~/.bashrc
echo "alias submit='spark-submit --class no.habitats.corpus.spark.SparkUtil --jars ~/corpus/ai/build/libs/ai-all.jar ~/corpus/ai/build/libs/ai.jar local=false '" >> ~/.bashrc

# Git
mv ~/.ssh/github_rsa ~/.ssh/id_rsa
chmod 600 ~/.ssh/id_rsa
chmod 600 ~/.ssh/google_compute_engine
eval $(ssh-agent -s) && ssh-agent bash -c 'ssh-add ~/.ssh/id_rsa; yes | git clone http://github.com/Habitats/corpus.git'
git config --global user.email "mail@habitats.no"
git config --global user.name "Patrick Skjennum"
cd corpus && git remote set-url origin git@github.com:Habitats/corpus.git && git pull && cd ..

# Maven
if [ ! -f ~/apache-maven-3.3.3-bin.tar.gz ]; then
	sudo apt-get purge -y maven
	wget http://apache.cs.utah.edu/maven/maven-3/3.3.3/binaries/apache-maven-3.3.3-bin.tar.gz
	tar -zxf apache-maven-3.3.3-bin.tar.gz
	sudo cp -R apache-maven-3.3.3 /usr/local
	sudo ln -s /usr/local/apache-maven-3.3.3/bin/mvn /usr/bin/mvn
	echo "export M2_HOME=/usr/local/apache-maven-3.3.3" >> ~/.profile
	source ~/.profile
	echo "Maven is on version `mvn -v`"
fi

# Downlaod corpus
cd ~
if [ ! -f corpus-archive.zip ]; then
	#rm corpus-archive.*
	echo "Downloading ..."
    wget --progress=bar:force:noscroll https://www.dropbox.com/s/n0750x98l0hsrhp/corpus-archive.zip
fi
if [ ! -d data ]; then
	echo "Unzipping ..."
	unzip -o corpus-archive.zip
	mv models ~/corpus/
fi

# cmake
if [ ! -f cmake-3.2.2.tar.gz ]; then
	sudo apt-get install build-essential
	wget http://www.cmake.org/files/v3.2/cmake-3.2.2.tar.gz
	tar xf cmake-3.2.2.tar.gz
	cd cmake-3.2.2
	./configure
	make
	sudo make install
	echo "export PATH=/usr/local/bin:$PATH" >> ~/.profile
	echo "export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH" >> ~/.profile
	source ~./profile
	cmake --version # This should give v3.2
fi

# DL4J
mkdir ~/dl4j
cd ~/dl4j
git clone https://github.com/deeplearning4j/libnd4j.git
git pull
cd libnd4j
bash buildnativeoperations.sh cpu 
#bash buildnativeoperations.sh -c cuda
echo "export LIBND4J_HOME=`pwd`" >> ~/.profile
source ~./profile
cd ..
git clone https://github.com/deeplearning4j/nd4j.git
git pull
cd nd4j
mvn clean install -DskipTests -Dmaven.javadoc.skip=true -pl '!:nd4j-cuda-7.5,!org.nd4j:nd4j-tests'
cd ..
git clone https://github.com/deeplearning4j/deeplearning4j.git
git pull
cd deeplearning4j
mvn clean install -DskipTests -Dmaven.javadoc.skip=true

# Gradle

cd ~
if [ ! -d opt ]; then
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
fi

cd ~/corpus/ 
~/opt/gradle/bin/gradle clean ai:jar ai:shadowJar

# Copy over the raw data for easy access. A better solution would be to use HDFS, but whatever. edit: Actually HDFS was slow as hell.
scp -oStrictHostKeyChecking=no -i ~/.ssh/google_compute_engine -r ~/data habispam@corpus-w-0:~/
scp -oStrictHostKeyChecking=no -i ~/.ssh/google_compute_engine -r ~/data habispam@corpus-w-1:~/
scp -oStrictHostKeyChecking=no -i ~/.ssh/google_compute_engine -r ~/data habispam@corpus-w-2:~/
scp -oStrictHostKeyChecking=no -i ~/.ssh/google_compute_engine -r ~/data habispam@corpus-w-3:~/

