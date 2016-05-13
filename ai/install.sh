#!/usr/bin/env bash
############################
### Install Google Cloud ###
############################

## Apps
sudo apt-get update
sudo apt-get install -y htop

sudo echo "deb http://http.us.debian.org/debian stable main contrib non-free" >> /etc/apt/sources.list # enable non-free packages
sudo apt-get install unrar

## .bashrc 
echo "alias submit2='cd ~/corpus/ && git pull && gradle ai:shadowJar && spark-submit --class no.habitats.corpus.spark.SparkUtil ~/corpus/ai/build/libs/ai-all.jar local=false '" >> ~/.bashrc
echo "alias submit='spark-submit --class no.habitats.corpus.spark.SparkUtil --jars ~/corpus/ai/build/libs/ai-all.jar ~/corpus/ai/build/libs/ai.jar local=false '" >> ~/.bashrc
echo "alias build='cd ~/corpus/ && git pull && gradle ai:jar'" >> ~/.bashrc
 
# Git
mv ~/.ssh/github_rsa ~/.ssh/id_rsa
chmod 600 ~/.ssh/id_rsa
chmod 600 ~/.ssh/google_compute_engine
eval $(ssh-agent -s) && ssh-agent bash -c 'ssh-add ~/.ssh/id_rsa; yes | git clone http://github.com/Habitats/corpus.git'
git config --global user.email "mail@habitats.no"
git config --global user.name "Patrick Skjennum"
cd ~/corpus && git remote set-url origin http://github.com/Habitats/corpus.git && git pull && cd ..

# Maven
if [ ! -d /usr/local/apache-maven-3.3.3 ]; then
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
if [ ! -d data ]; then
	mkdir data && cd data
	if [ ! -f fb_w2v_0.5.rar ]; then 
		wget --progress=bar:force:noscroll https://dl.dropboxusercontent.com/u/30450949/fb_w2v_0.5.rar
	fi
	if [ ! -f document_vectors_0.5.rar ]; then 
		wget --progress=bar:force:noscroll https://dl.dropboxusercontent.com/u/30450949/document_vectors_0.5.rar
	fi
	if [ ! -f nyt.rar ]; then
	    wget --progress=bar:force:noscroll https://dl.dropboxusercontent.com/u/30450949/nyt.rar
	fi
	unrar e nyt.rar && mkdir nyt && mv *txt nyt/ 
	unrar e document_vectors_0.5.rar
	unrar e fb_w2v_0.5.rar
	mkdir w2v && mv *txt w2v/ 
	rm *rar
fi

# cmake
cd ~ 
if [ ! -d cmake-3.2.2 ]; then
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
cd ~
if [ ! -d dl4j ]; then 
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
fi

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

