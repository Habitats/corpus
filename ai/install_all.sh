sudo echo "deb http://http.us.debian.org/debian stable main contrib non-free" | sudo tee -a /etc/apt/sources.list # enable non-free packages
sudo echo "deb-src http://ppa.launchpad.net/webupd8team/java/ubuntu xenial main" | sudo tee -a /etc/apt/sources.list.d/webupd8team-java.list
sudo echo "deb http://ppa.launchpad.net/webupd8team/java/ubuntu xenial main" | sudo tee /etc/apt/sources.list.d/webupd8team-java.list
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys EEA14886
sudo apt-get update
sudo apt-get install -y htop unrar build-essential git unzip oracle-java8-installer
echo "export JAVA_HOME=/usr/lib/jvm/java-8-oracle" >> ~/.profile && . ~/.profile
 
sudo wget www.scala-lang.org/files/archive/scala-2.10.6.deb
sudo dpkg -i scala-2.10.6.deb

echo "alias submit='spark-submit --class no.habitats.corpus.spark.SparkUtil --jars ~/corpus/ai/build/libs/ai-all.jar ~/corpus/ai/build/libs/ai.jar '" >> ~/.bashrc
echo "alias build='cd ~/corpus/ && git pull && gradle ai:clean ai:shadowJar'" >> ~/.bashrc

# cmake
cd ~ 
if [ ! -d cmake-3.2.2 ]; then
	wget http://www.cmake.org/files/v3.2/cmake-3.2.2.tar.gz
	tar xf cmake-3.2.2.tar.gz
	cd cmake-3.2.2
	./configure
	make
	sudo make install
	echo "export PATH=/usr/local/bin:$PATH" >> ~/.profile
	echo "export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH" >> ~/.profile
	. ~./profile
	cmake --version # This should give v3.2
fi

wget http://registrationcenter-download.intel.com/akdlm/irc_nas/9068/l_mkl_11.3.3.210.tgz
tar xf l_mkl_11.3.3.210.tgz

mkdir ~/data/ && cd ~/data
wget --progress=bar:force:noscroll https://storage.googleapis.com/ntnu-corpus/fb_w2v_0.5.rar
unrar e fb_w2v_0.5.rar && mkdir w2v && mv fb_*txt w2v/

wget --progress=bar:force:noscroll https://storage.googleapis.com/ntnu-corpus/document_vectors_0.5.rar
unrar e document_vectors_0.5.rar && mv doc*txt w2v/

wget --progress=bar:force:noscroll https://storage.googleapis.com/ntnu-corpus/nyt.rar
wget --progress=bar:force:noscroll https://dl.dropboxusercontent.com/u/30450949/nyt_sub.rar
unrar e nyt.rar
unrar e nyt_sub.rar
mkdir nyt && mv *txt nyt/ 

cd ~
wget http://d3kbcqa49mib13.cloudfront.net/spark-1.5.2-bin-hadoop2.4.tgz
tar xvf spark-1.5.2-bin-hadoop2.4.tgz
mv spark-1.5.2-bin-hadoop2.4 spark
cd ~/spark/conf/
touch spark-defaults.conf
echo "spark.master                    local[32]" >> spark-defaults.conf
echo "spark.driver.maxResultSize      0"  >> spark-defaults.conf
echo "spark.eventLog.dir              /home/patricls/.cache"  >> spark-defaults.conf
echo "spark.driver.memory             60g"  >> spark-defaults.conf
echo "spark.executor.extraJavaOptions -XX:+UseConcMarkSweepGC"  >> spark-defaults.conf
echo "spark.driver.extraJavaOptions   -XX:+UseConcMarkSweepGC"  >> spark-defaults.conf
echo "export PATH=~/spark/bin:$PATH" >> ~/.profile


# Gradle
cd ~
if [ ! -d opt ]; then
	sudo apt-get install unzip
	mkdir -p ~/opt/packages/gradle 
	cd $_ 
	wget --progress=bar:force:noscroll https://services.gradle.org/distributions/gradle-2.10-bin.zip 
	unzip -o gradle-2.10-bin.zip 
	ln -s ~/opt/packages/gradle/gradle-2.10/ ~/opt/gradle 
	echo "if [ -d \"\$HOME/opt/gradle\" ]; then
	    export GRADLE_HOME=\"\$HOME/opt/gradle\"
	    PATH=\"\$PATH:\$GRADLE_HOME/bin\"
	fi" >> ~/.profile
	. ~/.profile
	. ~/.bashrc
fi

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

# DL4J
cd ~
if [ ! -d dl4j ]; then 
	mkdir ~/dl4j
	cd ~/dl4j
	git clone https://github.com/deeplearning4j/libnd4j.git 
	git clone https://github.com/deeplearning4j/nd4j.git
	git clone https://github.com/habitats/deeplearning4j.git
	git clone https://github.com/deeplearning4j/Canova.git

	cd ~/dl4j/libnd4j && bash buildnativeoperations.sh cpu && echo "export LIBND4J_HOME=`pwd`" >> ~/.profile && export LIBND4J_HOME=`pwd` && . ~./profile
	cd ~/dl4j/nd4j && /usr/local/apache-maven-3.3.3/bin/mvn clean install -DskipTests -Dmaven.javadoc.skip=true -pl '!:nd4j-cuda-7.5,!org.nd4j:nd4j-tests'
	cd ~/dl4j/Canova && /usr/local/apache-maven-3.3.3/bin/mvn clean install -DskipTests -Dmaven.javadoc.skip=true 
	cd ~/dl4j/deeplearning4j && /usr/local/apache-maven-3.3.3/bin/mvn clean install -DskipTests -Dmaven.javadoc.skip=true -Dscala.binary.version=2.10 -Dscala.version=2.10.6 -Dspark.version=1.5.2
fi

git config --global user.email "mail@habitats.no"
git config --global user.name "Patrick Skjennum"
git config --global alias.lg "log --color --graph --pretty=format:'%Cred%h%Creset -%C(yellow)%d%Creset %s %Cgreen(%cr) %C(bold blue)<%an>%Creset' --abbrev-commit"
cd ~/ && git clone http://github.com/habitats/corpus.git && cd ~/corpus/ && ~/opt/gradle/bin/gradle clean ai:jar ai:shadowJar
