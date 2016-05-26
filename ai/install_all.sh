#sudo echo "deb http://http.us.debian.org/debian stable main contrib non-free" | sudo tee -a /etc/apt/sources.list # enable non-free packages
sudo echo "deb-src http://ppa.launchpad.net/webupd8team/java/ubuntu xenial main" | sudo tee -a /etc/apt/sources.list.d/webupd8team-java.list
sudo echo "deb http://ppa.launchpad.net/webupd8team/java/ubuntu xenial main" | sudo tee /etc/apt/sources.list.d/webupd8team-java.list
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys EEA14886
sudo apt-get update
sudo apt-get install -y htop build-essential git unzip oracle-java8-installer
echo "export JAVA_HOME=/usr/lib/jvm/java-8-oracle" >> ~/.profile && . ~/.profile
 
sudo wget www.scala-lang.org/files/archive/scala-2.11.7.deb && sudo dpkg -i scala-2.11.7.deb

echo "alias submit='screen -S spark spark-submit --class no.habitats.corpus.spark.SparkUtil --jars ~/corpus/ai-all.jar ~/corpus/ai/build/libs/ai.jar '" >> ~/.bashrc
echo "alias submit2='spark-submit --class no.habitats.corpus.spark.SparkUtil --jars ~/corpus/ai-all.jar ~/corpus/ai/build/libs/ai.jar '">> ~/.bashrc
echo "alias build='cd ~/corpus/ && git pull && gradle ai:clean ai:jar -PscalaBinary=2.11 -PscalaVersion=2.11.6'">> ~/.bashrc
echo "alias buildfat='cd ~/corpus/ && git pull && gradle ai:clean ai:shadowJar ai:jar -PscalaBinary=2.11 -PscalaVersion=2.11.6 && mv -f ~/corpus/ai/build/libs/ai-all.jar ~/corpus/'">> ~/.bashrc

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
	git clone https://github.com/deeplearning4j/canova.git

	cd ~/dl4j/libnd4j && bash buildnativeoperations.sh cpu && echo "export LIBND4J_HOME=`pwd`" >> ~/.profile && export LIBND4J_HOME=`pwd` && . ~/.profile
	cd ~/dl4j/nd4j && /usr/local/apache-maven-3.3.3/bin/mvn clean install -DskipTests -Dmaven.javadoc.skip=true -pl '!:nd4j-cuda-7.5,!org.nd4j:nd4j-tests'
	cd ~/dl4j/Canova && /usr/local/apache-maven-3.3.3/bin/mvn clean install -DskipTests -Dmaven.javadoc.skip=true -Dscala.binary.version=2.10 -Dscala.version='+project.scalaVersion+'
	cd ~/dl4j/deeplearning4j && /usr/local/apache-maven-3.3.3/bin/mvn clean install -DskipTests -Dmaven.javadoc.skip=true -Dscala.binary.version=2.10 -Dscala.version='+project.scalaVersion+'
fi

git config --global user.email "mail@habitats.no"
git config --global user.name "Patrick Skjennum"
git config --global alias.lg "log --color --graph --pretty=format:'%Cred%h%Creset -%C(yellow)%d%Creset %s %Cgreen(%cr) %C(bold blue)<%an>%Creset' --abbrev-commit"
cd ~/ && git clone http://github.com/habitats/corpus.git && cd ~/corpus/ && ~/opt/gradle/bin/gradle clean ai:jar ai:shadowJar
