#!/usr/bin/env bash
############################
### Install Google Cloud ###
############################

## Apps
sudo apt-get update
sudo apt-get install -y htop

## .bashrc 
echo "alias submit2='cd ~/corpus/ && git pull && gradle shadowJar && spark-submit --class no.habitats.corpus.spark.SparkUtil build/libs/corpus-all.jar local=false rdd=local '" >> ~/.bashrc
echo "alias submit='spark-submit --class no.habitats.corpus.spark.SparkUtil --jars ~/corpus/build/libs/corpus-all.jar ~/corpus.jar local=false rdd=local '" >> ~/.bashrc

# Git
mv ~/.ssh/github_rsa ~/.ssh/id_rsa
chmod 600 ~/.ssh/id_rsa
chmod 600 ~/.ssh/google_compute_engine
eval $(ssh-agent -s) && ssh-agent bash -c 'ssh-add ~/.ssh/id_rsa; yes | git clone http://github.com/Habitats/corpus.git'
rm corpus-archive.*
wget --progress=bar:force:noscroll https://www.dropbox.com/s/n0750x98l0hsrhp/corpus-archive.zip
unzip -o corpus-archive.zip
mv models ~/corpus/
git config --global user.email "mail@habitats.no"
git config --global user.name "Patrick Skjennum"
cd corpus && git remote set-url origin git@github.com:Habitats/corpus.git && cd ..

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
scp -oStrictHostKeyChecking=no -i ~/.ssh/google_compute_engine -r ~/data habispam@spark-w-0:~/
scp -oStrictHostKeyChecking=no -i ~/.ssh/google_compute_engine -r ~/data habispam@spark-w-1:~/
scp -oStrictHostKeyChecking=no -i ~/.ssh/google_compute_engine -r ~/data habispam@spark-w-2:~/
scp -oStrictHostKeyChecking=no -i ~/.ssh/google_compute_engine -r ~/data habispam@spark-w-3:~/
