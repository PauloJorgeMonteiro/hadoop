#! /bin/bash
#title           :hadoop-provision.sh
#description     :This script will download and build the hadoop plugin for Eclipse
#author		 :Paulo Monteiro
#date            :20141119
#version         :0.1    
#usage		 :./hadoop-provision.sh "[eclipse_home_path]" "[hadoop_home_path]"
#notes           :Based on Hadoop version 2.5.1


if [ "$#" -ne 2 ]; then
  echo "Illegal number of parameters"
  echo "Usage: ./hadoop-provision.sh '[eclipse_home_path]' '[hadoop_home_path]'"
  exit -1
fi

echo "Installing Hadoop Eclipse plugin ..."

ECLIPSE_DIR=$1
HADOOP_DIR=$2
hadoop_ver="2.5.1"

# Install ant
sudo apt-get -y install ant

if [ ! -f ~/Downloads/master.zip ]; then
  # Download the eclipse plugin source code`
  wget https://github.com/winghc/hadoop2x-eclipse-plugin/archive/master.zip -P ~/Downloads
fi

if [ ! -d ~/Downloads/hadoop2x-eclipse-plugin-master ]; then
  unzip -d ~/Downloads ~/Downloads/master.zip
fi

# Update Jackson version
sed -i 's/jackson.version=1.8.8/jackson.version=1.9.13/g' ~/Downloads/hadoop2x-eclipse-plugin-master/src/ivy/libraries.properties 
sed -i 's/jackson.version=1.8.8/jackson.version=1.9.13/g' ~/Downloads/hadoop2x-eclipse-plugin-master/ivy/libraries.properties
sed -i 's/hadoop.version=2.4.1/hadoop.version=2.5.1/g' ~/Downloads/hadoop2x-eclipse-plugin-master/src/ivy/libraries.properties 
sed -i 's/hadoop.version=2.4.1/hadoop.version=2.5.1/g' ~/Downloads/hadoop2x-eclipse-plugin-master/ivy/libraries.properties

# Build the eclipse plugin JAR and copy the Hadoop plugin to the Eclipse plugins folder
if [ ! -f ~/Downloads/hadoop2x-eclipse-plugin-master/build/contrib/eclipse-plugin/hadoop-eclipse-plugin-2.5.1.jar ]; then 
  cd ~/Downloads/hadoop2x-eclipse-plugin-master/src/contrib/eclipse-plugin
  ant jar -Dversion=${hadoop_ver} -Dhadoop.version=${hadoop_ver} -Declipse.home=${ECLIPSE_DIR} -Dhadoop.home=${HADOOP_DIR}
  cp -p ~/Downloads/hadoop2x-eclipse-plugin-master/build/contrib/eclipse-plugin/hadoop-eclipse-plugin-2.5.1.jar ${ECLIPSE_DIR}/plugins
  rm -rf ~/Downloads/hadoop2x-eclipse-plugin-master
  rm -f ~/Downloads/master.zip
  echo "Installation completed. Please restart Eclipse."
fi
