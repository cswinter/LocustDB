#!/bin/bash

set -euxo pipefail


if [ "$(uname)" == "Darwin" ]; then
	ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)" < /dev/null 2> /dev/null
	brew install lz4
	brew install capnp
elif [ "$(expr substr $(uname -s) 1 5)" == "Linux" ]; then
	sudo apt-get install -y g++
	sudo apt-get install -y liblz4-dev
	sudo apt-get install -y dpkg
	#sudo add-apt-repository "deb http://archive.ubuntu.com/ubuntu bionic universe" -y
	#sudo add-apt-repository "deb http://archive.ubuntu.com/ubuntu bionic main" -y
	#sudo apt-get update -q
	sudo apt-get install -y capnproto
else
	echo ERROR: Platform not supported
	exit 1
fi


