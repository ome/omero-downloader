#!/usr/bin/env sh

SCRIPT_DIR=`dirname "$0"`
JAR_FILE=downloader-jar-with-dependencies.jar

if [ -r "$SCRIPT_DIR"/$JAR_FILE ]
then java -jar "$SCRIPT_DIR"/$JAR_FILE "$@"
elif [ -r "$SCRIPT_DIR"/target/$JAR_FILE ]
then java -jar "$SCRIPT_DIR"/target/$JAR_FILE "$@"
else echo Failed to find JAR file named $JAR_FILE
fi
