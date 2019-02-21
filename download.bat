@echo off

set SCRIPT_DIR=%~dp0
set JAR_FILE=downloader-jar-with-dependencies.jar

if exist "%SCRIPT_DIR%\%JAR_FILE%" (
  java -jar "%SCRIPT_DIR%\%JAR_FILE%" %*
) else if exist "%SCRIPT_DIR%\target\%JAR_FILE%" (
  java -jar "%SCRIPT_DIR%\target\%JAR_FILE%" %*
) else (
  echo Failed to find JAR file named %JAR_FILE%
)
