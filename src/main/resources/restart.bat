@echo off

title DEVELOP ENVIRONMENT START/RESTART (=_=)\

for /f "tokens=2" %%I in ('tasklist /nh /fi "imagename eq java.exe" /fi "windowtitle ne visor*"') do taskkill /f /pid %%I

timeout /T 5 /NOBREAK

 

taskkill /F /FI "WINDOWTITLE eq KAFKA" /IM cmd.exe

taskkill /F /FI "WINDOWTITLE eq ZOOKEEPER" /IM cmd.exe

taskkill /F /FI "WINDOWTITLE eq WILDFLY*" /IM cmd.exe

 

set K_HOME=%~dp0kafka
 

echo Deleting Kafka logs

rmdir /S /Q %K_HOME%\logs\kafka-logs

rmdir /S /Q %K_HOME%\logs\zookeeper

del /F /Q %K_HOME%\logs\*

echo Starting Zookeeper

cd %K_HOME%

echo %K_HOME%\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties

start "ZOOKEEPER" %K_HOME%\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties

timeout /T 5 /NOBREAK



echo Starting Kafka

start "KAFKA" %K_HOME%\bin\windows\kafka-server-start.bat .\config\server.properties

timeout /T 5 /NOBREAK

timeout /T 5

cd ..\