@echo off
java -classpath ./bin;./lib/mysql-connector-java-5.1.0-bin.jar mongotest.Main mysql update 5000000 10.50.1.135 panabit panabit@mysql test 
pause