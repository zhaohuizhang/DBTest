@echo off
java -classpath ./bin;./lib/mongo-2.6.3.jar mongotest.Main mongo update 5000000 10.50.15.204 mongotest 
pause