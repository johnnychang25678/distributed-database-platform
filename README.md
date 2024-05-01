# Lab 4: Choose-your-Own Distributed System

As indicated in the lab writeup, you have an opportunity to design and implement your own distributed system, service, or application.  There is a lot of flexibility, so we aren't providing you with starter code.  This nearly empty repo should be your starting point, and you can build off any of your previous lab outcomes.  As usual, please use this repo for your project work, so the course staff can help you along the way


## How to run
- To skip test and start server at port 8080: 
```
mvn clean -DskipTests=true package && mvn exec:java -Dexec.mainClass="org.example.Coordinator"
```
- To execute test before compile and start server at port 5000:
```
mvn clean package && mvn exec:java -Dexec.mainClass="org.example.Coordinator" -Dexec.args="5000"
```


## Description of project topic, goals, and tasks

...

## Dependencies to run this code

...

## Description of tests and how to run them

1. Test for...

```
make test
```
