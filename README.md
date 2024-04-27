# Lab 4: Choose-your-Own Distributed System

As indicated in the lab writeup, you have an opportunity to design and implement your own distributed system, service, or application.  There is a lot of flexibility, so we aren't providing you with starter code.  This nearly empty repo should be your starting point, and you can build off any of your previous lab outcomes.  As usual, please use this repo for your project work, so the course staff can help you along the way


## How to run
1. At root, start the server with:
```
mvn clean package && mvn exec:java -Dexec.mainClass="org.example.Coordinator"
```
2. At root, start the rmi registry with:
```
rmiregistry -J-Djava.rmi.server.codebase=file:$(pwd)/target/classes/ -J-Djava.class.path=$(pwd)/target/classes/

```
3. Start the server with:
```
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
