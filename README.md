# Lab 4: Choose-your-Own Distributed System

Topic: **Distributed Database System**

## Proposal
Detailed design of our proposal can be found in `Lab_4_Proposal-final.pdf` within the root directory of this repository.

## Prerequisites to run and test:
    - Java 17
    - Maven 3.1 or later
## How to run
- To skip test and start server at port 8080:
```
mvn clean -DskipTests=true package && mvn exec:java -Dexec.mainClass="org.example.Coordinator"
```
- To execute test before compile and start server at port 5000:
```
mvn clean package && mvn exec:java -Dexec.mainClass="org.example.Coordinator" -Dexec.args="5000"
```
## How to test
```
mvn test
```

## View the Generated Javadoc
You can find the generated Javadoc in the target/site/apidocs folder. You can open the index.html file in a web browser to view the Javadoc.
