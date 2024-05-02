# compile all source files
build:
	mvn clean package

# run tests
final:
	mvn test

test:
	mvn test

# generate documentation for lab4
docs:
	mvn javadoc:javadoc
