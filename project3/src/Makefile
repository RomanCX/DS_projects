default:
	mkdir ../bin
	javac -cp . -d ../bin datanode/*.java
	javac -cp . -d ../bin examples/*.java
	javac -cp . -d ../bin mapredClient/*.java
	javac -cp . -d ../bin namenode/*.java
	javac -cp . -d ../bin tasktracker/*.java
	javac -cp . -d ../bin dfsClient/*.java
	javac -cp . -d ../bin jobtracker/*.java
	javac -cp . -d ../bin mapredCommon/*.java
	javac -cp . -d ../bin protocals/*.java
	javac -cp . -d ../bin example2/*.java
	jar cvf ../bin/example.jar ../bin/examples

clean:
	$(RM) *.class
