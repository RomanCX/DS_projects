default:
	javac -cp . -d ../bin datanode/*.java
	javac -cp . -d ../bin example1/*.java
	javac -cp . -d ../bin mapredClient/*.java
	javac -cp . -d ../bin namenode/*.java
	javac -cp . -d ../bin tasktracker/*.java
	javac -cp . -d ../bin dfsClient/*.java
	javac -cp . -d ../bin jobtracker/*.java
	javac -cp . -d ../bin mapredCommon/*.java
	javac -cp . -d ../bin protocols/*.java
	javac -cp . -d ../bin example2/*.java
	cd ../bin;jar cvf DegreeCount.jar example1

clean:
	rm -rf ../bin/datanode
	rm -rf ../bin/namenode
	rm -rf ../bin/tasktracker
	rm -rf ../bin/jobtracker
	rm -rf ../bin/protocols
	rm -rf ../bin/mapredCommon
	rm -rf ../bin/mapredClient
	rm -rf ../bin/dfsClient
	rm -rf ../bin/example1
	rm -rf ../bin/example2
	rm -f ../bin/DegreeCount.jar
	rm -rf ../bin/tmp
