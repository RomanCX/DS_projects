java -cp . namenode.Namenode &
sleep 1s
java -cp . datanode.Datanode &
java -cp . jobtracker.JobTracker &
sleep 1s
java -cp . tasktracker.TaskTracker &
