java -cp . mapredClient.JobClient -jar DegreeCount.jar -input /graphs -output /output-degree -reducenum 2 -delim ,
java -cp . dfsClient.Client < get_degree.txt
