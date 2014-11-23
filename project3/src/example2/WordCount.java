package example2;

import java.rmi.RemoteException;

import mapredClient.JobClient;
import mapredCommon.Job;

public class WordCount {
	public static void main(String[] args) throws RemoteException {
		Job job = new Job();
		job.setInputPath(args[0]);
		job.setOutputPath(args[1]);
		job.setMapClass(Map.class);
		job.setReduceClass(Reduce.class);
		job.setDelim("\t");
		job.setNumReduceTasks(1);
		JobClient.init();
		JobClient.runJob(job);
	}
}
