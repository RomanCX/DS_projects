package mapredClient;

import java.io.File;
import java.io.FileReader;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.URL;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Enumeration;
import java.util.Properties;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import jobtracker.JobProgress;
import mapredCommon.Job;
import protocols.JobTrackerProtocol;

/**
 * Client program used to submit job to MapReduce system
 * @author RomanC
 *
 */
public class JobClient {
	private static final String mapredCnf = "../conf/mapred.cnf";
	private static final String dfsCnf = "../conf/dfs.cnf";
	private static final long frequency = 2000;
	private static String jobTrackerAddr;
	private static int jobTrackerPort;
	private static JobTrackerProtocol jobTracker;
	
	/*
	 *  Arguments: 
	 *  -input <dfs path of input file>
	 *  -output <dfs path of output file>
	 *  -mapper <path of mapper>
	 *  -reducer  <path of reducer>
	 */
	public static void main(String[] args) throws RemoteException {
		if (init() == false) {
			System.out.println("fail to start client");
			System.exit(1);
		}
		Job job = initJob(args);
		runJob(job);
	}
	
	/**
	 * Initialize client program  
	 * @return true if the initialization succeeds <p> false otherwise
	 */
	public static boolean init() {
		Properties pro = new Properties();
		try {
			pro.load(new FileReader(dfsCnf));
			String name = pro.getProperty("fs.default.name");
			int pos1 = name.indexOf("//");
			int pos2 = name.indexOf(":", pos1 + 2);
			jobTrackerAddr = name.substring(pos1 + 2, pos2);
			pro.clear();
			pro.load(new FileReader(mapredCnf));
			jobTrackerPort = Integer.parseInt(pro.getProperty("jobtracker.port"));
			Registry registry = LocateRegistry.getRegistry(jobTrackerAddr,
															jobTrackerPort);
			jobTracker = (JobTrackerProtocol)registry.lookup("jobtracker");
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	/**
	 * Initialize a job with user specified settings
	 * @param args settings for the job
	 * @return prepared job which can be submitted to MapReduce system
	 */
	public static Job initJob(String[] args) {
		String input = null;
		String output = null;
		String jarFile = null;
		String delim = "\t";
		int numReduceTasks = 0;
		Job job = new Job();
		
		for (int i = 0; i < args.length; ++i) {
			if (args[i].equals("-input")) {
				input = args[i + 1];
				i++;
			}
			else if (args[i].equals("-output")) {
				output = args[i + 1];
				i++;
			}
			else if (args[i].equals("-jar")) {
				jarFile = args[i + 1];
				i++;
			}
			else if (args[i].equals("-reducenum")) {
				numReduceTasks = Integer.parseInt(args[i + 1]);
				i++;
			}
			else if (args[i].equals("-delim")) {
				delim = args[i + 1];
				i++;
			}
			else if (args[i].equals("shutdown")) {
				try {
					jobTracker.shutdown();
					System.exit(0);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			else {
				System.out.println("unknown parameter " + args[i]);
				System.exit(1);
			}
		}
		job.setInputPath(input);
		job.setOutputPath(output);
		job.setJarFile(jarFile);
		job.setDelim(delim);
		job.setNumReduceTasks(numReduceTasks);
		return job;
	}
	
	/**
	 * Submit the job to MapReduce system and wait for its completion.
	 * <p> The progress of job will be reported periodically
	 * @param job job to be run
	 * @throws RemoteException
	 */
	public static void runJob(Job job) throws RemoteException {
		int jobId;
		if ((jobId = jobTracker.submitJob(job)) > 0) {
			System.out.println("Submit job " + jobId);
			long lastMapProgress = -1;
			long lastReduceProgress = 0;
			long currentMapProgress = 0;
			long currentReduceProgress = 0;
			while (true) {
				JobProgress progress = jobTracker.checkProgress(jobId);
				currentMapProgress = Math.round(progress.getMapProgress() * 100);
				currentReduceProgress = Math.round(progress.getReduceProgress() * 100);
				if (currentMapProgress == lastMapProgress &&
						currentReduceProgress == lastReduceProgress)
					continue;
				System.out.print("map task: " + currentMapProgress + "% ");
				System.out.println("reduce task: " + currentReduceProgress + "%");
				if (progress.isFinished()) {
					System.out.println("job finished");
					break;
				}
				lastMapProgress = currentMapProgress;
				lastReduceProgress = currentReduceProgress;
				try {
					Thread.sleep(frequency);
				} catch (InterruptedException e) {
					// ignore
				}
			}
		}
	} 
}
