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

import mapredCommon.Job;
import protocals.JobTrackerProtocol;

public class JobClient {
	private static final String cnfFile = "mapred.cnf";
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
	public static void main(String[] args) {
		if (init() == false) {
			System.out.println("fail to start client");
			System.exit(1);
		}
		Job job = initJob(args);
		
		try {
			int jobId;
			if ((jobId = jobTracker.submitJob(job)) > 0) {
				System.out.println("Submit job " + jobId);
			}
			else {
				System.out.println("fail to submit job");
			}
		} catch (RemoteException e) {
			e.printStackTrace();
			System.out.println("fail to submit job");
		}
	}
	
	public static boolean init() {
		Properties pro = new Properties();
		try {
			pro.load(new FileReader(cnfFile));
			String name = pro.getProperty("jobtracker.name");
			int pos1 = name.indexOf("//");
			int pos2 = name.indexOf(":", pos1 + 2);
			jobTrackerAddr = name.substring(pos1 + 2, pos2);
			jobTrackerPort = Integer.parseInt(name.substring(pos2 + 1));
			Registry registry = LocateRegistry.getRegistry(jobTrackerAddr,
															jobTrackerPort);
			jobTracker = (JobTrackerProtocol)registry.lookup("jobtracker");
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	public static Job initJob(String[] args) {
		String input = null;
		String output = null;
		String mapper = null;
		String reducer = null;
		
		for (int i = 0; i < args.length; ++i) {
			if (args.equals("-input")) {
				input = args[i + 1];
				i++;
			}
			else if (args.equals("-output")) {
				output = args[i + 1];
				i++;
			}
			else if (args.equals("-mapper")) {
				mapper = args[i + 1];
				i++;
			}
			else if (args.equals("-reducer")) {
				reducer = args[i + 1];
				i++;
			}
			else {
				System.out.println("unknown parameter " + args[i]);
				System.exit(1);
			}
		}
		
		return new Job(input, output, mapper, reducer);
	}
}
