package protocals;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

import jobtracker.JobProgress;
import mapredCommon.Job;

public interface JobTrackerProtocol extends Remote {
	public TkRegistration register(String address ,int port) throws RemoteException;
	public HeartBeatResponse heartBeat(List<Integer> finishedTasks, 
			List<Integer> failedTasks, int numSlots, int taskTrackerId) throws RemoteException;
	public int submitJob(Job job) throws RemoteException;
	public JobProgress checkProgress(int jobId) throws RemoteException;
}
