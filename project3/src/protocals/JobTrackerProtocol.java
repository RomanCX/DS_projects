package protocals;

public class JobTrackerProtocol {
	public HeartBeatResponse heartBeat(List<Integer> finishedTasks, int numSlots);
	public int submitJob(Job job);
}
