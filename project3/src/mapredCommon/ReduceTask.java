package mapredCommon;

import java.util.List;

import jobtracker.TaskTrackerInfo;

public class ReduceTask extends Task{
	private List<TaskTrackerInfo> taskTrackers;
	
	public ReduceTask(int taskId, String tmpDir, Job job,
				List<TaskTrackerInfo> taskTrackers) {
		super(taskId, job);
		this.taskTrackers = taskTrackers;
	}

	@Override
	public void run(String tmpDir) throws Exception {
		
	}
}
