package protocals;

import java.io.Serializable;
import java.util.List;

import mapredCommon.Task;

public class HeartBeatResponse implements Serializable  {
	private List<Task> tasks;
	private TaskTrackerOperation operation;
	
	public HeartBeatResponse(List<Task> tasks, TaskTrackerOperation operation) {
		this.tasks = tasks;
		this.operation = operation;
	}
	
	public List<Task> getTasks() {
		return tasks;
	}
	
	public TaskTrackerOperation getOperation() {
		return operation;
	}
}
