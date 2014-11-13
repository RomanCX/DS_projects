package tasktracker;

import java.rmi.registry.Registry;
import java.util.HashMap;
import java.util.List;

import mapredCommon.Task;

public class TaskTracker {
	int numProcessors;
	HashMap<Integer, Task> runningTasks;
	List<Integer> finishedTasks;
	Registry registry;
	
	public TaskTracker() {
		this.numProcessors = Runtime.getRuntime().availableProcessors();
	}
	
	public void run() {
		System.out.println(numProcessors);
	}
	
	public static void main(String[] args) {
		TaskTracker taskTracker = new TaskTracker();
		taskTracker.run();
	}
	
}
