package migratableProcesses;

import java.io.Serializable;

public abstract class MigratableProcess implements Runnable, Serializable {
	public MigratableProcess() {}
	public MigratableProcess(String []args) {}
	public abstract void suspend();
}