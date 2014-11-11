package protocals;

public class DnRegistration {
	int nodeId;
	int interval;
	public DnRegistration(int nodeId, int interval) {
		this.nodeId = nodeId;
		this.interval = interval;
	}
	
	public int getNodeId() {
		return nodeId;
	}
	
	public int getInterval() {
		return interval;
	}
}
