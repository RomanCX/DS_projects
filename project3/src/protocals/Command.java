package protocals;

import java.util.ArrayList;
import java.util.List;

import namenode.DatanodeInfo;

public class Command {
	
	public Operation operation;
	private List<DatanodeInfo> targets;
	private List<Integer> blockIds;
	
	public Command(Operation operation, List<Integer> blockIds) {
		this.operation = operation;
		this.blockIds = blockIds;
	}
	
	public List<DatanodeInfo> getTargets() {
		return targets;
	}
	
	public List<Integer> getBlockIds() {
		return blockIds;
	}
}
