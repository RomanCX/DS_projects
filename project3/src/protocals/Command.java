package protocals;

import java.util.ArrayList;
import java.util.List;

import namenode.DatanodeInfo;

public class Command {
	
	public Operation operation;
	private DatanodeInfo target;
	private int blockId;
	
	public Command(Operation operation, int blockId, DatanodeInfo target) {
		this.operation = operation;
		this.blockId = blockId;
		this.target = target;
	}
	
	public Command(Operation operation) {
		this.operation = operation;
	}
	
	public DatanodeInfo getTarget() {
		return target;
	}
	
	public int getBlockId() {
		return blockId;
	}
}
