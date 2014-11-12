package protocals;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import namenode.DatanodeInfo;

public class Command implements Serializable {
	
	public Operation operation;
	private DatanodeInfo target;
	private int blockId;
	
	public Command(Operation operation, int blockId, DatanodeInfo target) {
		this.operation = operation;
		this.blockId = blockId;
		this.target = target;
	}
	
	public Command() {
		this.operation = Operation.NOOP;
	}
	
	public Command(Operation operation, int blockId) {
		this.operation = operation;
		this.blockId = blockId;
	}
	
	public DatanodeInfo getTarget() {
		return target;
	}
	
	public int getBlockId() {
		return blockId;
	}
}
