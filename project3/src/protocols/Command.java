package protocols;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import namenode.DatanodeInfo;

public class Command implements Serializable {
	
	public DatanodeOperation operation;
	private DatanodeInfo target;
	private int blockId;
	
	public Command(DatanodeOperation operation, int blockId, DatanodeInfo target) {
		this.operation = operation;
		this.blockId = blockId;
		this.target = target;
	}
	
	public Command() {
		this.operation = DatanodeOperation.NOOP;
	}
	
	public Command(DatanodeOperation operation, int blockId) {
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
