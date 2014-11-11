package namenode;
import java.rmi.AccessException;
import java.rmi.AlreadyBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import protocals.ClientProtocal;
import protocals.Command;
import protocals.DatanodeProtocal;
import protocals.DnRegistration;


public class Namenode implements DatanodeProtocal, ClientProtocal {
	private HashSet<Integer> availableDatanodes;
	private HashMap<Integer, DatanodeInfo> datanodes;
	private HashMap<String, List<Integer> > files;
	private HashMap<Integer, List<Integer> > blocks;
	private int dataNodeCount;
	private int blockCount;
	private Registry registry;
	
	public static final int DEFAULT_HEARTBEAT_INTERVAL = 3;
	public static final int DEFAULT_REPLICA_FACTOR = 3;
	
	
	public Namenode() {
		availableDatanodes = new HashSet<Integer>();
		datanodes = new HashMap<Integer, DatanodeInfo>();//<datanodeId, datanodeInfo>
		files = new HashMap<String, List<Integer>>();//<filename, blockIds>
		blocks = new HashMap<Integer, List<Integer>>();	//<blockId, datanodeIds> 
		dataNodeCount = 0;
		blockCount = 0;
		try {
			registry = LocateRegistry.getRegistry();
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			registry.bind("namenode", this);
		} catch (AccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (AlreadyBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	
	@Override
	public DnRegistration register(String address, int port) {
		DatanodeInfo newDatanode = new DatanodeInfo(address, port);
		availableDatanodes.add(dataNodeCount);
		datanodes.put(dataNodeCount, newDatanode);
		dataNodeCount++;
		return new DnRegistration((dataNodeCount - 1), DEFAULT_HEARTBEAT_INTERVAL);
	}
	
	@Override
	public Command heartBeat(int nodeId, Map<Integer, String> blocks) {
		// TODO Auto-generated method stub
		return null;
	}
	
	public void run() {
		
	}
	
	
	
	
	public static void main(String[] args) {
		Namenode namenode = new Namenode();
		namenode.run();
	}


	@Override
	public HashMap<DatanodeInfo, List<Integer>> read(String fileName, String address) {
		List<Integer> fileBlocks = files.get("fileName");
		HashMap<DatanodeInfo, List<Integer>> returnValue 
			= new HashMap<DatanodeInfo, List<Integer>>();
		if (fileBlocks == null) {
			return null;
		}
		for (int blockId : fileBlocks) {
			List<Integer> blockOnDatanodes = blocks.get(blockId);
			int selectedDataNode = -1;
			for (int datanodeId : blockOnDatanodes) {
				if (datanodes.get(datanodeId).getAddress().equals(address)) {
					List<Integer> blocksOnReturnedDatanode = returnValue.get(datanodeId);
					if (blocksOnReturnedDatanode == null) {
						blocksOnReturnedDatanode = new ArrayList<Integer>();
						returnValue.put(datanodes.get(datanodeId), blocksOnReturnedDatanode);
					}
					blocksOnReturnedDatanode.add(blockId);
					selectedDataNode = datanodeId;
					break;
				}
			}
			if (selectedDataNode != -1) {
				continue;
			}
			List<Integer> blockOnDatanodesCopy = new ArrayList<Integer>(blockOnDatanodes);
			Collections.shuffle(blockOnDatanodesCopy);
			selectedDataNode = blockOnDatanodesCopy.get(0);
			List<Integer> blocksOnReturnedDatanode = returnValue.get(selectedDataNode);
			if (blocksOnReturnedDatanode == null) {
				blocksOnReturnedDatanode = new ArrayList<Integer>();
				returnValue.put(datanodes.get(selectedDataNode), blocksOnReturnedDatanode);
			}
			blocksOnReturnedDatanode.add(blockId);
			
		}
		return returnValue;
	}


	@Override
	public HashMap<Integer, List<DatanodeInfo>> write(String fileName, int splitNum) {
		HashMap<Integer, List<DatanodeInfo>> returnValue = 
				new HashMap<Integer, List<DatanodeInfo>>();
		int replicaFactor = availableDatanodes.size() < DEFAULT_REPLICA_FACTOR ? 
				availableDatanodes.size() : DEFAULT_REPLICA_FACTOR;
		List<Integer> availableDatanodesPerm = new ArrayList<Integer>(availableDatanodes);
		for (int i = 0; i < splitNum; i++) {
			Collections.shuffle(availableDatanodesPerm);
			List<DatanodeInfo> datanodesForBlock = new ArrayList<DatanodeInfo>();
			returnValue.put(blockCount, datanodesForBlock);
			for (int j = 0; j < replicaFactor; j++) {
				datanodesForBlock.add(datanodes.get(availableDatanodesPerm.get(j)));
			}
			
			blockCount++;
		}
		return returnValue;
	}




}
