package namenode;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.rmi.AlreadyBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import protocals.ClientProtocal;
import protocals.Command;
import protocals.DatanodeProtocal;
import protocals.DnRegistration;
import protocals.Operation;


public class Namenode implements DatanodeProtocal, ClientProtocal {
	private HashSet<Integer> availableDatanodes;
	private HashMap<Integer, DatanodeInfo> datanodes;
	private HashMap<String, List<Integer> > files;
	private HashMap<Integer, List<Integer> > blocks;
	private int dataNodeCount;
	private int blockCount;
	private ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
	private long lastWriteToDiskTime;
	private int blockSize;
	private int replicaFactor;
	private String namenodeImageFilename;
	
	public static final int DEFAULT_HEARTBEAT_INTERVAL = 3000;//ms
	public static final int DEFAULT_WRITE_TO_DISK_INTERVAL = 10000;//ms
	public static final int DEFAULT_REPLICA_FACTOR = 3;
	public static final String NAMENODE_RMI_NAME = "namenode";
	public static final String DEFAULT_NAMENODE_IMAGE_FILENAME = "namenode_image";
	public static final int DEFAULT_BLOCK_SIZE = 64 * 1024 * 1024;
	public static final int DEFAULT_REGISTRY_PORT = 33333;
	public static final String CONFIG_FILE_NAME = "namenode.cnf";
	
	
	public Namenode() {
		datanodes = new HashMap<Integer, DatanodeInfo>();//<datanodeId, datanodeInfo>
		files = new HashMap<String, List<Integer>>();//<filename, blockIds>
		blocks = new HashMap<Integer, List<Integer>>();	//<blockId, datanodeIds> 
		availableDatanodes = new HashSet<Integer>();
		dataNodeCount = 0;
		blockCount = 0;
		lastWriteToDiskTime = System.currentTimeMillis();
		blockSize = DEFAULT_BLOCK_SIZE;
		replicaFactor = DEFAULT_REPLICA_FACTOR;
		namenodeImageFilename = DEFAULT_NAMENODE_IMAGE_FILENAME;
	}
	
	
	@Override
	public DnRegistration register(String address, int port) {
		DnRegistration returnValue = null;
		synchronized (datanodes) {
			DatanodeInfo newDatanode = new DatanodeInfo(address, port, dataNodeCount);
			availableDatanodes.add(dataNodeCount);
			datanodes.put(dataNodeCount, newDatanode);
			dataNodeCount++;
			returnValue = new DnRegistration((dataNodeCount - 1), DEFAULT_HEARTBEAT_INTERVAL);
		}
		return returnValue;
	}
	
	@Override
	public Command heartBeat(int nodeId, Map<Integer, String> blocks) {
		// TODO Not fully implemented
		if (System.currentTimeMillis() - lastWriteToDiskTime >= DEFAULT_WRITE_TO_DISK_INTERVAL) {
			writeToDisk();
			lastWriteToDiskTime = System.currentTimeMillis();
		}
		datanodes.get(nodeId).heartBeat();
	    Iterator<Entry<Integer, DatanodeInfo>> it = datanodes.entrySet().iterator();
	    while (it.hasNext()) {
	        Entry<Integer, DatanodeInfo> pairs = it.next();
	        if (pairs.getValue().checkHealth() == DatanodeStatus.DEAD) {
	        	availableDatanodes.remove(pairs.getKey());
	        }
	    }
	    return new Command(Operation.NOOP, new ArrayList<Integer>());
	}

	
	public static void main(String[] args) throws FileNotFoundException, IOException, ClassNotFoundException, AlreadyBoundException {
		Properties pro = new Properties();
		try {
			pro.loadFromXML(new FileInputStream(CONFIG_FILE_NAME));
		} catch(Exception e) {
			e.printStackTrace();
		}
		String imageFilename = pro.getProperty("dfs.name.dir", DEFAULT_NAMENODE_IMAGE_FILENAME);
		String addressPort = pro.getProperty("fs.default.name", "localhost:" + Integer.toString(DEFAULT_REGISTRY_PORT));
		int port = Integer.parseInt(addressPort.substring(addressPort.indexOf(':')));
		Namenode namenode;
		File f = new File(imageFilename);
		if (f.exists()) {
			ObjectInputStream ois = new ObjectInputStream(new FileInputStream(f));
			namenode = (Namenode)ois.readObject();
			ois.close();
		}
		else {
			namenode = new Namenode();
		}
		LocateRegistry.createRegistry(port);
		Registry registry = LocateRegistry.getRegistry();
		registry.bind(NAMENODE_RMI_NAME, namenode);
	}

	private void writeToDisk() {
		File f = new File(namenodeImageFilename);
		try {
			ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(f));
			oos.writeObject(this);
			oos.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public TreeMap<Integer, DatanodeInfo> read(String fileName, String address) {
		TreeMap<Integer, DatanodeInfo> returnValue 
			= new TreeMap<Integer, DatanodeInfo>();
		synchronized (datanodes) {
			rwLock.readLock().lock();;
			List<Integer> fileBlocks = files.get("fileName");
			if (fileBlocks == null) {
				return null;
			}
			for (int blockId : fileBlocks) {
				List<Integer> datanodesContainingBlock = blocks.get(blockId);
				DatanodeInfo selectedDataNode = selectDatanode(address, datanodesContainingBlock);
				returnValue.put(blockId, selectedDataNode);
			}
			rwLock.readLock().unlock();
		}
		return returnValue;
	}

	private DatanodeInfo selectDatanode(String address, List<Integer> datanodesContainingBlock) {
		DatanodeInfo selectedDatanode= null;
		for (int datanodeId : datanodesContainingBlock) {
			if (datanodes.get(datanodeId).getAddress().equals(address)) {
				if (datanodes.get(datanodeId).isAlive()) {
					selectedDatanode = datanodes.get(datanodeId);
				}
			}
		}
		if (selectedDatanode == null) {
			List<Integer> datanodesContainingBlockPerm = new ArrayList<Integer>(datanodesContainingBlock);
			Collections.shuffle(datanodesContainingBlockPerm);
			for (int datanodeId : datanodesContainingBlockPerm) {
				if (datanodes.get(datanodeId).isAlive()) {
					selectedDatanode = datanodes.get(datanodeId);
					
				}
			}
		}
		return selectedDatanode;
	}

	@Override
	public TreeMap<Integer, List<DatanodeInfo>> write(String fileName, int splitNum) {
		TreeMap<Integer, List<DatanodeInfo>> returnValue = 
			new TreeMap<Integer, List<DatanodeInfo>>();
		synchronized (datanodes) {
			rwLock.writeLock().lock();
			int replicaFactor = availableDatanodes.size() < this.replicaFactor ? 
				availableDatanodes.size() : this.replicaFactor;
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
			rwLock.writeLock().unlock();
		}
		return returnValue;
	}


	@Override
	public int getBlockSize() {
		return blockSize;
	}
	
	public void readConfigFile() {
		Properties pro = new Properties();
		try {
			pro.loadFromXML(new FileInputStream(CONFIG_FILE_NAME));
		} catch(Exception e) {
			e.printStackTrace();
		}
		blockSize = Integer.parseInt(pro.getProperty("dfs.block.size", 
				Integer.toString(DEFAULT_BLOCK_SIZE)));
		replicaFactor = Integer.parseInt(pro.getProperty("dfs.replication", Integer.toString(DEFAULT_REPLICA_FACTOR)));
		namenodeImageFilename = pro.getProperty("dfs.name.dir", DEFAULT_NAMENODE_IMAGE_FILENAME);
	}


	@Override
	public void delete(String fileName) {
		//TODO
	}

}
