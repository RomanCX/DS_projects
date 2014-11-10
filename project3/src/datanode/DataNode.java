package datanode;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.ObjectInputStream;
import java.io.FileInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import protocals.Command;
import protocals.DatanodeProtocal;
import protocals.Host;
import protocals.Operation;

public class DataNode implements Runnable {
	
	// configuration file for datanode
	private static final String cnfFile = "datanode.cnf";
	//host name of namenode
	private String namenodeAddr; 
	//host name of datanode
	private String myAddr; 
	//port number of datanode
	private int myPort; 
	//directory to store data files
	private String dataDir; 
	// id of this node
	private int nodeId;
	// block list
	private Map<Integer, String> blockMap; 
	// interval of hearbeat;
	private long heartBeatInterval; 
	// a remote object contains methods used to communicate with namenode
	private DatanodeProtocal namenode; 
	
	public DataNode() {
		
	}
	
	/* Load configuration for datanode from file */
	private void loadConfiguration() {
		Properties pro = new Properties();
		try {
			pro.loadFromXML(new FileInputStream(cnfFile));
			namenodeAddr = pro.getProperty("dfs.namenode");
			dataDir = pro.getProperty("dfs.data.dir");
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	/* Start datanode */
	private void start() {
		loadConfiguration();
		
		try {
			// pick a free port
			ServerSocket tmp = new ServerSocket(0);
			myPort = tmp.getLocalPort();
			
			// register datanode on namenode 
			Registry registry = LocateRegistry.getRegistry(namenodeAddr);
			namenode = (DatanodeProtocal)registry.lookup("namenode");
			myAddr = InetAddress.getLocalHost().getHostName();
			DnRegistration dr = namenode.register(myAddr, myPort);
			nodeId = dr.getNodeId();
			heartBeatInterval = dr.getInterval();
			
			// load image of datanode from file if it exists
			File f = new File(dataDir + "/datanode-image");
			if (f.exists()) {
				ObjectInputStream ois = new ObjectInputStream(new FileInputStream(f));
				blockMap = (Map<Integer, String>)ois.readObject();
				ois.close();
			}
			else {
				blockMap = new HashMap<Integer, String>();
			}
			
			// start listening
			new Thread(this).start();
		} catch (Exception e) {
			System.out.println("datanode fails to start");
			System.exit(1);
		}
		
		
	}
	
	private void offerService() {
		Command command = null;
		long timeLeft, lastBeat, now;
		
		lastBeat = 0;
		do {
			lastBeat = System.currentTimeMillis();
			command = namenode.heartBeat(nodeId, blockMap);
			switch (command.operation) {
			case FETCH_DATA:
				fetch_data(command);
				break;
			case DELETE_DATA:
				delete_data(command);
				break;
			default:
				break;
			}
			
			// sleep heartBeatInterval time and do next heart beat
			now = System.currentTimeMillis();
			timeLeft = lastBeat + heartBeatInterval - now;
			while (timeLeft > 0) {
				try {
					Thread.sleep(timeLeft);
				} catch (InterruptedException e) {
					// do nothing
				}	
				now = System.currentTimeMillis();
				timeLeft = lastBeat + heartBeatInterval - now;
			}
			
		} while(true);
	}
	
	/* Write the data into local file */
	private void writeBlock(String fileName, String data) {
		try {
			FileWriter fw = new FileWriter(fileName);
			fw.write(data);
			fw.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/* Fetch blocks from other datanodes */
	private void fetch_data(Command command) {
		ArrayList<Host> targets = command.getTargets();
		ArrayList<Integer> blockIds = command.getBlockIds();
		
		Map<Host, ArrayList<Integer>> m = 
				new HashMap<Host, ArrayList<Integer>>();
		
		// aggregate blockIds by datanode
		for (int i = 0; i < targets.size(); ++i) {
			if (!m.containsKey(targets.get(i))) {
				ArrayList<Integer> tmp = new ArrayList<Integer>();
				tmp.add(blockIds.get(i));
				m.put(targets.get(i), tmp);
			}
			else {
				m.get(targets.get(i)).add(blockIds.get(i));
			}
		}
		
		// get blocks from each datanode
		Iterator iter = m.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry)iter.next();
			Host target = (Host)entry.getKey();
			blockIds = (ArrayList<Integer>)entry.getValue();
			
			try (Socket socket = new Socket(target.getAddress(), target.getPort());
				ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
				ObjectInputStream ois = new ObjectInputStream(socket.getInputStream())) {
				
				oos.writeObject(new Command(Operation.READ_DATA, blockIds));
				ArrayList<Block> blocks = (ArrayList<Block>)ois.readObject();
				
				// write the blocks got from other datanode to local disk
				// and update the map of block to filename
				for (int i = 0; i < blocks.size(); ++i) {
					String fileName = "block" + blocks.get(i).getId();
					writeBlock(fileName, blocks.get(i).getData());
					blockMap.put(blocks.get(i).getId(), fileName);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	/* Delete blocks in this datanode */
	private void delete_data(Command command) {
		ArrayList<Integer> blockIds = command.getBlockIds();
		
		for (int i = 0; i < blockIds.size(); ++i) {
			File f = new File(blockMap.get(blockIds.get(i)));
			// To do: need lock
			f.delete();
			blockMap.remove(blockIds.get(i));
		}
	}
	
	/* Send blocks to client or other datanodes */
	private void transfer_data(ObjectOutputStream oos, Command command) {
		ArrayList<Integer> blockIds = command.getBlockIds();
		ArrayList<Block> blocks = new ArrayList<Block>();
		
		// read every blocks from local disk
		for (int i = 0; i < blockIds.size(); ++i) {
			//To do, may need lock
			File f = new File(blockMap.get(blockIds.get(i)));
			StringBuilder sb = new StringBuilder();
			
			try (BufferedReader br = new BufferedReader(new FileReader(f))) {
				String line;
				while ((line = br.readLine()) != null) {
					sb.append(line);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			blocks.add(new Block(blockIds.get(i), sb.toString()));
		}
		
		try {
			oos.writeObject(blocks);
			oos.flush();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/* Receive blocks from client or other datanodes */
	private void receive_data(ObjectInputStream ois) {
		try {
			ArrayList<Block> blocks = (ArrayList<Block>)ois.readObject();
			for (int i = 0; i < blocks.size(); ++i) {
				String fileName = "block" + blocks.get(i).getId();
				writeBlock(fileName, blocks.get(i).getData());
				// To do, may need lock
				blockMap.put(blocks.get(i).getId(), blocks.get(i).getData());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void run() {
		ServerSocket listen = null;
		try {
			listen = new ServerSocket(myPort);
		} catch (Exception e) {
			System.out.println("unable to listen on port " + myPort);
			return;
		}
		
		while (true) {
			try {
				Socket clientSocket = listen.accept();
				ObjectInputStream ois = new 
						ObjectInputStream(clientSocket.getInputStream());
				ObjectOutputStream oos = 
						new ObjectOutputStream(clientSocket.getOutputStream());
				Command command = (Command)ois.readObject();
				switch(command.operation) {
				case READ_DATA:
					transfer_data(oos, command);
					break;
				case WRITE_DATA:
					receive_data(ois);
					oos.writeObject("done");
					oos.flush();
					break;
				default:
					// do nothing
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
	}
	
	public static void main(String[] args) {
		DataNode datanode = new DataNode();
		datanode.start();
		datanode.offerService();
	}
	
}
