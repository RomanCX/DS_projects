package datanode;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
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

import protocals.Command;
import protocals.DatanodeProtocal;
import protocals.Host;

public class DataNode implements Runnable {
	private String namenodeAddr; //host name of namenode
	private int namenodePort; //port number of namenode
	
	private String myAddr; //host name of datanode
	private int myPort; //port number of datanode
	
	private String dataDir; //Directory to store data files
	private int nodeId; // id of this node
	private Map<Integer, String> blockMap; // block list
	
	private long heartBeatInterval; // Interval of hearbeat;
	// a remote object contains methods used to communicate with namenode
	private DatanodeProtocal namenode; 
	
	public DataNode() {
		
	}
	
	private void loadConfiguration() {
		
	}
	
	private void start() {
		loadConfiguration();
		
		try {
			Registry registry = LocateRegistry.getRegistry(namenodeAddr);
			namenode = (DatanodeProtocal)registry.lookup("namenode");
			myAddr = InetAddress.getLocalHost().getHostName();
			DnRegistration dr = namenode.register(myAddr, myPort);
			nodeId = dr.getNodeId();
			heartBeatInterval = dr.getInterval();
			
			File f = new File(dataDir + "/datanode-image");
			if (f.exists()) {
				ObjectInputStream ois = new ObjectInputStream(new FileInputStream(f));
				blocks = (Map<Integer, String>)ois.readObject();
				ois.close();
			}
			else {
				blocks = new HashMap<Integer, String>();
			}
		} catch (Exception e) {
			System.out.println("datanode fails to start");
			System.exit(1);
		}
	}
	
	private void offerService() {
		Command command = null;
		long timeLeft, lastBeat, now;
		
		do {
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
			timeLeft = heartBeatInterval;
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
	
	/* Fetch blocks from other datanodes */
	private void fetch_data(Command command) {
		ArrayList<Host> targets = command.getTargets();
		ArrayList<Integer> blockIds = command.getBlockIds();
		
		Map<Host, ArrayList<Integer>> m;
		
		// aggregate blockIds by datanode
		for (int i = 0; i < targets.size(); ++i) {
			if (!m.containsKey(targets.get(i))) {
				ArrayList<Integer> tmp;
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
				
				oos.writeObject(new Command(READ_DATA, blockIds));
				ArrayList<Block> blocks = (ArrayList<Block>)ois.readObject();
				
				// write the blocks got from other datanode to local disk
				// and update the map of block to filename
				for (int i = 0; i < blocks.size(); ++i) {
					String fileName = "block" + blocks.get(i).getId();
					writeBlock(fileName, blocks.get(i).getData());
					blockMap.put(blocks.get(i).getId(), fileName);
				}
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
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/* Receive blocks from client or other datanodes */
	private void receive_data(ObjectInputStream ois) {
		
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
				ObjectInputStream ois = new ObjectInputStream(clientSocket.getInputStream());
				Command command = (Command)ois.readObject();
				switch(command.operation) {
				case READ_DATA:
					ObjectOutputStream oos = 
						new ObjectOutputStream(clientSocket.getOutputStream());
					transfer_data(oos, command);
					break;
				case WRITE_DATA:
					receive_data(ois);
					break;
				default:
					// do nothing
				}
			} catch (Exception e) {
				
			}
		}
		
	}
	
	public static void main(String[] args) {
		DataNode datanode = new DataNode();
		datanode.start();
		datanode.offerService();
	}
	
}
