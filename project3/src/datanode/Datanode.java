package datanode;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
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
import java.util.List;
import java.util.Map;
import java.util.Properties;

import namenode.DatanodeInfo;
import protocals.Command;
import protocals.DatanodeProtocal;
import protocals.DnRegistration;
import protocals.Operation;

public class Datanode implements Runnable {
	
	// configuration file for datanode
	private static final String cnfFile = "datanode.cnf";
	//host name of namenode
	private String namenodeAddr; 
	// port numberof namenode
	private int namenodePort;
	// host name of datanode
	private String myAddr; 
	// port number of datanode
	private int myPort; 
	// directory to store data files
	private String dataDir; 
	// id of this node
	private int nodeId;
	// block list
	private Map<Integer, String> blockMap; 
	// interval of hearbeat;
	private long heartBeatInterval; 
	// a remote object contains methods used to communicate with namenode
	private DatanodeProtocal namenode; 
	// indicate if datanode should stop running
	private boolean stop;
	// path for datanode image
	private String imageFile;
	
	public Datanode() {
		stop = false;
	}
	
	/* Load configuration of datanode from file */
	private void loadConfiguration() {
		Properties pro = new Properties();
		try {
			pro.loadFromXML(new FileInputStream(cnfFile));
			// assume the format of fs.default.name is dfs://hostname:port
			String name = pro.getProperty("fs.default.name");
			int pos1 = name.indexOf("//");
			int pos2 = name.indexOf(":", pos1 + 2);
			namenodeAddr = name.substring(pos1 + 2, pos2);
			namenodePort = Integer.parseInt(name.substring(pos2 + 1));
			
			// format name and datadir
			dataDir = pro.getProperty("dfs.data.dir");
			int length = dataDir.length();
			if (length > 1 && dataDir.charAt(length - 1) == '/')
				dataDir = dataDir.substring(0, length - 1);
			
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
			imageFile = dataDir + "/datanode-image";
			File f = new File(imageFile);
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
		List<Command> commands = null;
		long timeLeft, lastBeat, now;
		
		lastBeat = 0;
		do {
			lastBeat = System.currentTimeMillis();
			commands = namenode.heartBeat(nodeId, blockMap);
			for (Command command : commands) {
				switch (command.operation) {
				case FETCH_DATA:
					fetchData(command.getBlockId(), command.getTarget());
					break;
				case DELETE_DATA:
					deleteData(command.getBlockId());
					break;
				case SHUT_DOWN:
					shutDown();
				default:
					break;
				}
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
			
		} while(stop == false);
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
	
	/* Fetch block from other datanode */
	private void fetchData(int blockId, DatanodeInfo target) {
		try (Socket socket = new Socket(target.getAddress(), target.getPort());
			ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
			ObjectInputStream ois = new ObjectInputStream(socket.getInputStream())) {
				
			oos.writeObject(new Command(Operation.READ_DATA, blockId, null));
			Block block = (Block)ois.readObject();
				
			// write the block got from other datanode to local disk
			// and update the map of block to filename
			String fileName = dataDir + "/block" + block.getId();
			writeBlock(fileName, block.getData());
			blockMap.put(block.getId(), fileName);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/* Delete blocks in this datanode */
	private void deleteData(int blockId) {
		File f = new File(blockMap.get(blockId));
		// To do: need lock
		f.delete();
		blockMap.remove(blockId);
	}
	
	/* Send block to client or other datanode */
	private void transferData(ObjectOutputStream oos, int blockId) {
		// read block from local file
		//To do, may need lock
		File f = new File(blockMap.get(blockId));
		StringBuilder sb = new StringBuilder();
			
		try (BufferedReader br = new BufferedReader(new FileReader(f))) {
			String line;
			while ((line = br.readLine()) != null) {
				sb.append(line);
			}
			oos.writeObject(new Block(blockId, sb.toString()));
			oos.flush();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/* Receive block from client or other datanodes */
	private boolean receiveData(ObjectInputStream ois) {
		try {
			Block block = (Block)ois.readObject();
			String fileName = dataDir + "/block" + block.getId();
			writeBlock(fileName, block.getData());
			// To do, may need lock
			blockMap.put(block.getId(), fileName);
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	/*
	 *  Shut down datanode
	 */
	private void shutDown() {
		System.out.println("Shutting down...");
		stop = true;
		try (ObjectOutputStream oos = new ObjectOutputStream(
						new FileOutputStream(new File(imageFile)))) {
			oos.writeObject(blockMap);
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
		
		while (stop == false) {
			try {
				Socket clientSocket = listen.accept();
				ObjectInputStream ois = new 
						ObjectInputStream(clientSocket.getInputStream());
				ObjectOutputStream oos = 
						new ObjectOutputStream(clientSocket.getOutputStream());
				Command command = (Command)ois.readObject();
				switch(command.operation) {
				case READ_DATA:
					transferData(oos, command.getBlockId());
					break;
				case WRITE_DATA:
					if (receiveData(ois) == true)
						oos.writeObject("succeed");
					else 
						oos.writeObject("fail");
					oos.flush();
					break;
				default:
					// do nothing
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		try {
			listen.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		Datanode datanode = new Datanode();
		datanode.start();
		datanode.offerService();
	}
	
}
