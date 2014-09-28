import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Constructor;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;

import migratableProcesses.MigratableProcess;

class Slave {
	protected Map<Integer, MigratableProcess> processes;
	protected Map<Integer, Thread> threads;
	protected String masterName;
	protected int masterPort;
	protected int nextID;
	private Lock locker;
	
	public Slave(String masterName, int masterPort, Lock locker) {
		processes = new TreeMap<Integer, MigratableProcess>();
		threads = new HashMap<Integer, Thread>();
		nextID = 0;
		this.masterName = masterName;
		this.masterPort = masterPort;
		this.locker = locker;
	}

	/* Launch process <processName> with arguments <args> */
	public void launchProcess(String processName, String []args) {
		try {
			/* Create an object by using reflection */
			Class<?> c = Class.forName(processName);
			Constructor<?> constructor = c.getConstructor(String[].class);
			MigratableProcess p = 
				(MigratableProcess)constructor.newInstance(new Object[]
																	{args});
			/* Create a thread to run the process */		
			Thread t = new Thread(p);
			locker.lock();
			processes.put(nextID, p);
			threads.put(nextID, t);
			nextID += 1;
			t.start();
			locker.unlock();
			System.out.println("Launch process " + processName + 
							" successfully");
		} catch(ClassNotFoundException e) {
			System.out.println("Can not find class " + processName);
		} catch(NoSuchMethodException e) {
			System.out.println("Can not find a constructor with" + 
							"String[] as parameter");
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	/* List all the running processes */
	public void listProcess() {
		locker.lock();
		if (processes.size() == 0)
			System.out.println("There is no running process");
		else {
			System.out.println("ID\tCommand");
			Iterator it = processes.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry entry = (Map.Entry)it.next();
				int id = (int)entry.getKey();
				MigratableProcess p = (MigratableProcess)entry.getValue();
				System.out.println(id + "\t" + p);
			}
		}
		locker.unlock();
	}
	
	/* Migrate process with processID to targetMachine */
	public void migrateProcess(int processID, String targetMachine) {
		boolean flag = false;
		MigratableProcess p = null;

		locker.lock();
		/* If process is terminated then we are not able to migrate it */
		Thread t = threads.get(processID);
		if (t == null || t.getState() == Thread.State.TERMINATED) {
			System.out.println("Process " + processID + 
							" is no longer running in this machine");
			flag = true;
		}
		/* Otherwise invoke process's suspend() method 
		 * to enter a safe state 
		 */
		else {
			p = processes.get(processID);
			p.suspend();
		}
		locker.unlock();
		
		if (flag) return;
		
		int targetPort = getTargetPort(targetMachine);
		try (Socket socket = new Socket(targetMachine, targetPort);
			ObjectOutputStream oos = new ObjectOutputStream(
												socket.getOutputStream());
			DataInputStream dis = new DataInputStream(
												socket.getInputStream())) {
			/* Send process p to target machine through network */
			oos.writeObject(p);
			oos.flush();
			/* Wait for target machine's acknowledgement */
			dis.readChar();
			/* Remove p and the corresponding thread */
			locker.lock();
			if (processes.containsKey(processID))
				processes.remove(processID);
			if (threads.containsKey(processID))
				threads.remove(processID);
			locker.unlock();
			System.out.println("Process " + processID + 
					" has been migrated to machine " + targetMachine);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/* Get the target machine's port number */
	private int getTargetPort(String slaverName) {
		int port = 0;
		
		try (Socket socket = new Socket(masterName, masterPort);
			PrintStream ps = new PrintStream(socket.getOutputStream());
			DataInputStream dis = new DataInputStream(
												socket.getInputStream())) {
			/* Send request to master */
			ps.println("Lookup " + slaverName);
			ps.flush();
			/* Get response */
			port = dis.readInt();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return port;
	}
}

class Listener implements Runnable {
	private Slave worker;
	private Lock locker;
	
	public Listener(Slave worker, Lock locker) {
		this.worker = worker;
		this.locker = locker;
	}
	
	public void run() {
		ServerSocket listenSocket = null;
		Socket clientSocket = null;
		ObjectInputStream ois = null;
		DataOutputStream dos = null;
		
		try {
			/* Select a free port and register it in master */
			listenSocket = new ServerSocket(0);
			int port = listenSocket.getLocalPort();
			String name = InetAddress.getLocalHost().getHostName();
			System.out.println("Slave name: " + name);
			register(worker.masterName, worker.masterPort, name, port);
			
			/* Listen for migration request coming from remote machine */
			while (true) {
				clientSocket = listenSocket.accept();
				ois = new ObjectInputStream(clientSocket.getInputStream());
				dos = new DataOutputStream(clientSocket.getOutputStream());
				/* Read the process and create a thread to run it */
				MigratableProcess p = (MigratableProcess)ois.readObject();
				Thread t = new Thread(p);
				locker.lock();
				worker.processes.put(worker.nextID, p);
				worker.threads.put(worker.nextID, t);
				worker.nextID += 1;
				t.start();
				locker.unlock();
				dos.writeChar('Y');
				dos.flush();
			}
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	/* Register current IP address and port number in master */
	private void register(String masterName, int masterPort, 
						String IPAddress, int port) {
		try (Socket socket = new Socket(masterName, masterPort);
			PrintStream ps = new PrintStream(socket.getOutputStream());
			DataInputStream dis = new DataInputStream(
												socket.getInputStream());
			) {
			ps.println("Register " + IPAddress + " " + port);
			ps.flush();
			dis.readChar();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

class Poller implements Runnable {
	private Slave target;
	private long frequency;  
	private Lock locker;
	
	public Poller(Slave target, long frequency, Lock locker) {
		this.target = target;
		this.frequency = frequency;
		this.locker = locker;
	}
	
	public void run() {
		/* Poll every process to see if it's alive */
		while (true) {
			locker.lock();
			Iterator it = target.threads.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry entry = (Map.Entry)it.next();
				Thread.State state = ((Thread)entry.getValue()).getState();
				/* Remove terminated process */
				if (state == Thread.State.TERMINATED) {
					target.processes.remove((Integer)entry.getKey());
					it.remove();
				}
			}
			locker.unlock();
			try {
				Thread.sleep(frequency);
			} catch(Exception e) {
				e.printStackTrace();
			}
		}
	}
}
