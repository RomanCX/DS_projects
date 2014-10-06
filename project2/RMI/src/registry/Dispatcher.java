package registry;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.net.Socket;

import remote.Remote;
import remote.RemoteReference;
import messages.*;

public class Dispatcher implements Runnable {
	private Registry registry;
	private int port;
	
	public Dispatcher(Registry registry, int port) {
		this.registry = registry;
		this.port = port;
	}
	
	/* Receive message from client and dispatch it to local object */
	private RMIServerMessage dispatch(RMIClientMessage msg) {
		Object o = registry.getLocalReference(msg.getObjectName());
		Class c = o.getClass();
		Object[] parameters = msg.getParameters();
		Class[] parameterTypes = new Class[parameters.length];
		RMIServerMessage ret;
		Object returnValue;
		
		try {
			Method m = c.getMethod(msg.getMethodName(), parameterTypes);
			returnValue = m.invoke(o, parameters);
		} catch (Exception e) {
			ret.setException(e);
			return ret;
		}
		
		/* If a remote object is returned by the method, then send its
		 * stub to client
		 */
		if ((returnValue instanceof Remote) && 
				!(returnValue instanceof RemoteReference)) {
			returnValue = registry.getRemoteReference(returnValue);
		}
		ret.setReturnValue(returnValue);
		return ret;
	}
	
	public void run() {
		ServerSocket listener = null;
		try {
			listener = new ServerSocket(port);
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
		
		/* Listen for message sent from client */
		while (true) {
			try (Socket socket = listener.accept();
			ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
			ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream())) {
				RMIClientMessage msg = (RMIClientMessage)ois.readObject();
				RMIServerMessage ret = dispatch(msg);
				oos.writeObject(ret);
			} catch(Exception e) {
				e.printStackTrace();
			}
		}
	}
}