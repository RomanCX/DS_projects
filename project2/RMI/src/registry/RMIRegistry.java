package registry;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.net.Socket;

import messages.RMIClientMessage;
import messages.RMIServerMessage;
import remote.Remote;
import remote.RemoteException;

public class RMIRegistry {

	private RegistryImp registry;
	private ServerSocket listenerSocket;
	public static void main(String[] args) {
		RMIRegistry rmiRegistry = new RMIRegistry();
		rmiRegistry.run();
		
	}
	
	public RMIRegistry() {
		registry = (RegistryImp) LocateRegistry.createRegistry();
		try {
			listenerSocket = new ServerSocket(Registry.REGISTRY_PORT);
		} catch (IOException e) {
			System.err.println("Failed to create listener socket on port: " 
				+ Registry.REGISTRY_PORT);
			System.exit(-1);
		}
	}
	
	public void run() {
		registry.bindSelf();
		while(true) {
			Socket newSocket = null;
			try {
				newSocket = listenerSocket.accept();
				ObjectOutputStream objectOutputStream = 
						new ObjectOutputStream(newSocket.getOutputStream());
				ObjectInputStream objectInStream = new ObjectInputStream(newSocket.getInputStream());
				RMIClientMessage inMessage = (RMIClientMessage) objectInStream.readObject();
				RMIServerMessage outMessage = invokeMethod(inMessage);
				objectOutputStream.writeObject(outMessage);
				objectOutputStream.flush();
			} catch (IOException | ClassNotFoundException e) {
				e.printStackTrace();
			}
			
		}
		
	}
	
	private RMIServerMessage invokeMethod(RMIClientMessage inMessage) {
		Remote object = registry.getObject(inMessage.getObjectName());
		if (object == null) {
			return new RMIServerMessage("Object " 
				+ inMessage.getObjectName() + " not found.");
		}
		Object[] parameters = inMessage.getParameters();
		Class<?>[] parameterTypes = new Class<?>[parameters.length];
		for (int i = 0; i < parameters.length; i++) {
			parameterTypes[i] = parameters[i].getClass();
 		}
		try {
			Method method = object.getClass().getMethod(inMessage.getMethodName(), parameterTypes);
			Object returnObject = method.invoke(object, parameters);
			return new RMIServerMessage(returnObject);
		} catch (NoSuchMethodException e) {
			return new RMIServerMessage("Method " 
				+ inMessage.getMethodName() + " not found");
		} catch (SecurityException e) {
			return new RMIServerMessage("Access to method " 
					+ inMessage.getMethodName() + " denied");
		} catch (IllegalAccessException e) {
			return new RMIServerMessage("Trying to access unaccessible class");
		} catch (IllegalArgumentException e) {
			return new RMIServerMessage("Illegal arguments");
		} catch (InvocationTargetException e) {
			return new RMIServerMessage("The target method throws an exception: \n"
					+ e.getMessage());
		}
	}

	

}
