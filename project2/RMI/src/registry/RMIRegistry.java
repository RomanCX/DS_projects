/*
 * Author: Siyuan Zhou, Zichang Feng
 * The RMIRegistry is the registry running on the server side.
 * It keeps a copy of Registry and thus maintains a object table.
 * It is responsible for listens to remote method invocation requests,
 * invoke methods and sending responding messages.
 */

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
	//The registry keeps the object table.
	private RegistryImp registry;
	private ServerSocket listenerSocket;
	public static void main(String[] args) {
		RMIRegistry rmiRegistry = new RMIRegistry();
		rmiRegistry.run();
		
	}
	
	//The constructor initialize class fields
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
	
	//After binding the registry itself, keeps waiting for requests
	//and responding to them
	public void run() {
		registry.bindSelf();
		while(true) {
			Socket newSocket = null;
			try {
				newSocket = listenerSocket.accept();
				//The object streams are to receive and send messages
				ObjectOutputStream objectOutputStream = 
						new ObjectOutputStream(newSocket.getOutputStream());
				ObjectInputStream objectInStream = new ObjectInputStream(newSocket.getInputStream());
				RMIClientMessage inMessage = (RMIClientMessage) objectInStream.readObject();
				//Do the real invocation
				RMIServerMessage outMessage = invokeMethod(inMessage);
				objectOutputStream.writeObject(outMessage);
				objectOutputStream.flush();
			} catch (IOException | ClassNotFoundException e) {
				e.printStackTrace();
			}
			
		}
		
	}
	
	/*
	 * Do the real invocation. It looks for the object, the method
	 * and invoke the method with parameters.
	 * In the end, this function assemble a RMIServerMessage as response
	 * with return value or the exception occurred.
	 */
	private RMIServerMessage invokeMethod(RMIClientMessage inMessage) {
		Remote object = registry.getObject(inMessage.getObjectName());
		if (object == null) {
			return new RMIServerMessage("Object " 
				+ inMessage.getObjectName() + " not found.");
		}
		Object[] parameters = inMessage.getParameters();

		try {
			/*
			 * We didn't use the getMethod from java because the java getMethod
			 * requires exact match for parameter types. However, the parameters 
			 * we get might be extended classes from what's declared in method.
			 */
			Method method = getMethodByName(object, inMessage.getMethodName());
			if (method == null) {
				throw new NoSuchMethodException();
			}
			System.out.println("Invoking " + object.getClass().getName() 
					+ "." + method.getName() + " with parameters:");
			for (Object parameter : parameters) {
				System.out.println("\t" + parameter.toString());
			}
			Object returnObject = method.invoke(object, parameters);
			return new RMIServerMessage(returnObject);
		} catch (NoSuchMethodException e) {
			return new RMIServerMessage("Method " 
				+ inMessage.getMethodName() + " not found");
		} catch (SecurityException e) {
			return new RMIServerMessage("Access to method " 
					+ inMessage.getMethodName() + " denied");
		} catch (IllegalAccessException e) {
			e.printStackTrace();
			return new RMIServerMessage("Trying to access unaccessible class");
		} catch (IllegalArgumentException e) {
			return new RMIServerMessage("Illegal arguments");
		} catch (InvocationTargetException e) {
			return new RMIServerMessage("The target method throws an exception: \n"
					+ e.getMessage());
		}
	}
	
	/*
	 * This function searches for the methodName. If match, returns the method
	 * Only works if there's exactly one such method.
	 */
	private Method getMethodByName(Object object, String methodName) {
		Method[] ms = object.getClass().getMethods();
		for (int i = 0; i < ms.length; i++) {
			if (ms[i].getName().equals(methodName))
				return ms[i];
		}
		return null;
	}

	

}
