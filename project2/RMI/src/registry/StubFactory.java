/*
 * Author: Siyuan Zhou, Zichang Feng
 * 
 * This class serves as a factory for creating stubs from objects.
 */

package registry;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import remote.RemoteReference;


public class StubFactory {
	
	//Disable the default constructor.
	private StubFactory() {	}
	/*
	 * Creates a stub from an classname(the original class name, not the stub class name),
	 *  the object name, the server host and the port for rmiregister
	 */
	public static RemoteReference createStub(String className, String objName, 
		String host, int port) {
		Class<?> clazz;
		try {
			clazz = Class.forName(className + "_Stub");
			Constructor<?> ctor = clazz.getConstructor(String.class, 
					String.class, int.class);
			Object object = ctor.newInstance(objName, host, port);
			return (RemoteReference)object;
		} catch (ClassNotFoundException | NoSuchMethodException 
				| SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
			e.printStackTrace();

		}
		return null;
	}
	
}
