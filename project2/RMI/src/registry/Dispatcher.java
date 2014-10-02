package registry;
import java.lang.reflect.Method;

import messages.*;

public class Dispatcher {
	private Registry registry;
	
	public Dispatcher(Registry registry) {
		this.registry = registry;
	}
	
	public RMIServerMessage dispatch(RMIClientMessage msg) {
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
		ret.setReturnValue(returnValue);
		return ret;
	}
}