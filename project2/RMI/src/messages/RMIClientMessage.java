package messages;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;


public class RMIClientMessage implements Serializable {
	private String objectName;
	private String methodName; 
	private Object[] parameters; 
	
	public RMIClientMessage(String objectName, String methodName, Object[] parameters) {
		this.objectName = objectName;
		this.methodName = methodName;
		this.parameters = parameters;
	}
	
	public String getMethodName() {
		return methodName;
	}
	
	public Object[] getParameters() {
		return parameters;
	}
	
	public String getObjectName() {
		return objectName;
	}
	
	/*public static void main(String []args) {
		Proxy p = new Proxy();
		
		System.out.println(p.getMessage());
		System.out.println(p.reverse("abcdefg"));
	}*/
}

/*class Proxy {
	private Dispatcher dispatcher;
	public Proxy() {
		dispatcher = new Dispatcher();
	}
	
	public String getMessage() {
		RMIMessage msg = new RMIMessage("Demo", "getMessage", new Object[]{});
		RMIMessage returnMsg = dispatcher.dispatch(msg);
		return (String)returnMsg.getReturnValue();
	}
	
	public String reverse(String str) {
		RMIMessage msg = new RMIMessage("Demo", "reverse", new Object[]{str});
		RMIMessage returnMsg = dispatcher.dispatch(msg);
		return (String)returnMsg.getReturnValue();
	}
}



class Demo {
	public String getMessage() {
		return "hello world";
	}
	
	public String reverse(String str) {
		String res = new String();
		
		for (int i = str.length() - 1; i >=0; --i) {
			res += str.charAt(i);
		}
		
		return res;
	}
}*/
