package messages;

import java.io.Serializable;

public class RMIServerMessage implements Serializable{
	private static final long serialVersionUID = -8270255187769367740L;
	
	private Object returnValue;
	private Exception exception;
	
	public void setReturnValue(Object returnValue) {
		this.returnValue = returnValue;
	}
	
	public void setException(Exception exception) {
		this.exception = exception;
	}
	
	public Object getReturnValue() {
		return returnValue;
	}
	
	public Exception getException() {
		return exception;
	}
}
