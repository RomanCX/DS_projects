package protocals;

import java.io.Serializable;

public enum Operation implements Serializable {
	FETCH_DATA,
	DELETE_DATA,
	READ_DATA,
	WRITE_DATA,
	SHUT_DOWN,
	NOOP
}
