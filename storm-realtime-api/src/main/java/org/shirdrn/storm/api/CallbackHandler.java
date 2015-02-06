package org.shirdrn.storm.api;

import java.io.Serializable;


public interface CallbackHandler<T> extends Serializable {

	void call(T client) throws Exception;
	
}
