package org.shirdrn.storm.api;

import java.io.Serializable;

/**
 * Package useful event data and a {@link CallbackHandler} object
 * after computing for a specified indicator.
 * 
 * @author Yanjun
 */
public interface Result extends Comparable<Result>, Serializable {

	<T> void setCallbackHandler(CallbackHandler<T> callbackHandler);
	
	<T> CallbackHandler<T> getCallbackHandler();
	
	int getIndicator();
	
	void setIndicator(int indicator);

}
