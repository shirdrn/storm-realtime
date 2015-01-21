package org.shirdrn.storm.analytics.common;

import java.io.Serializable;


public interface LazyCallback<T> extends Serializable {

	void call(T client) throws Exception;
	
}
