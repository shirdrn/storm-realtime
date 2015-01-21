package org.shirdrn.storm.analytics.common;

import java.io.Serializable;


public interface TimeoutCache<C, K, V> extends Serializable {

	void put(C client, K key, V value, int seconds);
	V get(C client, K key);
}
