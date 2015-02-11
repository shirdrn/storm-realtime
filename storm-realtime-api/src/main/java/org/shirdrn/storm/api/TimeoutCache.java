package org.shirdrn.storm.api;

import java.io.Serializable;

/**
 * Timeout cache interface to cache object with a fix expire time.
 * Usually a cache maybe a Memcached, or Redis, etc.
 * 
 * @author Yanjun
 *
 * @param <C> Connection used to put object into cache.
 * @param <K> Key name for cached object.
 * @param <V> Cached value data.
 */
public interface TimeoutCache<C, K, V> extends Serializable {

	/**
	 * Add object to the cache, with <code>key</code> and <code>value</code>, 
	 * as well as expire time.
	 * @param connection
	 * @param key
	 * @param value
	 * @param expredSecs
	 */
	void put(C connection, K key, V value, int expredSecs);
	
	/**
	 * Given a key, obtain a object from cache.
	 * @param connection
	 * @param key
	 * @return
	 */
	V get(C connection, K key);
}
