package org.shirdrn.storm.api;

/**
 * Combined multiple fileds' value, a new key is got.
 * 
 * @author yanjun
 */
public interface KeyCreateable {

	String createKey(String type);
}
