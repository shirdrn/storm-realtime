package org.shirdrn.storm.api;

/**
 * Combined multiple fileds' value, a new key is got.
 * 
 * @author Yanjun
 */
public interface KeyCreateable {

	String createKey(String type);
}
