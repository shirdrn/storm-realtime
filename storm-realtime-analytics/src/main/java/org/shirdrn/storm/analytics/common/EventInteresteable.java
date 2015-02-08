package org.shirdrn.storm.analytics.common;

/**
 * Register interested a set of event code for a filter bolt.
 * 
 * @author Yanjun
 */
public interface EventInteresteable {

	void InterestEventCode(String eventCode);
}
