package org.shirdrn.storm.live.common;

import java.io.Serializable;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;

public class HeartbeatEvent implements Serializable {

	private static final long serialVersionUID = 1L;
	private String room;
	private String udid;
	private Tuple input;
	private OutputCollector collector;
	private String eventCode;
	private String fragmentId;
	private String eventTime;
	private long eventTimestamp;
	
	public long getEventTimestamp() {
		return eventTimestamp;
	}
	public void setEventTimestamp(long eventTimestamp) {
		this.eventTimestamp = eventTimestamp;
	}
	public String getFragmentId() {
		return fragmentId;
	}
	public void setFragmentId(String fragmentId) {
		this.fragmentId = fragmentId;
	}
	
	public Tuple getInput() {
		return input;
	}
	public void setInput(Tuple input) {
		this.input = input;
	}
	public OutputCollector getCollector() {
		return collector;
	}
	public void setCollector(OutputCollector collector) {
		this.collector = collector;
	}
	public String getRoom() {
		return room;
	}
	public void setRoom(String room) {
		this.room = room;
	}
	public String getEventCode() {
		return eventCode;
	}
	public void setEventCode(String eventCode) {
		this.eventCode = eventCode;
	}
	public String getUdid() {
		return udid;
	}
	public void setUdid(String udid) {
		this.udid = udid;
	}
	public String getEventTime() {
		return eventTime;
	}
	public void setEventTime(String eventTime) {
		this.eventTime = eventTime;
	}
	
	
}
