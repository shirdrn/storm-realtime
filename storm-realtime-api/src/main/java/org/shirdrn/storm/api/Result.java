package org.shirdrn.storm.api;

import java.io.Serializable;


public abstract class Result implements Comparable<Result>, Serializable {

	private static final long serialVersionUID = 1L;
	protected int indicator;
	protected CallbackHandler<?> callbackHandler;

	public <T> void setCallbackHandler(CallbackHandler<T> callbackHandler) {
		this.callbackHandler = callbackHandler;
	}
	
	@SuppressWarnings("unchecked")
	public <T> CallbackHandler<T> getCallbackHandler() {
		return (CallbackHandler<T>) callbackHandler;
	}
	
	public int getIndicator() {
		return indicator;
	}
	
	public void setIndicator(int indicator) {
		this.indicator = indicator;
	}
	
	@Override
	public int compareTo(Result o) {
		if(this.indicator < o.indicator) {
			return -1;
		}
		if(this.indicator > o.indicator) {
			return 1;
		}
		return 1;
	}
}
