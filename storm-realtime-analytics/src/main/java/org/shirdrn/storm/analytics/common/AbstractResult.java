package org.shirdrn.storm.analytics.common;

import java.io.Serializable;


public abstract class AbstractResult implements Comparable<AbstractResult>, Serializable {

	private static final long serialVersionUID = 1L;
	protected int indicator;
	protected LazyCallback<?> callback;

	public <T> void setCallback(LazyCallback<T> callback) {
		this.callback = callback;
	}
	
	@SuppressWarnings("unchecked")
	public <T> LazyCallback<T> getCallback() {
		return (LazyCallback<T>) callback;
	}
	
	public int getIndicator() {
		return indicator;
	}
	
	public void setIndicator(int indicator) {
		this.indicator = indicator;
	}
	
	@Override
	public int compareTo(AbstractResult o) {
		if(this.indicator < o.indicator) {
			return -1;
		}
		if(this.indicator > o.indicator) {
			return 1;
		}
		return 1;
	}
}
