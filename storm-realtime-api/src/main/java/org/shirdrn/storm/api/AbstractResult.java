package org.shirdrn.storm.api;

public abstract class AbstractResult implements Result {

	private static final long serialVersionUID = 1L;
	protected int indicator;
	protected CallbackHandler<?> callbackHandler;

	@Override
	public <T> void setCallbackHandler(CallbackHandler<T> callbackHandler) {
		this.callbackHandler = callbackHandler;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public <T> CallbackHandler<T> getCallbackHandler() {
		return (CallbackHandler<T>) callbackHandler;
	}
	
	@Override
	public int getIndicator() {
		return indicator;
	}
	
	@Override
	public void setIndicator(int indicator) {
		this.indicator = indicator;
	}
	
	@Override
	public int compareTo(Result o) {
		if(this.indicator < o.getIndicator()) {
			return -1;
		}
		if(this.indicator > o.getIndicator()) {
			return 1;
		}
		return 1;
	}
}
