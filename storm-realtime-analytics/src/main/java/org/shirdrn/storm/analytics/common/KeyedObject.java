package org.shirdrn.storm.analytics.common;

public class KeyedObject<T> extends AbstractResult {

	private static final long serialVersionUID = 1L;
	private String key;
	private T object;
	
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public T getObject() {
		return object;
	}
	public void setObject(T object) {
		this.object = object;
	}
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb
		.append("[indicator=").append(indicator).append(",")
		.append("key=").append(key).append(",");
		if(object == null) {
			sb.append("object=").append("]");
		} else {
			sb.append("object=").append(object).append("]");
		}
		return sb.toString();
	}
	
}
