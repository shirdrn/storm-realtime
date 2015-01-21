package org.shirdrn.storm.spring;

import java.util.Collections;
import java.util.Map;

import com.google.common.collect.Maps;

public abstract class AbstractContextFactory<T, R> implements ContextFactory<T> {

	private final Map<String, T> cachedContexts = Maps.newHashMap();
	private final Map<T, Class<R>> realContextTypes = Maps.newHashMap();
	
	public synchronized void register(String name, T context) {
		if(!isContextExists(name)) {
			cachedContexts.put(name, context);
		}
	}
	
	protected synchronized boolean isContextExists(String name) {
		return cachedContexts.containsKey(name);
	}
	
	protected Map<String, T> getCachedContexts() {
		return Collections.unmodifiableMap(cachedContexts);
	}
	
	protected Map<T, Class<R>> getRealContextTypes() {
		return Collections.unmodifiableMap(realContextTypes);
	}
	
	protected synchronized void register(String name, T context, Class<R> realContextType) {
		this.register(name, context);
		if(!realContextTypes.containsKey(context)) {
			realContextTypes.put(context, realContextType);
		}
	}
	
	public Class<R> getRealContextType(String name) {
		Class<R> realType = null;
		T context = cachedContexts.get(name);
		if(context != null) {
			realType = realContextTypes.get(context);
		}
		return realType;
	}

	@Override
	public T getContext(String name) {
		return cachedContexts.get(name);
	}
	
}
