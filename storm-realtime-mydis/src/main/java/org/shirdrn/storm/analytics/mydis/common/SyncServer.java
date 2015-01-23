package org.shirdrn.storm.analytics.mydis.common;

import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.storm.analytics.mydis.DefaultSyncServer;
import org.shirdrn.storm.analytics.mydis.constants.Constants;
import org.shirdrn.storm.commons.utils.ThreadPoolUtils;
import org.shirdrn.storm.spring.utils.SpringFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;

import com.google.common.collect.Sets;

public abstract class SyncServer implements Server {

	private static final Log LOG = LogFactory.getLog(DefaultSyncServer.class);
	private final Object object = new Object();
	private final Configuration conf;
	static final String ctxId = "realtime";
	static final String SPTING_CONFIGS = "classpath*:/applicationContext.xml";
	protected final ApplicationContext context;
	private final String poolName = "SYNC";
	private final ScheduledExecutorService scheduledExecutorService;
	private final int period;
	private final JdbcTemplate jdbcTemplate;
	private final Set<SyncWorker<?, ?>> syncWorkers = Sets.newHashSet();
	
	public SyncServer(Configuration conf) {
		this.conf = conf;
		// Spring context
		context = SpringFactory.getContextFactory(ctxId, SPTING_CONFIGS).getContext(ctxId);
		LOG.info("Spring context initialized: " + context);
		jdbcTemplate = context.getBean(JdbcTemplate.class);
		
		// configure sync server
		int nThreads = conf.getInt(Constants.SYNC_SCHEDULER_THREAD_COUNT, 1);
		scheduledExecutorService = ThreadPoolUtils.newScheduledThreadPool(nThreads, poolName);
		period = conf.getInt(Constants.SYNC_SCHEDULER_PERIOD, 10000);
	}
	
	public void registerSyncWorkers(SyncWorker<?, ?> syncWorker) {
		syncWorkers.add(syncWorker);
	}
	
	@Override
	public void start() throws Exception {
		LOG.info("All sync workers: " + syncWorkers);
		int sleepInterval = 1000;
		for(SyncWorker<?, ?> syncWorker : syncWorkers) {
			scheduledExecutorService.scheduleAtFixedRate(syncWorker, 500, period, TimeUnit.MILLISECONDS);
			Thread.sleep(sleepInterval);
		}
	}
	
	@Override
	public void close() throws Exception {
		// shutdown thread pool
		scheduledExecutorService.shutdown();
		// server exits
		synchronized(object) {
			object.notify();
		}		
	}
	
	@Override
	public void join() throws InterruptedException {
		synchronized(object) {
			object.wait();
		}		
	}

	public Configuration getConf() {
		return conf;
	}

	public ApplicationContext getContext() {
		return context;
	}
}
