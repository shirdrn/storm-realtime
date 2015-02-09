package org.shirdrn.storm.spring;

import static org.junit.Assert.assertNotNull;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ApplicationContext;

public class TestIocContainer {

	private ContextFactory<ApplicationContext> factory;
	final String META_INF_DIR = "";
	private static String DEFAULT_CONFIG_FILE;
	private String contextID;
	
	@Before
	public void setUp() throws Exception {
		DEFAULT_CONFIG_FILE = "classpath*:/" + META_INF_DIR + "applicationContext*.xml";
		contextID = "test";
		factory = new SpringContextFactory();
		factory.register(contextID, DEFAULT_CONFIG_FILE);
	}
	
	@Test
	public void testGetSpringBean() {
		ApplicationContext ctx = factory.getContext(contextID);
		assertNotNull(ctx.getBean("obj1"));
		assertNotNull(ctx.getBean("obj2"));
	}
	
	@After
	public void destroy() {
		
	}
}
