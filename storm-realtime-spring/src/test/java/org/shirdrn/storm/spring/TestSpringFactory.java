package org.shirdrn.storm.spring;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;
import org.shirdrn.storm.spring.utils.SpringFactory;
import org.springframework.context.ApplicationContext;

public class TestSpringFactory {

	final String META_INF_DIR = "";
	
	@Test
	public void testSpringFactory() {
		String configs = "classpath*:/" + META_INF_DIR + "applicationContext*.xml";
		String contextID = "test";
		
		ContextFactory<ApplicationContext> cf1 = 
				SpringFactory.getContextFactory(contextID, configs);
		assertNotNull(cf1);
		
		ContextFactory<ApplicationContext> cf2 = 
				SpringFactory.getContextFactory(contextID, configs);
		assertNotNull(cf2);
		
		assertEquals(cf1, cf2);
		
		assertNotNull(cf2.getContext(contextID).getBean("obj1"));
	}
}
