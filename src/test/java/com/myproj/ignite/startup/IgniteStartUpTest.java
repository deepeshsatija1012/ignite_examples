package com.myproj.ignite.startup;

import com.myproj.ignite.config.IgniteConfigBuilder;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.ignite.Ignite;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class IgniteStartUpTest {

    @Test
    public void testOneServerNode() throws IllegalAccessException {
        IgniteContainer container = new IgniteContainer("testOneServerNode");
        Map<String, Ignite> igniteNodes = (Map<String, Ignite>) FieldUtils.readDeclaredField(container, "igniteNodes", true);
        Map<String, IgniteConfigBuilder> igniteNodeConfigMap = (Map<String, IgniteConfigBuilder>) FieldUtils.readDeclaredField(container, "igniteNodeConfigMap", true);

        Assert.assertTrue(igniteNodeConfigMap.size()==1);
        Assert.assertTrue(igniteNodes.size()==0);
        Assert.assertTrue(igniteNodeConfigMap.containsKey("testOneServerNode"));
        Assert.assertTrue(!igniteNodes.containsKey("testOneServerNode"));

        container.initializeConfiguredIgniteNode();
        Assert.assertTrue(igniteNodeConfigMap.size()==1);
        Assert.assertTrue(igniteNodes.size()==1);
        Assert.assertTrue(igniteNodeConfigMap.containsKey("testOneServerNode"));
        Assert.assertTrue(igniteNodes.containsKey("testOneServerNode"));

        container.initializeIgniteNode("testOneServerNode");
        Assert.assertTrue(igniteNodeConfigMap.size()==1);
        Assert.assertTrue(igniteNodes.size()==1);
        Assert.assertTrue(igniteNodeConfigMap.containsKey("testOneServerNode"));
        Assert.assertTrue(igniteNodes.containsKey("testOneServerNode"));

        container.initializeIgniteNode("testOneServerNode", igniteNodeConfigMap.get("igniteNodeConfigMap"));
        Assert.assertTrue(igniteNodeConfigMap.size()==1);
        Assert.assertTrue(igniteNodes.size()==1);
        Assert.assertTrue(igniteNodeConfigMap.containsKey("testOneServerNode"));
        Assert.assertTrue(igniteNodes.containsKey("testOneServerNode"));

        Map<String, IgniteConfigBuilder> destroyedMap = container.destroyConfiguredIgniteNode();
        Assert.assertTrue(destroyedMap.size()==1);
        Assert.assertTrue(destroyedMap.containsKey("testOneServerNode"));

        Assert.assertTrue(igniteNodeConfigMap.size()==0);
        Assert.assertTrue(igniteNodes.size()==0);
        Assert.assertTrue(!igniteNodeConfigMap.containsKey("testOneServerNode"));
        Assert.assertTrue(!igniteNodes.containsKey("testOneServerNode"));
    }

    @Test
    public void testTwoServerNode() throws IllegalAccessException {
        IgniteContainer container = new IgniteContainer("testOneServerNode");
        Map<String, Ignite> igniteNodes = (Map<String, Ignite>) FieldUtils.readDeclaredField(container, "igniteNodes", true);
        Map<String, IgniteConfigBuilder> igniteNodeConfigMap = (Map<String, IgniteConfigBuilder>) FieldUtils.readDeclaredField(container, "igniteNodeConfigMap", true);

        Assert.assertTrue(igniteNodeConfigMap.size()==1);
        Assert.assertTrue(igniteNodes.size()==0);
        Assert.assertTrue(igniteNodeConfigMap.containsKey("testOneServerNode"));
        Assert.assertTrue(!igniteNodes.containsKey("testOneServerNode"));

        container.initializeConfiguredIgniteNode();
        Assert.assertTrue(igniteNodeConfigMap.size()==1);
        Assert.assertTrue(igniteNodes.size()==1);
        Assert.assertTrue(igniteNodeConfigMap.containsKey("testOneServerNode"));
        Assert.assertTrue(igniteNodes.containsKey("testOneServerNode"));

        container.initializeIgniteNode("testOneServerNode");
        Assert.assertTrue(igniteNodeConfigMap.size()==1);
        Assert.assertTrue(igniteNodes.size()==1);
        Assert.assertTrue(igniteNodeConfigMap.containsKey("testOneServerNode"));
        Assert.assertTrue(igniteNodes.containsKey("testOneServerNode"));

        container.initializeIgniteNode("testOneServerNode", igniteNodeConfigMap.get("igniteNodeConfigMap"));
        Assert.assertTrue(igniteNodeConfigMap.size()==1);
        Assert.assertTrue(igniteNodes.size()==1);
        Assert.assertTrue(igniteNodeConfigMap.containsKey("testOneServerNode"));
        Assert.assertTrue(igniteNodes.containsKey("testOneServerNode"));

        Map<String, IgniteConfigBuilder> destroyedMap = container.destroyConfiguredIgniteNode();
        Assert.assertTrue(destroyedMap.size()==1);
        Assert.assertTrue(destroyedMap.containsKey("testOneServerNode"));

        Assert.assertTrue(igniteNodeConfigMap.size()==0);
        Assert.assertTrue(igniteNodes.size()==0);
        Assert.assertTrue(!igniteNodeConfigMap.containsKey("testOneServerNode"));
        Assert.assertTrue(!igniteNodes.containsKey("testOneServerNode"));
    }
}
