package com.myproj.ignite.startup;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.myproj.ignite.config.IgniteConfigBuilder;
import com.myproj.ignite.config.StartUpMode;
import com.myproj.ignite.logging.LoggingUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.ignite.Ignite;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;

import java.sql.Time;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class IgniteClusterStartUp {
    private static final Logger LOGGER = LoggingUtils.addLogger(IgniteClusterStartUp.class);
    private static final String NODE_1 = "testTwoCluster-Node-1";
    private static final String NODE_2 = "testTwoCluster-Node-2";
    private static final String CLUSTER_1 = "CLUSTER_1_";
    private static final String CLUSTER_2 = "CLUSTER_2_";

    @Test
    public void testOneClusterTwoNodes() throws InterruptedException, IllegalAccessException {
        IgniteContainer container = new IgniteContainer(NODE_1);

        Map<String, Ignite> igniteNodes = (Map<String, Ignite>) FieldUtils.readDeclaredField(container, "igniteNodes", true);
        Map<String, IgniteConfigBuilder> igniteNodeConfigMap = (Map<String, IgniteConfigBuilder>) FieldUtils.readDeclaredField(container, "igniteNodeConfigMap", true);

        IgniteConfigBuilder node1builder = container.createOrGetIgniteTcpCommunicationSpiNodeConfiguration(NODE_1);
        node1builder.startUpMode(StartUpMode.SERVER).setIpRanges(Lists.newArrayList("127.0.0.1:47500..47510"));

        IgniteConfigBuilder node2builder = container.createOrGetIgniteTcpCommunicationSpiNodeConfiguration(NODE_2);
        node2builder.startUpMode(StartUpMode.SERVER).setIpRanges(Lists.newArrayList("127.0.0.1:47500..47510"));

        container.initializeIgniteNode(NODE_1);
        Ignite node1 = igniteNodes.get(NODE_1);
        LoggingUtils.info(IgniteClusterStartUp.class, NODE_1, "Node Started Up..");
        container.initializeIgniteNode(NODE_2);
        Ignite node2 = igniteNodes.get(NODE_2);
        LoggingUtils.info(IgniteClusterStartUp.class, NODE_2, "Node Started Up..");

        Thread.sleep(TimeUnit.SECONDS.toMillis(10));
        Set<String> node1LocalNode = Sets.newHashSet(node1.cluster().localNode().id().toString());
        Set<String> node1RemoteNode = node1.cluster().forRemotes().forServers().nodes().stream().map(n -> n.id().toString()).collect(Collectors.toSet());

        Set<String> node2LocalNode = Sets.newHashSet(node2.cluster().localNode().id().toString());
        Set<String> node2RemoteNode = node2.cluster().forRemotes().forServers().nodes().stream().map(n -> n.id().toString()).collect(Collectors.toSet());

        Assert.assertTrue(Sets.difference(node1LocalNode, node2RemoteNode).size()==0);
        Assert.assertTrue(Sets.difference(node2LocalNode, node1RemoteNode).size()==0);

        container.destroyConfiguredIgniteNode();
    }

    @Test
    public void testTwoClusterTwoNodes() throws InterruptedException, IllegalAccessException {
        IgniteContainer containerClusterOne = new IgniteContainer(CLUSTER_1+NODE_1);

        Map<String, Ignite> igniteNodesClusterOne = (Map<String, Ignite>) FieldUtils.readDeclaredField(containerClusterOne, "igniteNodes", true);
        Map<String, IgniteConfigBuilder> igniteNodeConfigMapClusterOne = (Map<String, IgniteConfigBuilder>) FieldUtils.readDeclaredField(containerClusterOne, "igniteNodeConfigMap", true);

        IgniteConfigBuilder node1ClusterOneBuilder = containerClusterOne.createOrGetIgniteTcpCommunicationSpiNodeConfiguration(CLUSTER_1+NODE_1);
        node1ClusterOneBuilder.startUpMode(StartUpMode.SERVER).localPort(48100).localDiscoveryPort(48500).setIpRanges(Lists.newArrayList("127.0.0.1:48500..48510"));

        IgniteConfigBuilder node2ClusterOneBuilder = containerClusterOne.createOrGetIgniteTcpCommunicationSpiNodeConfiguration(CLUSTER_1+NODE_2);
        node2ClusterOneBuilder.startUpMode(StartUpMode.SERVER).localPort(48100).localDiscoveryPort(48500).setIpRanges(Lists.newArrayList("127.0.0.1:48500..48510"));

        containerClusterOne.initializeConfiguredIgniteNode();
        Ignite node1ClusterOne = igniteNodesClusterOne.get(CLUSTER_1+NODE_1);
        Ignite node2ClusterOne = igniteNodesClusterOne.get(CLUSTER_1+NODE_2);

        IgniteContainer containerClusterTwo = new IgniteContainer(CLUSTER_2+NODE_1);

        Map<String, Ignite> igniteNodesClusterTwo = (Map<String, Ignite>) FieldUtils.readDeclaredField(containerClusterTwo, "igniteNodes", true);
        Map<String, IgniteConfigBuilder> igniteNodeConfigMapClusterTwo = (Map<String, IgniteConfigBuilder>) FieldUtils.readDeclaredField(containerClusterTwo, "igniteNodeConfigMap", true);

        IgniteConfigBuilder node1ClusterTwoBuilder = containerClusterTwo.createOrGetIgniteTcpCommunicationSpiNodeConfiguration(CLUSTER_2+NODE_1);
        node1ClusterTwoBuilder.startUpMode(StartUpMode.SERVER).localPort(47100).localDiscoveryPort(47500).setIpRanges(Lists.newArrayList("127.0.0.1:47500..47510"));

        IgniteConfigBuilder node2ClusterTwoBuilder = containerClusterTwo.createOrGetIgniteTcpCommunicationSpiNodeConfiguration(CLUSTER_2+NODE_2);
        node2ClusterTwoBuilder.startUpMode(StartUpMode.SERVER).localPort(47100).localDiscoveryPort(47500).setIpRanges(Lists.newArrayList("127.0.0.1:47500..47510"));

        containerClusterTwo.initializeConfiguredIgniteNode();
        Ignite node1ClusterTwo = igniteNodesClusterTwo.get(CLUSTER_2+NODE_1);
        Ignite node2ClusterTwo = igniteNodesClusterTwo.get(CLUSTER_2+NODE_2);

        Set<String> nodesFormNode1ClusterOne = Sets.newHashSet(node1ClusterOne.cluster().localNode().id().toString());
        nodesFormNode1ClusterOne.addAll(node1ClusterOne.cluster().forRemotes().forServers().nodes().stream().map(n -> n.id().toString()).collect(Collectors.toSet()));
        Set<String> nodesFormNode2ClusterOne = Sets.newHashSet(node2ClusterOne.cluster().localNode().id().toString());
        nodesFormNode2ClusterOne.addAll(node2ClusterOne.cluster().forRemotes().forServers().nodes().stream().map(n -> n.id().toString()).collect(Collectors.toSet()));

        LoggingUtils.info(IgniteClusterStartUp.class, CLUSTER_1+NODE_1, "Nodes : " + nodesFormNode1ClusterOne);
        LoggingUtils.info(IgniteClusterStartUp.class, CLUSTER_1+NODE_2, "Nodes : " + nodesFormNode2ClusterOne);

        Set<String> nodesFormNode1ClusterTwo = Sets.newHashSet(node1ClusterTwo.cluster().localNode().id().toString());
        nodesFormNode1ClusterTwo.addAll(node1ClusterTwo.cluster().forRemotes().forServers().nodes().stream().map(n -> n.id().toString()).collect(Collectors.toSet()));
        Set<String> nodesFormNode2ClusterTwo = Sets.newHashSet(node2ClusterTwo.cluster().localNode().id().toString());
        nodesFormNode2ClusterTwo.addAll(node2ClusterTwo.cluster().forRemotes().forServers().nodes().stream().map(n -> n.id().toString()).collect(Collectors.toSet()));

        LoggingUtils.info(IgniteClusterStartUp.class, CLUSTER_2+NODE_1, "Nodes : " + nodesFormNode1ClusterTwo);
        LoggingUtils.info(IgniteClusterStartUp.class, CLUSTER_2+NODE_2, "Nodes : " + nodesFormNode2ClusterTwo);

        Assert.assertTrue(nodesFormNode1ClusterOne.size()==2);
        Assert.assertTrue(nodesFormNode2ClusterOne.size()==2);
        Assert.assertTrue(Sets.difference(nodesFormNode1ClusterOne, nodesFormNode2ClusterOne).size()==0);

        Assert.assertTrue(nodesFormNode1ClusterTwo.size()==2);
        Assert.assertTrue(nodesFormNode2ClusterTwo.size()==2);
        Assert.assertTrue(Sets.difference(nodesFormNode1ClusterTwo, nodesFormNode2ClusterTwo).size()==0);

        Assert.assertTrue(Sets.intersection(nodesFormNode1ClusterOne, nodesFormNode1ClusterTwo).size()==0);
        Assert.assertTrue(Sets.intersection(nodesFormNode2ClusterOne, nodesFormNode2ClusterTwo).size()==0);

        containerClusterOne.destroyConfiguredIgniteNode();
        containerClusterTwo.destroyConfiguredIgniteNode();
    }

}
