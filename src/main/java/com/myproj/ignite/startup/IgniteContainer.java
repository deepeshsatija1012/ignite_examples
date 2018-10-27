package com.myproj.ignite.startup;

import com.myproj.ignite.config.IgniteConfigBuilder;
import com.myproj.ignite.logging.LoggingUtils;
import org.apache.ignite.Ignite;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class IgniteContainer {
    private static final Logger LOGGER = LoggingUtils.addLogger(IgniteContainer.class);
    private String nodeName;
    private Map<String, Ignite> igniteNodes = new HashMap<>();
    private Map<String, IgniteConfigBuilder> igniteNodeConfigMap = new HashMap<>();

    public IgniteContainer(String currentNodeName) {
        this.nodeName = currentNodeName;
        igniteNodeConfigMap.put(currentNodeName, new IgniteConfigBuilder(this.nodeName));
    }

    public synchronized IgniteConfigBuilder createOrGetIgniteNodeConfiguration(String name) {
        IgniteConfigBuilder congifBuilder = this.igniteNodeConfigMap.get(name);
        Ignite node = this.igniteNodes.get(name);
        if(node!=null){
            LoggingUtils.info(this.getClass(), name, "IgniteNode Already Exists..");
            return congifBuilder;
        }else if(congifBuilder!=null && node==null) {
            LoggingUtils.info(this.getClass(), name,"Config has been initialized, but Ignite Node has not been created..");
            return congifBuilder;
        }
        LoggingUtils.info(this.getClass(), name,"Initializing new Config..");
        congifBuilder = new IgniteConfigBuilder(name);
        this.igniteNodeConfigMap.put(name, congifBuilder);
        LoggingUtils.info(this.getClass(), name,"Initialized new Config..");
        return congifBuilder;
    }

    public synchronized void initializeConfiguredIgniteNode() {
        Map<String, Ignite> newIgniteNodes = this.igniteNodeConfigMap.entrySet()
                .stream().filter(entry -> !this.igniteNodes.containsKey(entry.getKey()) )
                .map(entry -> IgniteStartUp.start(entry.getValue()))
                .collect(Collectors.toMap(node -> node.name(), node -> node));
        LOGGER.info("Nodes Added {}", newIgniteNodes);
        this.igniteNodes.putAll(newIgniteNodes);
    }

    public synchronized void initializeIgniteNode(String name) {
        IgniteConfigBuilder congifBuilder = this.igniteNodeConfigMap.get(name);
        Ignite node = this.igniteNodes.get(name);
        if(node!=null){
            LoggingUtils.info(this.getClass(), name, "IgniteNode Already Exists..");
            return;
        }else if(congifBuilder!=null){
            Ignite igniteNode = IgniteStartUp.start(congifBuilder);
            this.igniteNodes.put(igniteNode.name(), igniteNode);
            LoggingUtils.info(this.getClass(), name, "IgniteNode Instantiated..");
            return;
        }
        LOGGER.info("Ignite Node/Configuration [{}], not found. Please initialize config first", name);
    }

    public synchronized void initializeIgniteNode(String name, IgniteConfigBuilder igniteConfigBuilder) {
        IgniteConfigBuilder congifBuilder = this.igniteNodeConfigMap.get(name);
        Ignite node = this.igniteNodes.get(name);
        if(node!=null){
            LoggingUtils.info(this.getClass(), name, "IgniteNode Already Exists..");
            return;
        }else if(congifBuilder!=null && igniteConfigBuilder==congifBuilder){
            Ignite igniteNode = IgniteStartUp.start(congifBuilder);
            this.igniteNodes.put(igniteNode.name(), igniteNode);
            LoggingUtils.info(this.getClass(), name, "IgniteNode Instantiated..");
            return;
        }else if(congifBuilder!=null && igniteConfigBuilder!=congifBuilder){
            this.igniteNodeConfigMap.put(name, igniteConfigBuilder);
            LoggingUtils.info(this.getClass(), name, "Overriding Ignite Config\r\nOrig : [{}]\r\nOvr : [{}]", congifBuilder, igniteConfigBuilder);
            Ignite igniteNode = IgniteStartUp.start(igniteConfigBuilder);
            this.igniteNodes.put(igniteNode.name(), igniteNode);
            LoggingUtils.info(this.getClass(), name, "IgniteNode Instantiated..");
            return;
        }else{
            this.igniteNodeConfigMap.put(name, igniteConfigBuilder);
            LoggingUtils.info(this.getClass(), name, "New Ignite Node Config Identified\r\n Config : [{}]", igniteConfigBuilder);
            Ignite igniteNode = IgniteStartUp.start(igniteConfigBuilder);
            this.igniteNodes.put(igniteNode.name(), igniteNode);
            LoggingUtils.info(this.getClass(), name, "IgniteNode Instantiated..");
        }
    }

    public synchronized Map<String, IgniteConfigBuilder> destroyConfiguredIgniteNode() {
        Map<String, IgniteConfigBuilder> destroyedInstances = new HashMap<>();
        this.igniteNodes.entrySet().stream().forEach( entry -> {
            try{
                entry.getValue().close();
                destroyedInstances.put(entry.getKey(), this.igniteNodeConfigMap.get(entry.getKey()));
                LoggingUtils.info(IgniteContainer.class, entry.getKey(), "Destroyed Ignite Node..");
            }catch(Throwable e){
                LoggingUtils.error(IgniteContainer.class, entry.getKey(), "Unable to destroy Ignite Node..", e);
            }
        });
        for(Map.Entry<String, IgniteConfigBuilder> entry : destroyedInstances.entrySet()){
            this.igniteNodes.remove(entry.getKey());
            this.igniteNodeConfigMap.remove(entry.getKey());
        }
        return destroyedInstances;
    }

}
