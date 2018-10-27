package com.myproj.ignite.startup;

import com.myproj.ignite.config.IgniteConfigBuilder;
import com.myproj.ignite.logging.LoggingUtils;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class IgniteStartUp {
    private static final Logger LOGGER = LoggingUtils.addLogger(IgniteStartUp.class);

    public static Map<String, Ignite> start(IgniteConfigBuilder... configs){
        LOGGER.info("Starting up single IgniteNode...");
        if(configs.length>1){
            throw new RuntimeException("Number of Nodes is greater than one "+ configs);
        }
        Map<String, Ignite> igniteNodeMap = new HashMap<>();
        for(IgniteConfigBuilder configBuilder : configs){
            Ignite igniteNode = start(configBuilder);
            igniteNodeMap.put(igniteNode.name(), igniteNode);
        }
        LOGGER.info("Ignite Node Start-Up Completed {}", igniteNodeMap);
        return igniteNodeMap;
    }

    public static Ignite start(IgniteConfigBuilder configBuilder){
        return Ignition.start(configBuilder.build());
    }
}
