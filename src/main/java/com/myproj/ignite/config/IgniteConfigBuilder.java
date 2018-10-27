package com.myproj.ignite.config;

import com.google.common.base.MoreObjects;
import com.myproj.ignite.logging.LoggingUtils;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.logger.slf4j.Slf4jLogger;
import org.slf4j.Logger;

public class IgniteConfigBuilder {
    private static final Logger LOGGER = LoggingUtils.addLogger(IgniteConfigBuilder.class);
    private static final IgniteLogger IGNITE_LOGGER = new Slf4jLogger();

    private IgniteConfiguration igniteNodeConfig;
    public IgniteConfigBuilder(String nodeName){
        igniteNodeConfig = new IgniteConfiguration();
//        igniteNodeConfig.setGridLogger(IGNITE_LOGGER);
        igniteNodeConfig.setIgniteInstanceName(nodeName);
    }

    public IgniteConfigBuilder startUpMode(StartUpMode startUpMode){
        LoggingUtils.info(this.getClass(), this.igniteNodeConfig.getIgniteInstanceName(),"Overriding Start-up Mode {}", startUpMode);
        igniteNodeConfig.setClientMode(startUpMode==StartUpMode.CLIENT);
        return this;
    }
    public IgniteConfiguration build(){
        LoggingUtils.info(this.getClass(), this.igniteNodeConfig.getIgniteInstanceName(), "Building Node Config...");
        return igniteNodeConfig;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("igniteNodeConfig", igniteNodeConfig)
                .toString();
    }
}
