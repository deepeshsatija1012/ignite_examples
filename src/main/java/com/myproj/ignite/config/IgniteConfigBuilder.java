package com.myproj.ignite.config;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;
import com.myproj.ignite.logging.LoggingUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.logger.slf4j.Slf4jLogger;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class IgniteConfigBuilder {
    private static final Logger LOGGER = LoggingUtils.addLogger(IgniteConfigBuilder.class);
    private static final IgniteLogger IGNITE_LOGGER = new Slf4jLogger();

    private IgniteConfiguration igniteNodeConfig;
    private int localCommunicationPort = -1;
    private int localDiscoveryPort = -1;
    private List<String> ipRanges = null;

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

    public IgniteConfigBuilder setIpRanges(List<String> ipRanges){
        this.ipRanges = new ArrayList<>(ipRanges);
        return this;
    }

    public IgniteConfigBuilder localDiscoveryPort(int port){
        this.localDiscoveryPort = port;
        return this;
    }
    public IgniteConfigBuilder localPort(int port){
        this.localCommunicationPort = port;
        return this;
    }

    public IgniteConfiguration build(){
        LoggingUtils.info(this.getClass(), this.igniteNodeConfig.getIgniteInstanceName(), "Building Node Config...");
        if(CollectionUtils.isNotEmpty(this.ipRanges)) {
            LoggingUtils.info(this.getClass(), this.igniteNodeConfig.getIgniteInstanceName(),"Overriding IP Ranges for Static IP-Finder {}", ipRanges);
            TcpDiscoverySpi tcpDiscoverySpi = new TcpDiscoverySpi();
            if(this.localDiscoveryPort!=-1){
                LoggingUtils.info(this.getClass(), this.igniteNodeConfig.getIgniteInstanceName(),"Overriding Local Spi Port {}", this.localDiscoveryPort);
                tcpDiscoverySpi.setLocalPort(this.localDiscoveryPort);
            }
            TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
            ipFinder.setAddresses(ipRanges);
            tcpDiscoverySpi.setIpFinder(ipFinder);
            this.igniteNodeConfig.setDiscoverySpi(tcpDiscoverySpi);
        }
        if(this.localCommunicationPort!=-1) {
            LoggingUtils.info(this.getClass(), this.igniteNodeConfig.getIgniteInstanceName(),"Overriding Local Port {}", this.localCommunicationPort);
            TcpCommunicationSpi commSpi = new TcpCommunicationSpi();
            commSpi.setLocalPort(this.localCommunicationPort);
            this.igniteNodeConfig.setCommunicationSpi(commSpi);
        }
        return igniteNodeConfig;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("igniteNodeConfig", igniteNodeConfig)
                .toString();
    }
}
