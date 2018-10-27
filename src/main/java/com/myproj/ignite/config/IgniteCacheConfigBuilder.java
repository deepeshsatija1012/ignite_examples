package com.myproj.ignite.config;

import com.myproj.ignite.logging.LoggingUtils;
import org.apache.ignite.configuration.CacheConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IgniteCacheConfigBuilder<K, V> {
    private static final Logger LOGGER = LoggingUtils.addLogger(IgniteCacheConfigBuilder.class);
    private CacheConfiguration<K, V> cacheConfiguration;

    public IgniteCacheConfigBuilder(String name){
        cacheConfiguration = new CacheConfiguration<>(name);
    }


    public CacheConfiguration<K, V> build(){
        return this.cacheConfiguration;
    }
}
