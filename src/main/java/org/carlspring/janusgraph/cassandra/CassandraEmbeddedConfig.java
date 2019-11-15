package org.carlspring.janusgraph.cassandra;

import java.io.IOException;

import org.apache.cassandra.service.CassandraDaemon;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan
@ConfigurationPropertiesScan
public class CassandraEmbeddedConfig
{

    @Bean(destroyMethod = "deactivate")
    public CassandraDaemon activateCassandraDaemon(CassandraEmbeddedProperties properties)
        throws IOException,
        ClassNotFoundException
    {
        System.setProperty("cassandra.config.loader",
                           "org.carlspring.janusgraph.cassandra.CassandraEmbeddedProperties$CassandraEmbeddedPropertiesLoader");

        System.setProperty("cassandra-foreground", "true");
        System.setProperty("cassandra.native.epoll.enabled", "false");
        System.setProperty("cassandra.unsafesystem", "true");

        CassandraDaemon cassandraDaemon = new CassandraDaemon();
        cassandraDaemon.activate();

        return cassandraDaemon;
    }

}
