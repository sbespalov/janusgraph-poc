package org.carlspring.janusgraph.cassandra;

import org.apache.cassandra.service.CassandraDaemon;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

/**
 * @author Przemyslaw Fusik
 */
@Configuration
@ComponentScan
public class JanusGraphConfig
{

    @Bean(destroyMethod = "close")
    public JanusGraph janusGraph(CassandraDaemon cassandra,
                                 CassandraEmbeddedProperties cassandraEmbeddedProperties)
    {
        return JanusGraphFactory.build()
                                .set("storage.backend", "cql")
                                .set("storage.hostname", "127.0.0.1")
                                .set("storage.port", cassandraEmbeddedProperties.getPort())
                                .set("storage.cql.keyspace", "jgex")
                                .set("tx.log-tx", true)
                                .open();
    }

}
