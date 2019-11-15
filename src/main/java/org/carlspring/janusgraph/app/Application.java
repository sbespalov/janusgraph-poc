package org.carlspring.janusgraph.app;

import static org.apache.tinkerpop.gremlin.process.traversal.P.eq;

import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.carlspring.janusgraph.cassandra.CassandraEmbeddedConfig;
import org.carlspring.janusgraph.cassandra.JanusGraphConfig;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.VertexLabel;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.graphdb.database.management.GraphIndexStatusReport;
import org.janusgraph.graphdb.database.management.ManagementSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.validation.ValidationAutoConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Import;

@SpringBootApplication()
@Import({ CassandraEmbeddedConfig.class, JanusGraphConfig.class })
@EnableAutoConfiguration(exclude = ValidationAutoConfiguration.class)
public class Application implements CommandLineRunner
{

    private static final Logger logger = LoggerFactory.getLogger(Application.class);

    private static final String ARTIFACT_COORDINATES = "ArtifactCoordinates";
    private static final String ARTIFACT_ENTRY = "ArtifactEntry";

    @Inject
    private ConfigurableApplicationContext applicationContext;
    
    @Inject
    private JanusGraph janusGraph;
    
    public static void main(String[] args)
    {
        SpringApplication.run(Application.class, args);
    }

    @Override
    public void run(String... args)
        throws Exception
    {
        GraphTraversalSource g = janusGraph.traversal();
        Vertex vArtifactCoordinates = g.addV(ARTIFACT_COORDINATES).property("path", "org/carlspring/test-artifact-1.2.3.jar").property("version", "1.2.3").next();
        logger.info("Created vertex {}: [{}]", ARTIFACT_COORDINATES, vArtifactCoordinates);
        
        Map<String, Object> searchResult = g.V().hasLabel(ARTIFACT_COORDINATES).has("path", eq("org/carlspring/test-artifact-1.2.3.jar")).project("ac").next();
        logger.info("Search result: [{}]", searchResult);
        
        g.tx().rollback();
        
        try
        {
            JanusGraphFactory.drop(janusGraph);
        }
        catch (Exception e)
        {
            logger.error(String.format("Failed to drop Janusgraph instance: [%s]", janusGraph), e);
        }
        
        applicationContext.close();
        
        System.exit(0);
    }

    @Inject
    public void createSchema(JanusGraph jg)
        throws InterruptedException
    {
        JanusGraphManagement jgm = jg.openManagement();
        try
        {
            applySchemaChanges(jgm);
            jgm.commit();
        }
        catch (Exception e)
        {
            logger.error("Failed to apply schema changes.", e);
            jgm.rollback();
            throw new RuntimeException("Failed to apply schema changes.", e);
        }

        jgm = jg.openManagement();
        Set<String> indexes;
        try
        {
            indexes = createIndexes(jg, jgm);
            jgm.commit();
        }
        catch (Exception e)
        {
            logger.error("Failed to create indexes.", e);
            jgm.rollback();
            throw new RuntimeException("Failed to create indexes.", e);
        }

        for (String janusGraphIndex : indexes)
        {
            GraphIndexStatusReport report = ManagementSystem.awaitGraphIndexStatus(jg, janusGraphIndex).call();
            logger.info("Index status report: \n{}", report);
        }

        jgm = jg.openManagement();
        logger.info("Schema: \n{}", jgm.printSchema());
        jgm.commit();
    }

    protected Set<String> createIndexes(JanusGraph jg,
                                        JanusGraphManagement jgm)
        throws InterruptedException
    {
        Set<String> result = new HashSet<>();

        PropertyKey propertyPath = jgm.getPropertyKey("path");
        VertexLabel vertexLabel = jgm.getVertexLabel(ARTIFACT_COORDINATES);

        result.add(jgm.buildIndex(ARTIFACT_COORDINATES + ".path", Vertex.class)
                      .addKey(propertyPath)
                      .indexOnly(vertexLabel)
                      .buildCompositeIndex()
                      .name());

        return result;
    }

    private void applySchemaChanges(JanusGraphManagement jgm)
    {
        // Properties
        jgm.makePropertyKey("uuid").dataType(String.class).make();
        jgm.makePropertyKey("storageId").dataType(String.class).make();
        jgm.makePropertyKey("repositoryId").dataType(String.class).make();
        jgm.makePropertyKey("sizeInBytes").dataType(Long.class).make();
        jgm.makePropertyKey("created").dataType(Date.class).make();
        jgm.makePropertyKey("tags").dataType(String.class).cardinality(Cardinality.SET).make();

        jgm.makePropertyKey("path").dataType(String.class).make();
        jgm.makePropertyKey("version").dataType(String.class).make();

        // Vertices
        jgm.makeVertexLabel(ARTIFACT_ENTRY).make();
        jgm.makeVertexLabel(ARTIFACT_COORDINATES).make();

        // Edges
        jgm.makeEdgeLabel(ARTIFACT_ENTRY + "#" + ARTIFACT_COORDINATES)
           .multiplicity(Multiplicity.MANY2ONE)
           .make();
    }

}
