package com.example;

import com.example.model.GameMailboxItem;
import com.example.model.GameMailboxMergedItem;
import com.example.sink.DynamoDBSink;
import com.example.source.DynamoDBFullLoadSource;
import com.example.source.DynamoDBStreamSource;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

public class DynamoDBMergeJob {
    private static final Logger LOG = LoggerFactory.getLogger(DynamoDBMergeJob.class);
    private static final String REGION = "us-east-1";
    private static final String GAME_ID = "Souls";
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    // Source table names
    private static final String[] SOURCE_TABLES = {
            "GameMailbox1",
            "GameMailbox2",
            "GameMailbox3"
    };
    
    // Target table name
    private static final String TARGET_TABLE = "GameMailboxMerged";

    public static void main(String[] args) throws Exception {
        LOG.info("Starting DynamoDB Merge Job");
        
        try {
            // Ensure target table exists
            ensureTargetTableExists();
            
            // Set up the execution environment
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            
            // Configure restart strategy
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // number of restart attempts
                5000 // delay between attempts in milliseconds
            ));
            
            // Configure checkpointing for exactly-once processing
            env.enableCheckpointing(60000); // Checkpoint every 60 seconds
            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000); // Min 30 seconds between checkpoints
            env.getCheckpointConfig().setCheckpointTimeout(120000); // Timeout after 2 minutes
            env.getCheckpointConfig().setMaxConcurrentCheckpoints(1); // Only one checkpoint at a time
            
            // Process each source table
            for (String sourceTable : SOURCE_TABLES) {
                LOG.info("Setting up processing for source table: {}", sourceTable);
                
                try {
                    // Create a stream for initial full load
                    DataStream<GameMailboxItem> fullLoadStream = createFullLoadStream(env, sourceTable);
                    
                    // Create a stream for CDC from DynamoDB Streams
                    DataStream<GameMailboxItem> cdcStream = createCDCStream(env, sourceTable);
                    
                    // Process and write full load data to target
                    fullLoadStream
                        .map(new ItemTransformer())
                        .addSink(new DynamoDBSink(TARGET_TABLE))
                        .name("Full-Load-Sink-" + sourceTable);
                    
                    // Process and write CDC data to target
                    cdcStream
                        .map(new ItemTransformer())
                        .addSink(new DynamoDBSink(TARGET_TABLE))
                        .name("CDC-Sink-" + sourceTable);
                } catch (Exception e) {
                    LOG.error("Error setting up processing for table {}: {}", sourceTable, e.getMessage(), e);
                }
            }
            
            // Execute the job
            env.execute("DynamoDB Merge and Sync Job");
        } catch (Exception e) {
            LOG.error("Error in main method: {}", e.getMessage(), e);
            throw e;
        }
    }
    
    /**
     * Creates a stream for the initial full load from a DynamoDB table
     */
    private static DataStream<GameMailboxItem> createFullLoadStream(
            StreamExecutionEnvironment env, String tableName) {
        return env.addSource(new DynamoDBFullLoadSource(tableName))
                .name("FullLoad-Source-" + tableName);
    }
    
    /**
     * Creates a stream for CDC from DynamoDB Streams
     */
    private static DataStream<GameMailboxItem> createCDCStream(
            StreamExecutionEnvironment env, String tableName) {
        
        // Get the stream ARN for the table
        String streamArn = getTableStreamArn(tableName);
        
        return env.addSource(new DynamoDBStreamSource(streamArn))
                .name("CDC-Source-" + tableName);
    }
    
    /**
     * Gets the DynamoDB Stream ARN for a table
     */
    private static String getTableStreamArn(String tableName) {
        DynamoDbClient dynamoDbClient = null;
        try {
            dynamoDbClient = DynamoDbClient.builder()
                    .region(Region.US_EAST_1)
                    .credentialsProvider(DefaultCredentialsProvider.create())
                    .build();
            
            DescribeTableRequest request = DescribeTableRequest.builder()
                    .tableName(tableName)
                    .build();
            
            DescribeTableResponse response = dynamoDbClient.describeTable(request);
            String streamArn = response.table().latestStreamArn();
            
            if (streamArn != null) {
                LOG.info("Found stream ARN for table {}: {}", tableName, streamArn);
                return streamArn;
            }
        } catch (Exception e) {
            LOG.warn("Error getting stream ARN for table {}: {}", tableName, e.getMessage());
        } finally {
            if (dynamoDbClient != null) {
                dynamoDbClient.close();
            }
        }
        
        // If we can't get the real stream ARN, return a dummy one
        String dummyArn = "arn:aws:dynamodb:" + REGION + ":000000000000:table/" + tableName + "/stream/2023-01-01T00:00:00.000";
        LOG.info("Using dummy stream ARN for table {}: {}", tableName, dummyArn);
        return dummyArn;
    }
    
    /**
     * Ensures the target table exists
     */
    private static void ensureTargetTableExists() {
        DynamoDbClient dynamoDbClient = null;
        try {
            dynamoDbClient = DynamoDbClient.builder()
                    .region(Region.US_EAST_1)
                    .credentialsProvider(DefaultCredentialsProvider.create())
                    .build();
            
            try {
                DescribeTableRequest describeRequest = DescribeTableRequest.builder()
                    .tableName(TARGET_TABLE)
                    .build();
                
                dynamoDbClient.describeTable(describeRequest);
                LOG.info("Target table {} exists", TARGET_TABLE);
            } catch (ResourceNotFoundException e) {
                LOG.info("Target table {} does not exist, creating it", TARGET_TABLE);
                
                // Create the target table
                CreateTableRequest createRequest = CreateTableRequest.builder()
                    .tableName(TARGET_TABLE)
                    .attributeDefinitions(
                        AttributeDefinition.builder().attributeName("PK").attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName("PK1").attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName("PK2").attributeType(ScalarAttributeType.S).build()
                    )
                    .keySchema(
                        KeySchemaElement.builder().attributeName("PK").keyType(KeyType.HASH).build()
                    )
                    .globalSecondaryIndexes(
                        GlobalSecondaryIndex.builder()
                            .indexName("PK1Index")
                            .keySchema(KeySchemaElement.builder().attributeName("PK1").keyType(KeyType.HASH).build())
                            .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
                            .provisionedThroughput(ProvisionedThroughput.builder()
                                .readCapacityUnits(5L)
                                .writeCapacityUnits(5L)
                                .build())
                            .build(),
                        GlobalSecondaryIndex.builder()
                            .indexName("PK2Index")
                            .keySchema(KeySchemaElement.builder().attributeName("PK2").keyType(KeyType.HASH).build())
                            .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
                            .provisionedThroughput(ProvisionedThroughput.builder()
                                .readCapacityUnits(5L)
                                .writeCapacityUnits(5L)
                                .build())
                            .build()
                    )
                    .provisionedThroughput(
                        ProvisionedThroughput.builder()
                            .readCapacityUnits(5L)
                            .writeCapacityUnits(5L)
                            .build()
                    )
                    .build();
                    
                dynamoDbClient.createTable(createRequest);
                LOG.info("Created target table: {}", TARGET_TABLE);
                
                // Wait for table to become active
                boolean tableActive = false;
                int attempts = 0;
                while (!tableActive && attempts < 10) {
                    try {
                        Thread.sleep(5000);
                        DescribeTableResponse response = dynamoDbClient.describeTable(
                            DescribeTableRequest.builder().tableName(TARGET_TABLE).build()
                        );
                        tableActive = response.table().tableStatus() == TableStatus.ACTIVE;
                        attempts++;
                    } catch (Exception ex) {
                        LOG.warn("Error checking table status: {}", ex.getMessage());
                    }
                }
                
                if (tableActive) {
                    LOG.info("Target table {} is now active", TARGET_TABLE);
                } else {
                    LOG.warn("Target table {} may not be active yet", TARGET_TABLE);
                }
            }
        } catch (Exception e) {
            LOG.error("Error checking/creating target table: {}", e.getMessage(), e);
        } finally {
            if (dynamoDbClient != null) {
                dynamoDbClient.close();
            }
        }
    }
    
    /**
     * Transforms source items to target format
     */
    public static class ItemTransformer implements MapFunction<GameMailboxItem, GameMailboxMergedItem> {
        @Override
        public GameMailboxMergedItem map(GameMailboxItem item) {
            // Check if this is already a GameMailboxMergedItem with delete flag
            if (item instanceof GameMailboxMergedItem && ((GameMailboxMergedItem) item).isDeleteOperation()) {
                return (GameMailboxMergedItem) item;
            }
            
            // Otherwise, create a new merged item
            return new GameMailboxMergedItem(item, GAME_ID);
        }
    }
}
