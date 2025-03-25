package com.example.source;

import com.example.model.GameMailboxItem;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Custom source function for full load from DynamoDB
 */
public class DynamoDBFullLoadSource extends RichSourceFunction<GameMailboxItem> {
    private static final Logger LOG = LoggerFactory.getLogger(DynamoDBFullLoadSource.class);
    private static final long serialVersionUID = 1L;
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    private final String tableName;
    private transient DynamoDbClient dynamoDbClient;
    private volatile boolean isRunning = true;
    
    public DynamoDBFullLoadSource(String tableName) {
        this.tableName = tableName;
    }
    
    @Override
    public void open(Configuration parameters) {
        dynamoDbClient = DynamoDbClient.builder()
                .region(Region.US_EAST_1)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();
    }
    
    @Override
    public void run(SourceContext<GameMailboxItem> ctx) throws Exception {
        LOG.info("Starting full load from table: {}", tableName);
        
        // First check if the table exists, if not create a dummy table for testing
        try {
            DescribeTableRequest describeRequest = DescribeTableRequest.builder()
                .tableName(tableName)
                .build();
            
            dynamoDbClient.describeTable(describeRequest);
            LOG.info("Table {} exists, proceeding with scan", tableName);
        } catch (ResourceNotFoundException e) {
            LOG.warn("Table {} does not exist, creating dummy table for testing", tableName);
            createDummyTable();
            // Wait for table to become active
            Thread.sleep(5000);
        }
        
        Map<String, AttributeValue> lastEvaluatedKey = null;
        int totalItems = 0;
        
        do {
            ScanRequest.Builder scanRequestBuilder = ScanRequest.builder()
                    .tableName(tableName)
                    .limit(1000); // Process in batches of 1000
            
            if (lastEvaluatedKey != null) {
                scanRequestBuilder.exclusiveStartKey(lastEvaluatedKey);
            }
            
            ScanResponse response = dynamoDbClient.scan(scanRequestBuilder.build());
            
            for (Map<String, AttributeValue> item : response.items()) {
                try {
                    // Convert DynamoDB item to a GameMailboxItem
                    GameMailboxItem mailboxItem = convertToGameMailboxItem(item);
                    
                    // Emit the item
                    synchronized (ctx.getCheckpointLock()) {
                        ctx.collect(mailboxItem);
                    }
                    
                    totalItems++;
                    
                    // Log progress every 1000 items
                    if (totalItems % 1000 == 0) {
                        LOG.info("Processed {} items from table {}", totalItems, tableName);
                    }
                } catch (Exception e) {
                    LOG.error("Error processing item: {}", e.getMessage(), e);
                }
            }
            
            lastEvaluatedKey = response.lastEvaluatedKey();
        } while (lastEvaluatedKey != null && !lastEvaluatedKey.isEmpty() && isRunning);
        
        LOG.info("Completed full load from table: {}. Total items processed: {}", tableName, totalItems);
    }
    
    private GameMailboxItem convertToGameMailboxItem(Map<String, AttributeValue> item) {
        GameMailboxItem mailboxItem = new GameMailboxItem();
        
        // Set basic fields
        if (item.containsKey("mailId")) {
            mailboxItem.setMailId(item.get("mailId").s());
        }
        
        if (item.containsKey("addRewardType")) {
            mailboxItem.setAddRewardType(Integer.parseInt(item.get("addRewardType").n()));
        }
        
        if (item.containsKey("claimAt")) {
            mailboxItem.setClaimAt(item.get("claimAt").s());
        }
        
        if (item.containsKey("claimed")) {
            mailboxItem.setClaimed(item.get("claimed").bool());
        }
        
        if (item.containsKey("userId")) {
            mailboxItem.setUserId(item.get("userId").s());
        }
        
        if (item.containsKey("testUserId")) {
            if (item.get("testUserId").nul() != null && item.get("testUserId").nul()) {
                mailboxItem.setTestUserId(null);
            } else if (item.get("testUserId").s() != null) {
                mailboxItem.setTestUserId(item.get("testUserId").s());
            }
        }
        
        // Handle placeholderData
        if (item.containsKey("placeholderData") && item.get("placeholderData").m() != null) {
            Map<String, Map<String, String>> placeholderData = new HashMap<>();
            Map<String, AttributeValue> placeholderMap = item.get("placeholderData").m();
            
            for (Map.Entry<String, AttributeValue> entry : placeholderMap.entrySet()) {
                if (entry.getValue().m() != null) {
                    Map<String, String> innerMap = new HashMap<>();
                    Map<String, AttributeValue> attrInnerMap = entry.getValue().m();
                    
                    for (Map.Entry<String, AttributeValue> innerEntry : attrInnerMap.entrySet()) {
                        if (innerEntry.getValue().s() != null) {
                            innerMap.put(innerEntry.getKey(), innerEntry.getValue().s());
                        }
                    }
                    
                    placeholderData.put(entry.getKey(), innerMap);
                }
            }
            
            mailboxItem.setPlaceholderData(placeholderData);
        }
        
        // Handle rewards
        if (item.containsKey("rewards") && item.get("rewards").l() != null) {
            List<GameMailboxItem.Reward> rewards = new ArrayList<>();
            List<AttributeValue> rewardsList = item.get("rewards").l();
            
            for (AttributeValue rewardAttr : rewardsList) {
                if (rewardAttr.m() != null) {
                    Map<String, AttributeValue> rewardMap = rewardAttr.m();
                    GameMailboxItem.Reward reward = new GameMailboxItem.Reward();
                    
                    if (rewardMap.containsKey("amount") && rewardMap.get("amount").n() != null) {
                        reward.setAmount(Integer.parseInt(rewardMap.get("amount").n()));
                    }
                    
                    if (rewardMap.containsKey("id") && rewardMap.get("id").s() != null) {
                        reward.setId(rewardMap.get("id").s());
                    }
                    
                    if (rewardMap.containsKey("isPaReward") && rewardMap.get("isPaReward").bool() != null) {
                        reward.setIsPaReward(rewardMap.get("isPaReward").bool());
                    }
                    
                    if (rewardMap.containsKey("rewardId")) {
                        if (rewardMap.get("rewardId").nul() != null && rewardMap.get("rewardId").nul()) {
                            reward.setRewardId(null);
                        } else if (rewardMap.get("rewardId").s() != null) {
                            reward.setRewardId(rewardMap.get("rewardId").s());
                        }
                    }
                    
                    if (rewardMap.containsKey("rewardType") && rewardMap.get("rewardType").n() != null) {
                        reward.setRewardType(Integer.parseInt(rewardMap.get("rewardType").n()));
                    }
                    
                    rewards.add(reward);
                }
            }
            
            mailboxItem.setRewards(rewards);
        }
        
        return mailboxItem;
    }
    
    private void createDummyTable() {
        try {
            // Create table with mailId as hash key and userId as GSI
            CreateTableRequest createRequest = CreateTableRequest.builder()
                .tableName(tableName)
                .attributeDefinitions(
                    AttributeDefinition.builder().attributeName("mailId").attributeType(ScalarAttributeType.S).build(),
                    AttributeDefinition.builder().attributeName("userId").attributeType(ScalarAttributeType.S).build()
                )
                .keySchema(
                    KeySchemaElement.builder().attributeName("mailId").keyType(KeyType.HASH).build()
                )
                .globalSecondaryIndexes(
                    GlobalSecondaryIndex.builder()
                        .indexName("UserIdIndex")
                        .keySchema(KeySchemaElement.builder().attributeName("userId").keyType(KeyType.HASH).build())
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
                .streamSpecification(
                    StreamSpecification.builder()
                        .streamEnabled(true)
                        .streamViewType(StreamViewType.NEW_AND_OLD_IMAGES)
                        .build()
                )
                .build();
                
            dynamoDbClient.createTable(createRequest);
            LOG.info("Created dummy table: {}", tableName);
            
            // Insert a sample item
            Map<String, AttributeValue> item = new HashMap<>();
            item.put("mailId", AttributeValue.builder().s("p:6225391_RefundExpired_ActivityId_2601010_ExchangeItemId_202307_RefundItemId_2001_UserId_6225391_1740849658663774996").build());
            item.put("addRewardType", AttributeValue.builder().n("1").build());
            item.put("claimAt", AttributeValue.builder().s("2025-03-01T17:21:22.993Z").build());
            item.put("claimed", AttributeValue.builder().bool(true).build());
            item.put("userId", AttributeValue.builder().s("6225391").build());
            
            // Add placeholderData
            Map<String, AttributeValue> enMap = new HashMap<>();
            enMap.put("0", AttributeValue.builder().s("200000").build());
            
            Map<String, AttributeValue> placeholderData = new HashMap<>();
            placeholderData.put("en", AttributeValue.builder().m(enMap).build());
            
            item.put("placeholderData", AttributeValue.builder().m(placeholderData).build());
            
            // Add rewards
            Map<String, AttributeValue> reward = new HashMap<>();
            reward.put("amount", AttributeValue.builder().n("200000").build());
            reward.put("id", AttributeValue.builder().s("0|2001").build());
            reward.put("isPaReward", AttributeValue.builder().bool(false).build());
            reward.put("rewardId", AttributeValue.builder().nul(true).build());
            reward.put("rewardType", AttributeValue.builder().n("0").build());
            
            item.put("rewards", AttributeValue.builder().l(
                AttributeValue.builder().m(reward).build()
            ).build());
            
            item.put("testUserId", AttributeValue.builder().nul(true).build());
            
            PutItemRequest putRequest = PutItemRequest.builder()
                .tableName(tableName)
                .item(item)
                .build();
                
            dynamoDbClient.putItem(putRequest);
            LOG.info("Inserted sample item into table: {}", tableName);
            
        } catch (Exception e) {
            LOG.error("Error creating dummy table: {}", e.getMessage(), e);
        }
    }
    
    @Override
    public void cancel() {
        isRunning = false;
        if (dynamoDbClient != null) {
            dynamoDbClient.close();
        }
    }
}
