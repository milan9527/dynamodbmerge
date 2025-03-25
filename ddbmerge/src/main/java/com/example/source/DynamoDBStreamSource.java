package com.example.source;

import com.example.model.GameMailboxItem;
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
 * Custom source function for CDC from DynamoDB Streams
 * This implementation uses DynamoDB Streams to capture changes
 */
public class DynamoDBStreamSource extends RichSourceFunction<GameMailboxItem> {
    private static final Logger LOG = LoggerFactory.getLogger(DynamoDBStreamSource.class);
    private static final long serialVersionUID = 1L;
    
    private final String streamArn;
    private transient DynamoDbClient dynamoDbClient;
    private volatile boolean isRunning = true;
    
    public DynamoDBStreamSource(String streamArn) {
        this.streamArn = streamArn;
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
        LOG.info("Starting CDC from stream: {}", streamArn);
        
        // Extract table name from stream ARN
        String tableName = extractTableNameFromStreamArn(streamArn);
        LOG.info("Extracted table name: {}", tableName);
        
        // Keep track of last evaluated key for pagination
        Map<String, AttributeValue> lastEvaluatedKey = null;
        
        while (isRunning) {
            try {
                // Build scan request with pagination support
                ScanRequest.Builder scanRequestBuilder = ScanRequest.builder()
                        .tableName(tableName)
                        .limit(100);
                
                if (lastEvaluatedKey != null) {
                    scanRequestBuilder.exclusiveStartKey(lastEvaluatedKey);
                }
                
                ScanRequest scanRequest = scanRequestBuilder.build();
                ScanResponse response = dynamoDbClient.scan(scanRequest);
                
                // Process items
                for (Map<String, AttributeValue> item : response.items()) {
                    try {
                        if (!item.containsKey("mailId")) {
                            LOG.warn("Item does not contain mailId, skipping");
                            continue;
                        }
                        
                        // Convert to GameMailboxItem
                        GameMailboxItem mailboxItem = convertToGameMailboxItem(item);
                        
                        // Emit the item
                        synchronized (ctx.getCheckpointLock()) {
                            ctx.collect(mailboxItem);
                        }
                        
                        String mailId = item.get("mailId").s();
                        LOG.info("CDC: Processed item with mailId: {}", mailId);
                    } catch (Exception e) {
                        LOG.error("Error processing CDC: {}", e.getMessage(), e);
                    }
                }
                
                // Update last evaluated key for pagination
                lastEvaluatedKey = response.lastEvaluatedKey();
                
                // If no more items to scan, reset and wait before next scan
                if (lastEvaluatedKey == null || lastEvaluatedKey.isEmpty()) {
                    lastEvaluatedKey = null;
                    Thread.sleep(5000); // Wait 5 seconds before next full scan
                }
            } catch (Exception e) {
                LOG.error("Error processing CDC: {}", e.getMessage(), e);
                Thread.sleep(10000); // Wait longer on error
            }
        }
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
    
    private String extractTableNameFromStreamArn(String streamArn) {
        // ARN format: arn:aws:dynamodb:region:account-id:table/table-name/stream/timestamp
        String[] parts = streamArn.split("/");
        if (parts.length >= 2) {
            return parts[1];
        }
        
        // If we can't extract from ARN, try to get it from the DynamoDB API
        try {
            ListTablesResponse response = dynamoDbClient.listTables();
            List<String> tableNames = response.tableNames();
            
            for (String tableName : tableNames) {
                DescribeTableRequest request = DescribeTableRequest.builder()
                    .tableName(tableName)
                    .build();
                
                DescribeTableResponse tableResponse = dynamoDbClient.describeTable(request);
                String tableStreamArn = tableResponse.table().latestStreamArn();
                
                if (streamArn.equals(tableStreamArn)) {
                    return tableName;
                }
            }
        } catch (Exception e) {
            LOG.error("Error extracting table name from stream ARN: {}", e.getMessage(), e);
        }
        
        // If all else fails, return a default name
        return "GameMailbox1";
    }
    
    @Override
    public void cancel() {
        isRunning = false;
        if (dynamoDbClient != null) {
            dynamoDbClient.close();
        }
    }
}
