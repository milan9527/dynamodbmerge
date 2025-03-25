package com.example.sink;

import com.example.model.GameMailboxItem;
import com.example.model.GameMailboxMergedItem;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Custom sink function for writing to DynamoDB
 */
public class DynamoDBSink extends RichSinkFunction<GameMailboxMergedItem> {
    private static final Logger LOG = LoggerFactory.getLogger(DynamoDBSink.class);
    private static final long serialVersionUID = 1L;
    
    private final String tableName;
    private transient DynamoDbClient dynamoDbClient;
    
    public DynamoDBSink(String tableName) {
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
    public void invoke(GameMailboxMergedItem value, Context context) {
        try {
            // Convert the item to a DynamoDB item
            Map<String, AttributeValue> item = convertToAttributeValues(value);
            
            // Write to DynamoDB
            PutItemRequest putItemRequest = PutItemRequest.builder()
                    .tableName(tableName)
                    .item(item)
                    .build();
            
            dynamoDbClient.putItem(putItemRequest);
            LOG.info("Successfully wrote item with PK: {} to table: {}", value.getPK(), tableName);
        } catch (Exception e) {
            LOG.error("Error writing item to DynamoDB: {}", e.getMessage(), e);
        }
    }
    
    @Override
    public void close() {
        if (dynamoDbClient != null) {
            dynamoDbClient.close();
        }
    }
    
    /**
     * Converts a GameMailboxMergedItem to DynamoDB AttributeValues
     */
    private Map<String, AttributeValue> convertToAttributeValues(GameMailboxMergedItem item) {
        Map<String, AttributeValue> result = new HashMap<>();
        
        // Add the partition keys
        result.put("PK", AttributeValue.builder().s(item.getPK()).build());
        result.put("PK1", AttributeValue.builder().s(item.getPK1()).build());
        result.put("PK2", AttributeValue.builder().s(item.getPK2()).build());
        result.put("gameId", AttributeValue.builder().s(item.getGameId()).build());
        
        // Add other attributes
        if (item.getMailId() != null) {
            result.put("mailId", AttributeValue.builder().s(item.getMailId()).build());
        }
        
        if (item.getAddRewardType() != null) {
            result.put("addRewardType", AttributeValue.builder().n(item.getAddRewardType().toString()).build());
        }
        
        if (item.getClaimAt() != null) {
            result.put("claimAt", AttributeValue.builder().s(item.getClaimAt()).build());
        }
        
        if (item.getClaimed() != null) {
            result.put("claimed", AttributeValue.builder().bool(item.getClaimed()).build());
        }
        
        if (item.getUserId() != null) {
            result.put("userId", AttributeValue.builder().s(item.getUserId()).build());
        }
        
        if (item.getTestUserId() != null) {
            result.put("testUserId", AttributeValue.builder().s(item.getTestUserId()).build());
        } else {
            result.put("testUserId", AttributeValue.builder().nul(true).build());
        }
        
        // Handle placeholderData (Map<String, Map<String, String>>)
        if (item.getPlaceholderData() != null) {
            Map<String, AttributeValue> placeholderMap = new HashMap<>();
            
            for (Map.Entry<String, Map<String, String>> entry : item.getPlaceholderData().entrySet()) {
                Map<String, AttributeValue> innerMap = new HashMap<>();
                
                for (Map.Entry<String, String> innerEntry : entry.getValue().entrySet()) {
                    innerMap.put(innerEntry.getKey(), AttributeValue.builder().s(innerEntry.getValue()).build());
                }
                
                placeholderMap.put(entry.getKey(), AttributeValue.builder().m(innerMap).build());
            }
            
            result.put("placeholderData", AttributeValue.builder().m(placeholderMap).build());
        }
        
        // Handle rewards (List<Reward>)
        if (item.getRewards() != null && !item.getRewards().isEmpty()) {
            List<AttributeValue> rewardsList = new ArrayList<>();
            
            for (GameMailboxItem.Reward reward : item.getRewards()) {
                Map<String, AttributeValue> rewardMap = new HashMap<>();
                
                if (reward.getAmount() != null) {
                    rewardMap.put("amount", AttributeValue.builder().n(reward.getAmount().toString()).build());
                }
                
                if (reward.getId() != null) {
                    rewardMap.put("id", AttributeValue.builder().s(reward.getId()).build());
                }
                
                if (reward.getIsPaReward() != null) {
                    rewardMap.put("isPaReward", AttributeValue.builder().bool(reward.getIsPaReward()).build());
                }
                
                if (reward.getRewardId() != null) {
                    rewardMap.put("rewardId", AttributeValue.builder().s(reward.getRewardId()).build());
                } else {
                    rewardMap.put("rewardId", AttributeValue.builder().nul(true).build());
                }
                
                if (reward.getRewardType() != null) {
                    rewardMap.put("rewardType", AttributeValue.builder().n(reward.getRewardType().toString()).build());
                }
                
                rewardsList.add(AttributeValue.builder().m(rewardMap).build());
            }
            
            result.put("rewards", AttributeValue.builder().l(rewardsList).build());
        }
        
        return result;
    }
}
