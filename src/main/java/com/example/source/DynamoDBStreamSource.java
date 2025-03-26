package com.example.source;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.streamsadapter.AmazonDynamoDBStreamsAdapterClient;
import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordAdapter;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.model.Record;
import com.example.model.GameMailboxItem;
import com.example.model.GameMailboxMergedItem;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Custom source function for CDC from DynamoDB Streams using the Kinesis Client Library (KCL)
 * This implementation uses the DynamoDB Streams Adapter to read from DynamoDB Streams
 */
public class DynamoDBStreamSource extends RichSourceFunction<GameMailboxItem> {
    private static final Logger LOG = LoggerFactory.getLogger(DynamoDBStreamSource.class);
    private static final long serialVersionUID = 1L;
    private static final String REGION = "us-east-1";
    private static final String GAME_ID = "Souls";
    private static final int QUEUE_CAPACITY = 10000;
    
    private final String streamArn;
    private transient Worker worker;
    private transient BlockingQueue<Object> recordQueue;
    private volatile boolean isRunning = true;
    
    public DynamoDBStreamSource(String streamArn) {
        this.streamArn = streamArn;
    }
    
    @Override
    public void open(Configuration parameters) {
        recordQueue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);
        
        // Extract table name from stream ARN for use as application name
        String tableName = extractTableNameFromStreamArn(streamArn);
        String applicationName = "DynamoDBStreamProcessor-" + tableName;
        
        // Create a KCL worker to process the stream
        AmazonDynamoDBStreamsAdapterClient adapterClient = new AmazonDynamoDBStreamsAdapterClient(
                DefaultAWSCredentialsProviderChain.getInstance());
        adapterClient.setRegion(com.amazonaws.regions.Region.getRegion(com.amazonaws.regions.Regions.US_EAST_1));
        
        // Configure the KCL worker
        KinesisClientLibConfiguration kclConfig = new KinesisClientLibConfiguration(
                applicationName,
                streamArn,
                DefaultAWSCredentialsProviderChain.getInstance(),
                applicationName + "-worker")
                .withInitialPositionInStream(InitialPositionInStream.LATEST)
                .withRegionName(REGION);
        
        // Create and start the worker in a separate thread
        worker = new Worker.Builder()
                .recordProcessorFactory(new StreamRecordProcessorFactory(recordQueue))
                .config(kclConfig)
                .kinesisClient(adapterClient)
                .build();
        
        Thread workerThread = new Thread(worker);
        workerThread.setDaemon(true);
        workerThread.start();
        
        LOG.info("Started DynamoDB Stream processor for stream: {}", streamArn);
    }
    
    @Override
    public void run(SourceContext<GameMailboxItem> ctx) throws Exception {
        LOG.info("Starting CDC from DynamoDB Stream: {}", streamArn);
        
        while (isRunning) {
            try {
                // Poll the queue for records processed by the KCL worker
                Object item = recordQueue.poll(100, TimeUnit.MILLISECONDS);
                if (item != null) {
                    synchronized (ctx.getCheckpointLock()) {
                        if (item instanceof GameMailboxItem) {
                            ctx.collect((GameMailboxItem) item);
                        } else if (item instanceof GameMailboxMergedItem) {
                            // This is a delete operation, we need to pass it through
                            // Since our sink expects GameMailboxMergedItem, we'll convert it back
                            ctx.collect((GameMailboxItem) item);
                        }
                    }
                }
            } catch (InterruptedException e) {
                if (isRunning) {
                    LOG.warn("Interrupted while waiting for records", e);
                }
            } catch (Exception e) {
                LOG.error("Error processing records from DynamoDB Stream", e);
            }
        }
    }
    
    @Override
    public void cancel() {
        isRunning = false;
        if (worker != null) {
            try {
                worker.shutdown();
            } catch (Exception e) {
                LOG.error("Error shutting down KCL worker", e);
            }
        }
    }
    
    private String extractTableNameFromStreamArn(String streamArn) {
        // ARN format: arn:aws:dynamodb:region:account-id:table/table-name/stream/timestamp
        String[] parts = streamArn.split("/");
        if (parts.length >= 2) {
            return parts[1];
        }
        return "unknown-table";
    }
    
    /**
     * Factory for creating record processors that will handle DynamoDB Stream records
     */
    private static class StreamRecordProcessorFactory implements IRecordProcessorFactory {
        private final BlockingQueue<Object> queue;
        
        public StreamRecordProcessorFactory(BlockingQueue<Object> queue) {
            this.queue = queue;
        }
        
        @Override
        public IRecordProcessor createProcessor() {
            return new StreamRecordProcessor(queue);
        }
    }
    
    /**
     * Processor for DynamoDB Stream records
     */
    private static class StreamRecordProcessor implements IRecordProcessor {
        private static final Logger LOG = LoggerFactory.getLogger(StreamRecordProcessor.class);
        private final BlockingQueue<Object> queue;
        private String shardId;
        
        public StreamRecordProcessor(BlockingQueue<Object> queue) {
            this.queue = queue;
        }
        
        @Override
        public void initialize(InitializationInput initializationInput) {
            this.shardId = initializationInput.getShardId();
            LOG.info("Initialized record processor for shard: {}", shardId);
        }
        
        @Override
        public void processRecords(ProcessRecordsInput processRecordsInput) {
            for (Record record : processRecordsInput.getRecords()) {
                try {
                    // Convert Kinesis adapter record to DynamoDB Streams record
                    com.amazonaws.services.dynamodbv2.model.Record dynamoRecord = 
                            ((RecordAdapter) record).getInternalObject();
                    
                    // Process based on event type
                    String eventName = dynamoRecord.getEventName();
                    if ("INSERT".equals(eventName) || "MODIFY".equals(eventName)) {
                        // Get the new image of the item
                        Map<String, AttributeValue> newImage = dynamoRecord.getDynamodb().getNewImage();
                        if (newImage != null && !newImage.isEmpty()) {
                            // Convert to GameMailboxItem
                            GameMailboxItem item = convertToGameMailboxItem(newImage);
                            
                            // Add to the queue for the source function to emit
                            if (!queue.offer(item, 5, TimeUnit.SECONDS)) {
                                LOG.warn("Queue is full, dropping record from shard: {}", shardId);
                            }
                            
                            LOG.info("Processed {} event for item: {}", eventName, 
                                    newImage.get("mailId").getS());
                        }
                    } else if ("REMOVE".equals(eventName)) {
                        // For REMOVE events, create a delete marker
                        Map<String, AttributeValue> oldImage = dynamoRecord.getDynamodb().getOldImage();
                        if (oldImage != null && oldImage.containsKey("mailId") && oldImage.containsKey("userId")) {
                            String mailId = oldImage.get("mailId").getS();
                            String userId = oldImage.get("userId").getS();
                            
                            // Create a delete marker
                            GameMailboxMergedItem deleteMarker = new GameMailboxMergedItem(
                                    mailId, userId, "Souls", true);
                            
                            // Add to the queue
                            if (!queue.offer(deleteMarker, 5, TimeUnit.SECONDS)) {
                                LOG.warn("Queue is full, dropping delete marker from shard: {}", shardId);
                            }
                            
                            LOG.info("Created delete marker for removed item: {}", mailId);
                        } else {
                            LOG.warn("Received REMOVE event without required fields in old image");
                        }
                    }
                } catch (Exception e) {
                    LOG.error("Error processing record from shard {}: {}", shardId, e.getMessage(), e);
                }
            }
            
            // Checkpoint progress
            try {
                processRecordsInput.getCheckpointer().checkpoint();
            } catch (InvalidStateException | ShutdownException | ThrottlingException e) {
                LOG.error("Error checkpointing progress for shard {}: {}", shardId, e.getMessage(), e);
            }
        }
        
        @Override
        public void shutdown(ShutdownInput shutdownInput) {
            LOG.info("Shutting down record processor for shard: {}", shardId);
        }
        
        /**
         * Convert DynamoDB item to GameMailboxItem
         */
        private GameMailboxItem convertToGameMailboxItem(Map<String, AttributeValue> item) {
            GameMailboxItem mailboxItem = new GameMailboxItem();
            
            // Set basic fields
            if (item.containsKey("mailId")) {
                mailboxItem.setMailId(item.get("mailId").getS());
            }
            
            if (item.containsKey("addRewardType")) {
                mailboxItem.setAddRewardType(Integer.parseInt(item.get("addRewardType").getN()));
            }
            
            if (item.containsKey("claimAt")) {
                mailboxItem.setClaimAt(item.get("claimAt").getS());
            }
            
            if (item.containsKey("claimed")) {
                mailboxItem.setClaimed(item.get("claimed").getBOOL());
            }
            
            if (item.containsKey("userId")) {
                mailboxItem.setUserId(item.get("userId").getS());
            }
            
            if (item.containsKey("testUserId")) {
                AttributeValue testUserIdAttr = item.get("testUserId");
                if (testUserIdAttr.getNULL() != null && testUserIdAttr.getNULL()) {
                    mailboxItem.setTestUserId(null);
                } else if (testUserIdAttr.getS() != null) {
                    mailboxItem.setTestUserId(testUserIdAttr.getS());
                }
            }
            
            // Handle placeholderData
            if (item.containsKey("placeholderData") && item.get("placeholderData").getM() != null) {
                Map<String, Map<String, String>> placeholderData = new HashMap<>();
                Map<String, AttributeValue> placeholderMap = item.get("placeholderData").getM();
                
                for (Map.Entry<String, AttributeValue> entry : placeholderMap.entrySet()) {
                    if (entry.getValue().getM() != null) {
                        Map<String, String> innerMap = new HashMap<>();
                        Map<String, AttributeValue> attrInnerMap = entry.getValue().getM();
                        
                        for (Map.Entry<String, AttributeValue> innerEntry : attrInnerMap.entrySet()) {
                            if (innerEntry.getValue().getS() != null) {
                                innerMap.put(innerEntry.getKey(), innerEntry.getValue().getS());
                            }
                        }
                        
                        placeholderData.put(entry.getKey(), innerMap);
                    }
                }
                
                mailboxItem.setPlaceholderData(placeholderData);
            }
            
            // Handle rewards
            if (item.containsKey("rewards") && item.get("rewards").getL() != null) {
                List<GameMailboxItem.Reward> rewards = new ArrayList<>();
                List<AttributeValue> rewardsList = item.get("rewards").getL();
                
                for (AttributeValue rewardAttr : rewardsList) {
                    if (rewardAttr.getM() != null) {
                        Map<String, AttributeValue> rewardMap = rewardAttr.getM();
                        GameMailboxItem.Reward reward = new GameMailboxItem.Reward();
                        
                        if (rewardMap.containsKey("amount") && rewardMap.get("amount").getN() != null) {
                            reward.setAmount(Integer.parseInt(rewardMap.get("amount").getN()));
                        }
                        
                        if (rewardMap.containsKey("id") && rewardMap.get("id").getS() != null) {
                            reward.setId(rewardMap.get("id").getS());
                        }
                        
                        if (rewardMap.containsKey("isPaReward") && rewardMap.get("isPaReward").getBOOL() != null) {
                            reward.setIsPaReward(rewardMap.get("isPaReward").getBOOL());
                        }
                        
                        if (rewardMap.containsKey("rewardId")) {
                            AttributeValue rewardIdAttr = rewardMap.get("rewardId");
                            if (rewardIdAttr.getNULL() != null && rewardIdAttr.getNULL()) {
                                reward.setRewardId(null);
                            } else if (rewardIdAttr.getS() != null) {
                                reward.setRewardId(rewardIdAttr.getS());
                            }
                        }
                        
                        if (rewardMap.containsKey("rewardType") && rewardMap.get("rewardType").getN() != null) {
                            reward.setRewardType(Integer.parseInt(rewardMap.get("rewardType").getN()));
                        }
                        
                        rewards.add(reward);
                    }
                }
                
                mailboxItem.setRewards(rewards);
            }
            
            return mailboxItem;
        }
    }
}
