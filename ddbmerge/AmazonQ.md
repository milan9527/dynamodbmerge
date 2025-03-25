# DynamoDB Table Merge and Sync with Apache Flink - Implementation Notes

## Solution Architecture

This solution uses Apache Flink on Amazon EMR to merge and continuously sync three DynamoDB tables (GameMailbox1, GameMailbox2, GameMailbox3) into a single target table (GameMailboxMerged) with a new partition key structure.

### Data Flow

1. **Initial Full Load**:
   - Scan all items from each source table
   - Transform items to the target schema
   - Write to the target table

2. **Continuous Data Capture (CDC)**:
   - Capture changes from DynamoDB Streams
   - Transform changes to the target schema
   - Apply changes to the target table

### Key Components

- **DynamoDBFullLoadSource**: Custom Flink source for initial table scan
- **DynamoDBStreamSource**: Custom source for CDC from DynamoDB Streams
- **ItemTransformer**: Transforms source items to target format
- **DynamoDBSink**: Writes transformed items to target table

## Implementation Details

### Partition Key Transformation

The solution transforms the partition keys as follows:

- **PK**: `gameId#mailId`
- **PK1**: `gameId#[part before UserId_ in mailId]`
- **PK2**: `gameId#userId`

### Exactly-Once Processing

The solution uses Flink's checkpointing mechanism to ensure exactly-once processing:

```java
env.enableCheckpointing(60000); // Checkpoint every 60 seconds
env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);
env.getCheckpointConfig().setCheckpointTimeout(120000);
env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
```

### CDC Processing

The CDC processing handles DynamoDB Stream records by:

1. Extracting the NewImage from the record
2. Converting DynamoDB JSON format to standard JSON
3. Creating a GameMailboxItem object
4. Transforming it to a GameMailboxMergedItem
5. Writing to the target table

### Parallel Processing

Each source table is processed in parallel, with separate streams for full load and CDC.

## Optimization Considerations

1. **Batch Size**: The full load uses a batch size of 1000 items to optimize throughput
2. **Connection Pooling**: AWS SDK clients are reused to minimize connection overhead
3. **Error Handling**: Robust error handling to prevent job failures
4. **Logging**: Comprehensive logging for monitoring and troubleshooting

## Monitoring and Maintenance

- Monitor the Flink job using the Flink Web UI
- Check CloudWatch metrics for DynamoDB throughput and throttling
- Set up alarms for job failures or high latency

## Future Enhancements

1. **Backfill Capability**: Add support for backfilling specific time ranges
2. **Schema Evolution**: Handle schema changes in source tables
3. **Performance Tuning**: Optimize parallelism and batch sizes based on workload
4. **Monitoring Dashboard**: Create a custom dashboard for job metrics

## Deployment Instructions

1. Build the project:
   ```bash
   cd /home/ubuntu/ddbmerge
   mvn clean package
   ```

2. Upload to EMR:
   ```bash
   scp -i ~/.ssh/iad.pem target/ddbmerge-1.0-SNAPSHOT.jar hadoop@ec2-44-212-42-172.compute-1.amazonaws.com:/home/hadoop/
   ```

3. Submit the Flink job:
   ```bash
   ssh -i ~/.ssh/iad.pem hadoop@ec2-44-212-42-172.compute-1.amazonaws.com
   
   flink run -d \
     -m yarn-cluster \
     -ynm "DynamoDB-Merge-Job" \
     -c com.example.DynamoDBMergeJob \
     /home/hadoop/ddbmerge-1.0-SNAPSHOT.jar
   ```

4. Monitor the job:
   ```bash
   ssh -i ~/.ssh/iad.pem -N -L 8081:localhost:8081 hadoop@ec2-44-212-42-172.compute-1.amazonaws.com
   ```
   Then open http://localhost:8081 in your browser.
