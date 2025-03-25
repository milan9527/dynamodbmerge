# DynamoDB Table Merge and Sync with Apache Flink

This project implements a solution to merge and continuously sync multiple DynamoDB tables into a single target table using Apache Flink on Amazon EMR.

## Overview

The solution performs the following:

1. Initial full load from three source DynamoDB tables (GameMailbox1, GameMailbox2, GameMailbox3)
2. Continuous data capture (CDC) from the source tables using DynamoDB Streams
3. Transformation of data to the target schema with new partition keys
4. Writing data to the target table (GameMailboxMerged)

## Architecture

- **Source Tables**: Three DynamoDB tables with identical schema (GameMailbox1, GameMailbox2, GameMailbox3)
- **Target Table**: Single DynamoDB table (GameMailboxMerged) with new partition key structure
- **Processing Engine**: Apache Flink 1.16 running on Amazon EMR
- **CDC Mechanism**: DynamoDB Streams

## Prerequisites

- Java 11+
- Maven 3.6+
- AWS CLI configured with appropriate permissions
- Existing EMR cluster named "demo" with Flink 1.16 installed
- Source DynamoDB tables with streams enabled

## Building the Project

```bash
cd /home/ubuntu/ddbmerge
mvn clean package
```

This will create a JAR file in the `target` directory.

## Deployment

1. Upload the JAR file to the EMR cluster:

```bash
scp target/ddbmerge-1.0-SNAPSHOT.jar hadoop@<EMR-MASTER-DNS>:/home/hadoop/
```

2. Submit the Flink job to the cluster:

```bash
ssh hadoop@<EMR-MASTER-DNS>

flink run -d \
  -m yarn-cluster \
  -ynm "DynamoDB-Merge-Job" \
  -c com.example.DynamoDBMergeJob \
  /home/hadoop/ddbmerge-1.0-SNAPSHOT.jar
```

## Monitoring

You can monitor the Flink job using the Flink Web UI:

1. Set up an SSH tunnel to the EMR master node:

```bash
ssh -i <YOUR-KEY.pem> -N -L 8081:localhost:8081 hadoop@<EMR-MASTER-DNS>
```

2. Open your browser and navigate to `http://localhost:8081`

## Data Transformation

The source data format:
```json
{
  "mailId": "p:6225391_RefundExpired_ActivityId_2601010_ExchangeItemId_202307_RefundItemId_2001_UserId_6225391_1740849658663774996",
  "addRewardType": 1,
  "claimAt": "2025-03-01T17:21:22.993Z",
  "claimed": true,
  "placeholderData": {
    "en": {
      "0": "200000"
    }
  },
  "rewards": [
    {
      "amount": 200000,
      "id": "0|2001",
      "isPaReward": false,
      "rewardId": null,
      "rewardType": 0
    }
  ],
  "testUserId": null,
  "userId": "6225391"
}
```

Is transformed to the target format:
```json
{
  "PK": "Souls#p:6225391_RefundExpired_ActivityId_2601010_ExchangeItemId_202307_RefundItemId_2001_UserId_6225391_1740849658663774996",
  "PK1": "Souls#RefundExpired_ActivityId_2601010_ExchangeItemId_202307_RefundItemId_2001_UserId_6225391_1740849658663774996",
  "PK2": "Souls#6225391",
  "gameId": "Souls",
  "mailId": "p:6225391_RefundExpired_ActivityId_2601010_ExchangeItemId_202307_RefundItemId_2001_UserId_6225391_1740849658663774996",
  "addRewardType": 1,
  "claimAt": "2025-03-01T17:21:22.993Z",
  "claimed": true,
  "placeholderData": {
    "en": {
      "0": "200000"
    }
  },
  "rewards": [
    {
      "amount": 200000,
      "id": "0|2001",
      "isPaReward": false,
      "rewardId": null,
      "rewardType": 0
    }
  ],
  "testUserId": null,
  "userId": "6225391"
}
```

## Key Features

- **Exactly-once processing**: Using Flink's checkpointing mechanism
- **Parallel processing**: Each source table is processed in parallel
- **Continuous synchronization**: Changes from source tables are continuously captured and applied to the target
- **No data loss**: Seamless transition from full load to CDC
- **Long-running job**: Designed to run continuously for ongoing CDC

## Implementation Details

- **Full Load**: Uses a custom source function to scan the source tables
- **CDC**: Uses Flink's DynamoDB Streams connector to capture changes
- **Transformation**: Maps source items to target schema with new partition keys
- **Writing**: Uses a custom sink function to write to the target DynamoDB table
