#!/bin/bash

# Build the project
echo "Building the project..."
mvn clean package

# Check if build was successful
if [ $? -ne 0 ]; then
    echo "Build failed. Exiting."
    exit 1
fi

# Upload the JAR file to EMR
echo "Uploading JAR file to EMR..."
scp -i ~/.ssh/iad.pem target/ddbmerge-1.0-SNAPSHOT.jar hadoop@ec2-44-212-42-172.compute-1.amazonaws.com:/home/hadoop/

# Check if upload was successful
if [ $? -ne 0 ]; then
    echo "Upload failed. Exiting."
    exit 1
fi

# Submit the Flink job with significantly reduced parallelism and increased resources
echo "Submitting Flink job..."
ssh -i ~/.ssh/iad.pem hadoop@ec2-44-212-42-172.compute-1.amazonaws.com \
    "flink run -d \
    -m yarn-cluster \
    -ynm \"DynamoDB-Merge-Job\" \
    -p 16 \
    -ys 3 \
    -ytm 4096 \
    -yjm 2048 \
    -yD taskmanager.numberOfTaskSlots=6 \
    -yD yarn.containers.vcores=4 \
    -yD jobmanager.memory.process.size=2560m \
    -yD taskmanager.memory.process.size=4608m \
    -c com.example.DynamoDBMergeJob \
    /home/hadoop/ddbmerge-1.0-SNAPSHOT.jar"

echo "Job submitted successfully!"
echo "To monitor the job, run:"
echo "ssh -i ~/.ssh/iad.pem -N -L 8081:localhost:8081 hadoop@ec2-44-212-42-172.compute-1.amazonaws.com"
echo "Then open http://localhost:8081 in your browser."
