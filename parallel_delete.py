#!/usr/bin/env python3
import boto3
import time
import sys
import threading
import concurrent.futures
from botocore.exceptions import ClientError

class DynamoDBTableCleaner:
    def __init__(self, table_name, region='us-east-1', segments=10, batch_size=25):
        """
        Initialize the DynamoDB table cleaner
        
        Args:
            table_name (str): Name of the DynamoDB table
            region (str): AWS region
            segments (int): Number of parallel segments to scan
            batch_size (int): Number of items to process in each batch
        """
        self.table_name = table_name
        self.region = region
        self.segments = segments
        self.batch_size = batch_size
        self.dynamodb = boto3.resource('dynamodb', region_name=region)
        self.table = self.dynamodb.Table(table_name)
        self.client = boto3.client('dynamodb', region_name=region)
        self.lock = threading.Lock()
        self.total_deleted = 0
        self.key_name = None
        
    def get_key_schema(self):
        """Get the primary key name from the table schema"""
        response = self.client.describe_table(TableName=self.table_name)
        key_schema = response['Table']['KeySchema']
        for key in key_schema:
            if key['KeyType'] == 'HASH':
                return key['AttributeName']
        return None
        
    def delete_segment(self, segment, total_segments):
        """
        Delete items from a specific segment of the table
        
        Args:
            segment (int): The segment number to process
            total_segments (int): Total number of segments
        """
        deleted_count = 0
        last_evaluated_key = None
        
        while True:
            # Prepare scan parameters
            scan_params = {
                'TableName': self.table_name,
                'Segment': segment,
                'TotalSegments': total_segments,
                'Limit': self.batch_size,
                'ReturnConsumedCapacity': 'TOTAL'
            }
            
            if last_evaluated_key:
                scan_params['ExclusiveStartKey'] = last_evaluated_key
                
            # Scan the segment
            try:
                response = self.client.scan(**scan_params)
                items = response.get('Items', [])
                
                if not items:
                    break
                    
                # Process items in batches
                batch_items = []
                for item in items:
                    key_value = item[self.key_name]
                    batch_items.append({
                        'DeleteRequest': {
                            'Key': {self.key_name: key_value}
                        }
                    })
                    
                # Delete items in batch
                if batch_items:
                    try:
                        self.client.batch_write_item(
                            RequestItems={
                                self.table_name: batch_items
                            }
                        )
                        deleted_count += len(batch_items)
                        
                        # Update total count with thread safety
                        with self.lock:
                            self.total_deleted += len(batch_items)
                            if self.total_deleted % 1000 == 0:
                                print(f"Total deleted: {self.total_deleted} items")
                                
                    except ClientError as e:
                        print(f"Error in batch delete: {e}")
                        # Fall back to individual deletes
                        for item in items:
                            try:
                                self.table.delete_item(Key={self.key_name: item[self.key_name]})
                                deleted_count += 1
                                with self.lock:
                                    self.total_deleted += 1
                            except Exception as e2:
                                print(f"Error deleting individual item: {e2}")
                
                # Update for next iteration
                last_evaluated_key = response.get('LastEvaluatedKey')
                if not last_evaluated_key:
                    break
                    
                # Add a small delay to avoid throttling
                time.sleep(0.05)
                
            except Exception as e:
                print(f"Error scanning segment {segment}: {e}")
                time.sleep(1)  # Longer delay on error
                
        return deleted_count
        
    def clean_table(self):
        """Clean the entire table using parallel segments"""
        start_time = time.time()
        
        # Get the primary key name
        self.key_name = self.get_key_schema()
        if not self.key_name:
            print("Could not determine primary key name")
            return False
            
        print(f"Primary key is: {self.key_name}")
        print(f"Starting parallel deletion with {self.segments} segments...")
        
        # Use thread pool to process segments in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.segments) as executor:
            futures = []
            for segment in range(self.segments):
                futures.append(executor.submit(self.delete_segment, segment, self.segments))
                
            # Wait for all futures to complete
            for future in concurrent.futures.as_completed(futures):
                try:
                    segment_deleted = future.result()
                    print(f"Segment completed, deleted {segment_deleted} items")
                except Exception as e:
                    print(f"Segment failed with error: {e}")
                    
        end_time = time.time()
        duration = end_time - start_time
        
        print(f"Deletion completed. Total deleted: {self.total_deleted} items in {duration:.2f} seconds")
        return True

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python parallel_delete.py <table_name> [region] [segments]")
        sys.exit(1)
        
    table_name = sys.argv[1]
    region = sys.argv[2] if len(sys.argv) > 2 else 'us-east-1'
    segments = int(sys.argv[3]) if len(sys.argv) > 3 else 10
    
    cleaner = DynamoDBTableCleaner(table_name, region, segments)
    cleaner.clean_table()
