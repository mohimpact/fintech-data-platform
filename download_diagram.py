import base64
import json
import urllib.request

mermaid_code = """graph TD
    classDef aws fill:#FF9900,stroke:#232F3E,stroke-width:2px,color:white;
    classDef kafka fill:#231F20,stroke:#FFFFFF,stroke-width:2px,color:white;
    classDef spark fill:#E25A1C,stroke:#FFFFFF,stroke-width:2px,color:white;
    classDef iceberg fill:#00A0D1,stroke:#FFFFFF,stroke-width:2px,color:white;
    classDef python fill:#3776AB,stroke:#FFD43B,stroke-width:2px,color:white;

    subgraph Phase 1: Real-time Data Generation
        Gen[Mock Data Generator]:::python
        TopicRaw[(Kafka: raw_transactions)]:::kafka
        Gen -- Produces JSON --> TopicRaw
    end

    subgraph Phase 2: Stream Processing & Data Quality
        StreamApp[PySpark Streaming Job]:::spark
        GX{Great Expectations Validation}:::python
        TopicClean[(Kafka: clean_transactions)]:::kafka
        TopicDLQ[(Kafka: dead_letter_queue)]:::kafka

        TopicRaw --> StreamApp
        StreamApp -- foreachBatch --> GX
        GX -- Valid --> TopicClean
        GX -- Invalid --> TopicDLQ
    end

    subgraph Phase 3: The Medallion Lakehouse
        BronzeApp[PySpark Bronze Ingestion]:::spark
        BronzeTable[(AWS S3: Bronze Iceberg)]:::aws
        
        ETL[PySpark Batch ETL Job]:::spark
        SilverTable[(AWS S3: Silver Iceberg)]:::aws
        GoldTable[(AWS S3: Gold Iceberg)]:::aws

        TopicClean --> BronzeApp
        BronzeApp -- Appends 1-min Batches --> BronzeTable
        BronzeTable --> ETL
        ETL -- Deduplication --> SilverTable
        ETL -- Aggregations --> GoldTable
    end
"""

state = {
    "code": mermaid_code,
    "mermaid": {"theme": "default"}
}
json_str = json.dumps(state)
b64_str = base64.urlsafe_b64encode(json_str.encode('utf-8')).decode('utf-8')

url = f"https://mermaid.ink/img/{b64_str}?type=png&bgColor=!white"
print(f"Downloading from {url}")

try:
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    with urllib.request.urlopen(req) as response:
        with open('fintech_architecture.png', 'wb') as f:
            f.write(response.read())
    print("Downloaded successfully to fintech_architecture.png")
except Exception as e:
    print(f"Error downloading image: {e}")
