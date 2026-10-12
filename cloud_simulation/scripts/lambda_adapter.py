import json
from pathlib import Path


input_file = Path("events/sns-event.json")
output_file = Path("events/sqs-event.json")


with open(input_file, "r", encoding="utf-8") as f:
    sns_event = json.load(f)


sqs_records = []

for record in sns_event["Records"]:

    # The SNS MessageId becomes our simulated SQS messageId.
    message_id = record["MessageId"]

    # Everything that was inside the SNS notification becomes
    # the body of the SQS message.
    sns_body = json.dumps(record)

    sqs_record = {
        "messageId": message_id,
        "receiptHandle": f"local-test-{message_id}",
        "body": sns_body,
        "attributes": {
            "ApproximateReceiveCount": "1",
            "SentTimestamp": "0",
        },
        "messageAttributes": {},
        "md5OfBody": "",
        "eventSource": "aws:sqs",
        "eventSourceARN": "arn:aws:sqs:local:000000000000:openaq-queue",
        "awsRegion": "us-east-1",
    }

    sqs_records.append(sqs_record)


sqs_event = {
    "Records": sqs_records
}


with open(output_file, "w", encoding="utf-8") as f:
    json.dump(sqs_event, f, indent=2, ensure_ascii=False)


print(f"Created {output_file}")
print(f"Converted {len(sqs_records)} messages")