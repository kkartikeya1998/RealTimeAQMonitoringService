import json

from writer.handler import lambda_handler


def test_writer():

    with open("events/sqs-event.json") as f:
        event = json.load(f)

    result = lambda_handler(event, None)

    print(result)

    assert "batchItemFailures" in result
    assert result["batchItemFailures"] == []