import json
import boto3

firehose = boto3.client('firehose')
deliveryStreamName = 'PUT-RED-ZlAir'

def convertToFirehoseRecord(ddbRecord):
    newImage = ddbRecord['NewImage']
    try:
        firehoseRecord = "{},{},{}".format(newImage['id']['S'],
                                           newImage['name']['S'],
                                           newImage['age']['S']) + '\n'
    except KeyError as e:
        raise ValueError(f"Missing key in DynamoDB record: {e}")
    return firehoseRecord

def lambda_handler(event, context):
    print(event)
    processed_records = 0
    for record in event['Records']:
        print(record)
        print(record['dynamodb'])
        ddbRecord = record['dynamodb']
        print('DDB record:' + json.dumps(ddbRecord))

        try:
            firehoseRecord = convertToFirehoseRecord(ddbRecord)
        except ValueError as e:
            print(e)
            continue  # Skip this record

        print('firehose Record:' + firehoseRecord)

        try:
            result = firehose.put_record(DeliveryStreamName=deliveryStreamName, Record={'Data': firehoseRecord})
            print(result)
        except Exception as e:
            print(f"Error sending to Firehose: {e}")
            continue  # Skip this record
        
        processed_records += 1

    return 'processed {} records'.format(processed_records)
