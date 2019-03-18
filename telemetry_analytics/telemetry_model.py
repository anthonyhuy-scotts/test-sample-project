import os
from pynamodb.models import Model as DynamoModel
from pynamodb.attributes import UnicodeAttribute, UTCDateTimeAttribute
from pynamodb.indexes import GlobalSecondaryIndex, AllProjection

ENV = os.getenv('DBENV', 'production')


class KeyIndex(GlobalSecondaryIndex):
    class Meta:
        read_capacity_units = 4
        write_capacity_units = 52
        index_name = ENV + '.TelemetryKeyIndex'
        projection = AllProjection()

    key = UnicodeAttribute(hash_key=True)


class DeviceTelemetry(DynamoModel):
    # Dynamo DB
    class Meta:
        table_name = ENV + '.Telemetry'
        region = 'us-west-1'

    device = UnicodeAttribute(hash_key=True)
    timestamp = UTCDateTimeAttribute(range_key=True)
    key = UnicodeAttribute()
    value = UnicodeAttribute()
    key_index = KeyIndex()
