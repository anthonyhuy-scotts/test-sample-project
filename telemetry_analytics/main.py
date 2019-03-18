import os
import sys
import argparse
import logging
import unicodecsv as csv
import time
from psycopg2.extras import NamedTupleCursor
from telemetry_model import DeviceTelemetry
from postgress_conn import PostgresConn

# fields = ['serial', 'dhcpStatus', 'fsVer', 'fwver', 'connType', 'ticks', 'rssi', 'nf', 'bootpart', 'fault', 'bootpart']
fields = ['serial', 'd_rssi_l', 'd_rssi_a']

SQL_QUERY_FILEPATH_GET_FIRMWARE_VERSION = 'sql_query_get_firmware_versions.sql'
SQL_QUERY_FILEPATH_GET_CONTROLLER_MODELS = 'sql_query_get_controller_models.sql'

date_window = [1, 7, 14]



def get_active_devices(model):

    global list_of_controllers
    global conn

    with open('scripts/controller_query.sql', 'r') as f:
        query_controllers = f.read()

    with open('scripts/query_get_controller_models.sql', 'r') as f:
        query_get_controller_models = f.read()

    # Read data
    with conn.cursor(cursor_factory=NamedTupleCursor) as cursor:
        cursor.execute(query_controllers.format(model, '0.1.6148'))
        gros = cursor.fetchall()
    for x in gros:
        print(x)
    print("size={}".format(len(gros)))
    list_of_controllers = list(gros)


def query_data(starting_date, max_records, model, limit):

    """
    dkdke
    dd
    :keywordd

    """

    global list_of_controllers

    print(starting_date)

    # Get count of query
    value_count = DeviceTelemetry.count(
        filter_condition=DeviceTelemetry.key.is_in(list_of_controllers),
        range_key_condition=DeviceTelemetry.timestamp > starting_date
    )
    print("Total count {}".format(value_count))
    #time.sleep(5)
    #exit(1)
    #	and s.firmware_version='0.1.6148'

    # Read each block
    cnt = 0
    for obj in list_of_controllers[:limit]:

        print("\n---------------------------------------------\n")
        controller_uuid = obj.uuid

        cnt += 1
        try:

            if True:
                telemetry_set = list(DeviceTelemetry.query(
                    controller_uuid,
                    #filter_condition=DeviceTelemetry.key.is_in(*fields),
                    range_key_condition=starting_date < DeviceTelemetry.timestamp,
                    limit=max_records
                ), )

                if True:
                    print("{} uuid={} | count={}".format(cnt, controller_uuid, len(telemetry_set)))
                else:
                    timez = [x.timestamp for x in telemetry_set]
                    table = [(x.timestamp.isoformat(), x.key, x.value) for x in telemetry_set]
                    with open('data/{}/ctrl-{}.csv'.format(model, controller_uuid), 'wb') as f:
                        writer = csv.writer(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                        writer.writerows(table)

                    cnt2 = 0
                    for x in telemetry_set:
                        print("{} | {} {} | {} | {}={}".format(cnt2, x.timestamp, x.timestamp.tzinfo, x.device,
                                                               x.key, x.value))
                        cnt2 += 1

            else:

                value_count = DeviceTelemetry.count(
                    hash_key=controller_uuid
                )
                print("{} = {}".format(controller_uuid, value_count))

                time.sleep(5)

        except Exception as ex:

                print("Exception: {}".format(ex))


if __name__ == '__main__':

    # Parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--max_records', type=int, default=20000)
    parser.add_argument('--verbose', type=bool)
    parser.add_argument('--model', choices=['gro7', 'blossom'], default='gro7')
    parser.add_argument('--limit',type=int,default=10)
    parser.add_argument('--window_days', type=int, choices=[1, 7, 14, 30], default=1)
    args = parser.parse_args()

    # Prep logging
    logging.basicConfig(level=(logging.DEBUG if args.verbose else logging.INFO), stream=sys.stdout)

    # Connect to postgres RDS. Exit if no good
    postgres_conn = PostgresConn(os.getenv("POSTGRES_CONNECTION"))
    if not postgres_conn.connect():
        exit(1)

    # Read controller models and process for each one
    recs = postgres_conn.run_query(SQL_QUERY_FILEPATH_GET_CONTROLLER_MODELS)
    for x in recs:

        print("Processing records for model={}".format(x.name))

        recs = postgres_conn.run_query(SQL_QUERY_FILEPATH_GET_FIRMWARE_VERSION)
        for r in recs:
            print(r)

        for i in date_window:
            print(i)

    exit(1)



        # Get start date for statistical window
        #start_date = datetime.now() - timedelta(args.window_days)




    exit(1)

    # Get statistic across all samples on all devices per firmware
    get_active_devices(args.model)

    # Get statistic across all samples on single device per firmware
    # Also mark / tag if single device is out of band with standard deviation from
    #

    query_data(start_date, args.max_records, args.model, args.limit)
