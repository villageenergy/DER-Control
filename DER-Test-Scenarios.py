

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from boto3.session import Session
from io import StringIO
import random, boto3, pprint, datetime, sagemaker, time, ast
import timeit

bucketName = 'testsmdata2'
prefix = 'DERTestData/'

def write_df_to_csv(bkt, prefix, f_name, df):

    """
    Writes the results on s3
    """

    path='{}{}'.format(prefix,f_name)
    print(f'{path}\n')

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    s3_resource = boto3.client('s3')
    s3_resource.put_object(Body=csv_buffer.getvalue(), Bucket=bkt,
                           Key=path)



def readStreamData():
    client = boto3.client('kinesis')

    shard_id = 'shardId-000000000000' #we only have one shard!
    shard_it = client.get_shard_iterator(StreamName="representativeDERResultStream", ShardId=shard_id, ShardIteratorType="LATEST")["ShardIterator"]
    #shard_it = client.get_shard_iterator(StreamName="Veda5sMessage", ShardId=shard_id, ShardIteratorType="LATEST")["ShardIterator"]
    print('Waiting for data from DER ...')

    records = []
    while len(records) == 0:
        out = client.get_records(ShardIterator=shard_it, Limit=2)
        shard_it = out["NextShardIterator"]
        records = out['Records']
        #print (records);
        time.sleep(1)

    #print(records)
    recordDER = records[0]
    recordNetwork = records[1]
    #print('\n', recordDER, '\n', recordNetwork )
    print('Data is ready ...')
    return recordDER, recordNetwork


def binary2Dict(record):

    byte_str = record['Data']
    dict_str = byte_str.decode("UTF-8")
    data = ast.literal_eval(dict_str)
    return data


def filterRecords(Networkdata, DERdata):
    Networkrecords = {}
    for key in Networkdata.keys():
        #print(key)
        Networkrecords[key] = Networkdata[key]
    #print(Networkrecords)

    Networkrecords.pop('DERstd10Sec', None)
    Networkrecords.pop('DERstd30Sec', None)
    Networkrecords.pop('DERstd1Min', None)
    Networkrecords.pop('DERstd2Min', None)
    Networkrecords.pop('DERstd5Min', None)

    Networkrecords.pop('DEREnergy10Sec', None)
    Networkrecords.pop('DEREnergy30Sec', None)
    Networkrecords.pop('DEREnergy1Min', None)
    Networkrecords.pop('DEREnergy2Min', None)
    Networkrecords.pop('DEREnergy5Min', None)
    #print(Networkrecords)

    DERrecords = {}
    for key in DERdata.keys():
        #print(key)
        DERrecords[key] = DERdata[key]
    #print(DERrecords)
    #records['TimeStamp'] = records['ApproximateArrivalTimestamp']

    DERrecords.pop('Networkstd10Sec', None)
    DERrecords.pop('Networkstd30Sec', None)
    DERrecords.pop('Networkstd1Min', None)
    DERrecords.pop('Networkstd2Min', None)
    DERrecords.pop('Networkstd5Min', None)

    DERrecords.pop('networkEnergy10Sec', None)
    DERrecords.pop('networkEnergy30Sec', None)
    DERrecords.pop('networkEnergy1Min', None)
    DERrecords.pop('networkEnergy2Min', None)
    DERrecords.pop('networkEnergy5Min', None)
    #print(DERrecords)

    records = {}
    if DERrecords['eventTime'] == Networkrecords['eventTime']:
        for key in DERrecords.keys():
            records[key] = DERrecords[key]

        for key in Networkrecords.keys():
            records[key] = Networkrecords[key]

    return records


def finalDataFrame(records, s, t):
    df = pd.DataFrame(records, index=[0])
    df ['state'] = s
    df ['duration'] = t[0]
    for timespan in ['10Sec', '30Sec', '1Min', '2Min', '5Min']:
    #if (df['networkEnergy' + timespan])!=0:
        df['pen' + timespan] = (1.5 * df['DEREnergy' + timespan]) / ((1.5 * df['DEREnergy' + timespan]) + df['networkEnergy' + timespan])

    for timespan in ['10Sec', '30Sec', '1Min', '2Min', '5Min']:
        df = df.drop (columns = 'DEREnergy' + timespan)
        df = df.drop (columns = 'networkEnergy' + timespan)

    df['eventTime'] = pd.to_datetime(df['eventTime']) + datetime.timedelta(hours = 5, minutes =30)
    df['eventTime'] = df['eventTime'].apply(lambda x: x.strftime('%Y%m%dT%H%M%SZ')).astype(str)

    return df


N = 2

durationTimes = [10, 20, 30, 60, 120]
States = [[1, 1, 1, 1],
          [0, 1, 1, 1],
          [0, 0, 1, 1],
          [0, 0, 0, 1]
         ]


start = timeit.default_timer()

state , duration = [[1, 1, 1, 1]] , [0]
i = 0
dfAll = pd.DataFrame()

for i in range (N):

    dfStage = pd.DataFrame()
    i += 1
    print (f'Sample Number {i} :')

    try:

        print (f'State before sampling: {state} , {duration}')
        recordDER, recordNetwork = readStreamData()

        DERdata, Networkdata = binary2Dict(recordDER), binary2Dict(recordNetwork)
        records = filterRecords(Networkdata, DERdata)
        dfBefore = finalDataFrame(records, state, duration)
        sumStatesBefore = np.array(state).sum()

        state = random.sample(States, 1)
        duration = random.sample(durationTimes, 1)
        sumStatesAfter = np.array(state).sum()

        print (f'State after sampling: {state} , {duration}')
        print ('Sending Command to DER ...')

        # {Call the lambda function to send MQTT message to DER}
        
        if sumStatesAfter > sumStatesBefore:
            time.sleep(180.0 / 1.0)    # 180 Ramp Up Time (Ping)

        time.sleep(int(duration[0])/1.0)  # duration[0] Wait Time

        recordDER, recordNetwork = readStreamData()

        DERdata, Networkdata = binary2Dict(recordDER), binary2Dict(recordNetwork)
        records = filterRecords(Networkdata, DERdata)
        dfAfter = finalDataFrame(records, state , duration)


        dfStage = pd.concat([dfBefore, dfAfter], axis=0)
        fileName = 'DER_testStreamTransaction-' + dfAfter['eventTime'][0] + '.csv'
        write_df_to_csv(bucketName, prefix, fileName, dfStage)

        dfAll = pd.concat([dfAll, dfStage], axis=0)

    except Exception as e:

        print (getattr(e, 'message', repr(e)))
        continue

fileName = 'DER_testStreamWholeExperimentTransaction-' + dfAfter['eventTime'][0] + '.csv'
write_df_to_csv(bucketName, prefix, fileName, dfAll.reset_index())

stop = timeit.default_timer()
print('Running Time: ', stop - start)
