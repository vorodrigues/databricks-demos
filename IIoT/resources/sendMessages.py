import argparse
import asyncio
import datetime
import json
from random import random, choice
from time import sleep
from azure.iot.device.aio import IoTHubDeviceClient
from azure.iot.device import Message


def getConnStr(message_type):
    if message_type == 'turbine':
        conn_str = "HostName=vr-iot-hub.azure-devices.net;DeviceId=WindTurbine-000001;SharedAccessKey=WDmwLE1sepvrJg6hmhl/SyBWfAwdLRK4tjeq8iGv3cg="
    elif message_type == 'weather':
        conn_str = 'HostName=vr-iot-hub.azure-devices.net;DeviceId=WeatherCapture;SharedAccessKey=kpFhhxa7/G619omr+aExODYBk6dpQwXCoJ4+TpPYtwg='
    else:
        raise Exception('Message type not found')
    return conn_str


def turbineMessage():
    m = {}
    m['deviceId'] = 'WindTurbine-000001'
    m['timestamp'] = datetime.datetime.now()
    m['rpm'] = 7 * (1 + 0.6 * (-1 + 2 * random()))
    m['angle'] = 6 * (1 + 0.6 * (-1 + 2 * random()))
    return Message(json.dumps(m, default=str))
    

def weatherMessage():
    m = {}
    m['deviceId'] = 'WeatherCapture'
    m['timestamp'] = datetime.datetime.now()
    m['temperature'] = 27 * (1 + 0.6 * (-1 + 2 * random()))
    m['humidity'] = 64 * (1 + 0.6 * (-1 + 2 * random()))
    m['windspeed'] = 6 * (1 + 0.6 * (-1 + 2 * random()))
    m['winddirection'] = choice(['N','NW','W','SW','S','SE','E','NE'])
    return Message(json.dumps(m, default=str))


def getMessageTemplate(message_type):
    if message_type == 'turbine':
        template = turbineMessage
    elif message_type == 'weather':
        template = weatherMessage
    else:
        raise Exception('Message type not found')
    return template


async def sendRecurringMessages(device_client):
    # Connect the client.
    await device_client.connect()
    
    start = datetime.datetime.now()
    
    total = 0
    for i in range(messages_to_send):
        msg = template()
        msg.content_encoding = "utf-8"
        msg.content_type = "application/json"
        await device_client.send_message(msg)
        total = total + 1
        if i % 100 == 0:
            timestamp = str(datetime.datetime.now())
            print(f'{timestamp}: Messages sent: {total}')
        sleep(interval/1000)
    end = datetime.datetime.now()
    duration = (end - start).total_seconds()
    rate = total / duration
    print('===============================================')
    print(f'Start time: {start}')
    print(f'End time: {end}')
    print(f'Duration: {duration}s')
    print(f'Total messages sent: {total}')
    print(f'Rate: {rate} msgs/sec')
    print('===============================================')


def sendMessages():

    # The client object is used to interact with your Azure IoT hub.
    device_client = IoTHubDeviceClient.create_from_connection_string(conn_str)
   
    loop = asyncio.get_event_loop()
    try:
        print('Sending messages...')
        loop.run_until_complete(sendRecurringMessages(device_client))
    except KeyboardInterrupt:
        print("User initiated exit")
    except Exception:
        print("Unexpected exception!")
        raise
    finally:
        loop.run_until_complete(device_client.shutdown())
        loop.close()

    # send `messages_to_send` messages in parallel
    #await asyncio.gather(*[send_test_message(i) for i in range(1, messages_to_send + 1)])

    # Finally, shut down the client
    #await device_client.shutdown()


if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--type", type=str)
    parser.add_argument("--n", type=int)
    parser.add_argument("--interval", type=int)
    args = parser.parse_args()

    message_type = args.type
    messages_to_send = args.n
    interval = args.interval
    
    print(f'Message Type: {message_type}')
    print(f'Message Count: {messages_to_send}')
    print(f'Interval: {interval}ms')
    
    conn_str = getConnStr(message_type)
    template = getMessageTemplate(message_type)
    
    # asyncio.run(sendMessages())
    sendMessages()