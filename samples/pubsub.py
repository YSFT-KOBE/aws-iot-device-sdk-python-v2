# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0.

from awscrt import mqtt
import sys
import threading
import time
from uuid import uuid4
import json
import asyncio
import time
import RPi.GPIO as GPIO
import cv2
import threading
import boto3

# This sample uses the Message Broker for AWS IoT to send and receive messages
# through an MQTT connection. On startup, the device connects to the server,
# subscribes to a topic, and begins publishing messages to that topic.
# The device should receive those same messages back from the message broker,
# since it is subscribed to that same topic.

# Parse arguments
import command_line_utils;
cmdUtils = command_line_utils.CommandLineUtils("PubSub - Send and recieve messages through an MQTT connection.")
cmdUtils.add_common_mqtt_commands()
cmdUtils.add_common_topic_message_commands()
cmdUtils.add_common_proxy_commands()
cmdUtils.add_common_logging_commands()
cmdUtils.register_command("key", "<path>", "Path to your key in PEM format.", True, str)
cmdUtils.register_command("cert", "<path>", "Path to your client certificate in PEM format.", True, str)
cmdUtils.register_command("port", "<int>", "Connection port. AWS IoT supports 443 and 8883 (optional, default=auto).", type=int)
cmdUtils.register_command("client_id", "<str>", "Client ID to use for MQTT connection (optional, default='test-*').", default="test-" + str(uuid4()))
cmdUtils.register_command("count", "<int>", "The number of messages to send (optional, default='10').", default=10, type=int)
# Needs to be called so the command utils parse the commands
cmdUtils.get_args()

chime_message_topic = 'chime/message'
image_request_topic = 'image/request'
image_release_topic = 'image/release'

GPIO.setmode(GPIO.BCM)

GPIO.setup(14, GPIO.IN, pull_up_down=GPIO.PUD_UP)
GPIO.setup(15, GPIO.OUT)
GPIO.output(15, GPIO.LOW)

queue = asyncio.Queue(maxsize=2)
queue_message_chime = {"chime": ""}
queue_message_image = {"image": ""}

threadEvent = threading.Event()

s3 =boto3.resource('s3')
bucket_name = "ohenro-iot-bucket-2022"


# Callback when connection is accidentally lost.
def on_connection_interrupted(connection, error, **kwargs):
    print("Connection interrupted. error: {}".format(error))


# Callback when an interrupted connection is re-established.
def on_connection_resumed(connection, return_code, session_present, **kwargs):
    print("Connection resumed. return_code: {} session_present: {}".format(return_code, session_present))

    if return_code == mqtt.ConnectReturnCode.ACCEPTED and not session_present:
        print("Session did not persist. Resubscribing to existing topics...")
        resubscribe_future, _ = connection.resubscribe_existing_topics()

        # Cannot synchronously wait for resubscribe result because we're on the connection's event-loop thread,
        # evaluate result with a callback instead.
        resubscribe_future.add_done_callback(on_resubscribe_complete)


def on_resubscribe_complete(resubscribe_future):
        resubscribe_results = resubscribe_future.result()
        print("Resubscribe results: {}".format(resubscribe_results))

        for topic, qos in resubscribe_results['topics']:
            if qos is None:
                sys.exit("Server rejected resubscribe to topic: {}".format(topic))

# Callback when the subscribed topic receives a message
def on_message_received(topic, payload, dup, qos, retain, **kwargs):
    print("Received message from topic '{}': {}".format(topic, payload))
    if topic == image_request_topic :
        print("image create Req")
        threadEvent.set()

#
async def task_aws_iotcore_main_proc():
    mqtt_connection = cmdUtils.build_mqtt_connection(on_connection_interrupted, on_connection_resumed)
    print("Connecting to {} with client ID '{}'...".format(
        cmdUtils.get_command(cmdUtils.m_cmd_endpoint), cmdUtils.get_command("client_id")))

    connect_future = mqtt_connection.connect()

    # Future.result() waits until a result is available
    connect_future.result()
    print("Connected!")


    # Subscribe
    print("Subscribing to topic '{}'...".format(image_request_topic))
    subscribe_future, packet_id = mqtt_connection.subscribe(
        topic=image_request_topic,
        qos=mqtt.QoS.AT_LEAST_ONCE,
        callback=on_message_received)

    subscribe_result = subscribe_future.result()
    print("Subscribed with {}".format(str(subscribe_result['qos'])))

    # Publish message to server desired number of times.
    # This step is skipped if message is blank.
    # This step loops forever if count was set to 0.
    while (True):
        if queue.qsize():
            print("queue.get()")
            qdata = await queue.get()
            print(qdata)
        else:
            await asyncio.sleep(0)
            continue
        qdatajson = json.loads(qdata)
        if 'chime' in qdatajson:
            message = qdatajson
            print("Publishing message to topic '{}': {}".format(chime_message_topic, message))
            message_json = json.dumps(message)
            print(message_json)
            mqtt_connection.publish(
                topic=chime_message_topic,
                payload=message_json,
                qos=mqtt.QoS.AT_LEAST_ONCE)
        elif 'image' in qdatajson:
            message = {'image_url': ''}
            message['image_url'] = qdatajson['image']
            print("Publishing message to topic '{}': {}".format(image_release_topic, message))
            message_json = json.dumps(message)
            mqtt_connection.publish(
                topic=image_release_topic,
                payload=message_json,
                qos=mqtt.QoS.AT_LEAST_ONCE)
            
        #time.sleep(1)
        #await asyncio.sleep(1)

    # Disconnect
    print("Disconnecting...")
    disconnect_future = mqtt_connection.disconnect()
    disconnect_future.result()
    print("Disconnected!")

async def task_io_main_proc():
    print("task_main Started")
    preStatus = 2

    try:
        while(True):
            pinstate = GPIO.input(14)

            if pinstate == 1: # Chime ON
                if preStatus != 1:
                    preStatus = 1
                    print("Chime ON")
                    queue_message_chime['chime'] = 'ON'
                    qdata = json.dumps(queue_message_chime)
                    await queue.put(qdata);
                    GPIO.output(15, GPIO.HIGH)
            else: # Chime OFF
                if preStatus != 0:
                    preStatus = 0
                    print("Chime OFF")
                    queue_message_chime['chime'] = 'OFF'
                    qdata = json.dumps(queue_message_chime)
                    await queue.put(qdata);
                    GPIO.output(15, GPIO.LOW)
            await asyncio.sleep(0)
    finally:
        GPIO.output(15, GPIO.LOW)

async def task_image_create_proc():
    print("task_image_create_proc START")

    user_id = "6199"
    user_pw = "4003"
    host = "10.108.34.17"

    try:
        while(True):
            if threadEvent.wait(0) == True :
                threadEvent.clear()
                print("Image Create Req Received!")
                cap = cv2.VideoCapture(f"rtsp://{user_id}:{user_pw}@{host}/live")
                ret, frame = cap.read()
                filename = ""
                if ret == True:
                    filename = "image.jpg"
                    print(filename)
                    # cv2.imshow('VIDEO', frame)
                    cv2.imwrite('./'+filename, frame)
                cv2.waitKey(1)
                cap.release()
                cv2.destroyAllWindows()
                s3.Bucket(bucket_name).upload_file('./'+filename, filename)
                print("Image Uploaded!")
                queue_message_image['image'] = "s3://"+bucket_name+"/"+filename
                qdata = json.dumps(queue_message_image)
                await queue.put(qdata);
            else:
                await asyncio.sleep(0)
    except Exception as e:
        print("Exception Occured: {}".format(e))
    finally:
        cap.release()
        cv2.destroyAllWindows()

async def create_task():
    task1 = asyncio.create_task(task_io_main_proc())
    task2 = asyncio.create_task(task_aws_iotcore_main_proc())
    task3 = asyncio.create_task(task_image_create_proc())
    await task1
    await task2
    await task3

if __name__ == '__main__':

    asyncio.run(create_task())

