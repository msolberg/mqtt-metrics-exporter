# Copyright Michael Solberg <mpsolberg@gmail.com>
# Based on AWS IOT SDK samples:
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0.

from awscrt import mqtt, http
from awsiot import mqtt_connection_builder
from prometheus_client import start_http_server, Gauge
import sys
import threading
import time
import json
import configparser

config = configparser.ConfigParser()
config.read('mqtt-metrics-exporter.ini')

listen_port = int(config['DEFAULT']['port'])
endpoint=config['broker']['endpoint']
cert_filepath=config['broker']['cert_filepath']
pri_key_filepath=config['broker']['pri_key_filepath']
ca_filepath=config['broker']['ca_filepath']
clientId=config['broker']['clientId']
message_topic=config['broker']['message_topic']

received_all_event = threading.Event()

gauges = {}

def update_gauge(topic, k, v):
    # Update an existing guage or create a new one with the value.
    global gauges
    #print("topic name: %s", (topic))
    
    device = topic.split('/')[-1]
    #print("device name: %s", (device))
    gauge_name = device + "_" + k
    
    try:
        vf = float(v)
        try:
            #print("Setting existing gauge")
            gauges[gauge_name].set(vf)
        except KeyError:
            #print("Making a new gauge")
            gauges[gauge_name] = Gauge(gauge_name, k)
            gauges[gauge_name].set(vf)
            print(gauges)
    except ValueError:
        return

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
    
    try:
        data = json.loads(payload)
        #print("Got payload %s"% (payload,))
        for k in data.keys():
            update_gauge(topic, k, data[k])
    except json.decoder.JSONDecodeError:
        print("Received malformed message %s"% (payload))
        return

# Callback when the connection successfully connects
def on_connection_success(connection, callback_data):
    assert isinstance(callback_data, mqtt.OnConnectionSuccessData)
    print("Connection Successful with return code: {} session present: {}".format(callback_data.return_code, callback_data.session_present))

# Callback when a connection attempt fails
def on_connection_failure(connection, callback_data):
    assert isinstance(callback_data, mqtt.OnConnectionFailureData)
    print("Connection failed with error code: {}".format(callback_data.error))

# Callback when a connection has been disconnected or shutdown successfully
def on_connection_closed(connection, callback_data):
    print("Connection closed")

if __name__ == '__main__':
    # Start the status page
    start_http_server(listen_port)

    # Create a MQTT connection from the command line data
    mqtt_connection = mqtt_connection_builder.mtls_from_path(
        endpoint=endpoint,
        cert_filepath=cert_filepath,
        pri_key_filepath=pri_key_filepath,
        ca_filepath=ca_filepath,
        on_connection_interrupted=on_connection_interrupted,
        on_connection_resumed=on_connection_resumed,
        client_id=clientId,
        clean_session=False,
        keep_alive_secs=30,
        on_connection_success=on_connection_success,
        on_connection_failure=on_connection_failure,
        on_connection_closed=on_connection_closed)

    connect_future = mqtt_connection.connect()

    # Future.result() waits until a result is available
    connect_future.result()
    print("Connected!")

    # Subscribe
    print("Subscribing to topic '{}'...".format(message_topic))
    subscribe_future, packet_id = mqtt_connection.subscribe(
        topic=message_topic,
        qos=mqtt.QoS.AT_LEAST_ONCE,
        callback=on_message_received)

    subscribe_result = subscribe_future.result()
    print("Subscribed with {}".format(str(subscribe_result['qos'])))

    received_all_event.wait()

    # Disconnect
    print("Disconnecting...")
    disconnect_future = mqtt_connection.disconnect()
    disconnect_future.result()
    print("Disconnected!")
