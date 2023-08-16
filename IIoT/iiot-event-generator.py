# Databricks notebook source
# MAGIC %md # Setup

# COMMAND ----------

# MAGIC %pip install azure-iot-device

# COMMAND ----------

# MAGIC %md # Async

# COMMAND ----------

# MAGIC %sh python3 sendMessagesAsync.py --type turbine --n 1000

# COMMAND ----------

# MAGIC %md # Sync

# COMMAND ----------

# MAGIC %sh python3 sendMessages.py --type weather --n 1000 --interval 1

# COMMAND ----------

# MAGIC %md # Threaded Async

# COMMAND ----------

import logging
import threading
import os

def sendMessagesAsyncThread(message_type, device, messages_to_send):
    logging.info(f'TYPE:{message_type} DEVICE:{device} N:{messages_to_send}')
    s = os.popen(f'python3 sendMessagesAsync.py --type {message_type} --device {device} --n {messages_to_send}')
    print(s.read())

# COMMAND ----------

devices = [
  {'type':'turbine', 'id':'WindTurbine-000001', 'n':10000},
  {'type':'turbine', 'id':'WindTurbine-000002', 'n':10000},
  {'type':'weather', 'id':'WeatherCapture', 'n':10000}
]

# COMMAND ----------

threads = []

for device in devices:
  print(f'Sending messages for {device["id"]}')
  t = threading.Thread(target=sendMessagesAsyncThread, args=(device['type'], device['id'], device['n']))
  threads.append(t)
  t.start()
  
for thread in threads:
  thread.join()
