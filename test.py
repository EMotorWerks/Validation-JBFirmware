import os
import sys
import time
import json
import csv
import itertools
import random
import requests
import pytz
import serial

import email, smtplib, ssl
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from datetime import datetime, timedelta
from azure.cosmosdb.table.tableservice import TableService


SERIAL_PORT = '/dev/ttyUSB0'
SERIAL_SPEED = 9600
ENERGY_REQ = b'\xC0\x48\xFF\xFF\xFD\x00\x00\x00\x00\x00\xD2\x01\x30\x00\x01\x1C\xC0'
STORE_FOLDER = ''


STORE_FILE_ALL = STORE_FOLDER + 'all.pickle'


#to be extracted from a config file - connection data deleted
CONNECTION_STRING = ''
UNIT_URL = ''
DEV_API_SECURE_URL = ''
ACCOUNT_TOKEN = ''
TOKEN = ''
DEVICE_ID = ''
API_HISTORY_REQUEST = {'cmd':'get_history','device_id': DEVICE_ID,'token': TOKEN,'account_token': ACCOUNT_TOKEN}
TCP_TABLE = ''
UDP_TABLE = ''

EMAIL_CC = ''
EMAIL_TO = ''
EMAIL_SENDER = ''
EMAIL_PASS = ''

#Data storage - list constsing of:
#timestamp (seconds from epoch, also used for sorting by time)
#datetime session start
#datetime session stop
#source name (UDP, TCP, LOCAL)  
#energy in session


def init():
    #to be replaced by py module
    os.system('echo "15" > /sys/class/gpio/export')
    os.system('echo "18" > /sys/class/gpio/export')
    time.sleep(0.2)
    os.system('echo "out" > /sys/class/gpio/gpio15/direction')
    os.system('echo "out" > /sys/class/gpio/gpio18/direction')
    time.sleep(0.2)


def energy_session(duration):
    port = serial.Serial(SERIAL_PORT, SERIAL_SPEED)
    port.timeout = 1
    os.system('echo "1" > /sys/class/gpio/gpio15/value')
    os.system('echo "1" > /sys/class/gpio/gpio18/value')
    session_timestamp = int(time.time())
    session_datetime_start = datetime.now(pytz.utc)
    datetime_start = session_datetime_start
    datetime_end = datetime_start + timedelta(seconds=duration)
    possible_measurements = []
    possible_time = datetime.now(pytz.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    for _ in range(0, 192): #2 days
        possible_measurements.append(possible_time)
        possible_time = possible_time + timedelta(minutes=15)
    measurement_times = []
    for m in possible_measurements:
        if m < datetime_start:
            ecache_datetime_start = m
        if datetime_start < m < datetime_end:
            measurement_times.append(m)
    if len(measurement_times) > 0:
        ecache_datetime_start = measurement_times[0] - timedelta(minutes=15)
    ecache_timestamp = int(datetime.timestamp(ecache_datetime_start))
    measurement_times.append(datetime_end)
    time_chunks = []
    time_start = datetime_start
    for m in measurement_times:
        time_end = m
        chunk = time_end - time_start
        time_start = m
        time_chunks.append(chunk.seconds)
    time.sleep(1.5) #energy meter bugging
    port.write(ENERGY_REQ)
    resp = port.read(1000)
    energy_start = int.from_bytes(resp[-6:-2], byteorder=sys.byteorder)
    print('START ENERGY ', energy_start)
    session_energy_start = energy_start
    local_ecache = []
    for chunk in time_chunks:
        time.sleep(chunk)
        port.write(ENERGY_REQ)
        resp = port.read(1000)
        energy_end = int.from_bytes(resp[-6:-2], byteorder=sys.byteorder)
        ecache_datetime_end = ecache_datetime_start + timedelta(minutes=15)
        ecache_energy_delta = energy_end - energy_start
        print('ECACHE ENERGY ', ecache_energy_delta)
        local_ecache.append([ecache_timestamp, ecache_datetime_start, ecache_datetime_end, 'LOC', ecache_energy_delta])
        energy_start = energy_end
        ecache_datetime_start = datetime.now(pytz.utc)
        ecache_timestamp = int(time.time())
    os.system('echo "0" > /sys/class/gpio/gpio15/value')
    os.system('echo "0" > /sys/class/gpio/gpio18/value')
    datetime_end = datetime.now(pytz.utc)
    energy_delta = energy_end - session_energy_start
    print('ENERGY SESSION END ', energy_end)
    print('ENERGY SESSION ', energy_delta)
    local_session = []
    local_session.append(session_timestamp)
    local_session.append(session_datetime_start)
    local_session.append(datetime_end)
    local_session.append('LOC')
    local_session.append(energy_delta*10)
    date = datetime.now().strftime("%Y-%m-%d")
    export_csv([local_session], 'sessions_' + date + '.csv')
    export_csv(local_ecache, 'ecaches_' + date + '.csv')
    return [[local_session], local_ecache]


def export_csv(input_list, name):
    with open(STORE_FOLDER + name, 'a', newline='') as csvfile:
        csvwriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
        for line in input_list:
            csvwriter.writerow(line)


def planned_scenario():
    init()
    energy_session(2000)


def random_scenario():
    init()
    while 1:
        use_time = random.randint(1, 11000)  #from 1sec to 3h
        print('use_time ', use_time)
        energy_session(use_time)
        idle_time = random.randint(1, 11000)
        print('idle_time', idle_time)
        time.sleep(idle_time)


planned_scenario()