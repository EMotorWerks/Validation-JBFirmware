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


def tcp_sessions():
    table_service = TableService(connection_string=CONNECTION_STRING)
    requests = list(table_service.query_entities(TCP_TABLE, filter="Url eq '"+UNIT_URL+"'", select='Request'))
    eventlogs = []
    for request in requests:
        if 'eventlog' in json.loads(request['Request']):
            eventlogs.append(json.loads(request['Request'])['eventlog'])
    tcp_sessions = []
    for eventlog in eventlogs:
        event_day_key = list(eventlog.keys())[0]
        for event_time_key in list(eventlog[event_day_key].keys()):
            event_datetime = datetime.strptime(event_day_key + event_time_key, '%Y-%m-%dZ%H:%M:%S')
            event_datetime = pytz.utc.localize(event_datetime)
            messages = eventlog[event_day_key][event_time_key]
            #messages should be in time order
            for message in messages:
                if message['code'] == '066': #start session
                    time_start = event_datetime
                    timestamp = int(event_datetime.timestamp())
                if message['code'] == '067': #end session
                    time_end = event_datetime
                    energy = message['energy']
                    tcp_session = []
                    tcp_session.append(timestamp)
                    tcp_session.append(time_start)
                    tcp_session.append(time_end)
                    tcp_session.append('TCP')
                    tcp_session.append(energy)
                    tcp_sessions.append(tcp_session)
    return tcp_sessions


def udp_sessions():
    req = requests.post(DEV_API_SECURE_URL, json=API_HISTORY_REQUEST)
    udp_sessions = req.json()['sessions']
    udp_list = []
    for udp_session in udp_sessions:
        udp_item = []
        udp_item.append(int(udp_session['time_start']))
        udp_item.append(datetime.fromtimestamp(int(udp_session['time_start']), pytz.utc))
        udp_item.append(datetime.fromtimestamp(int(udp_session['time_end']), pytz.utc))
        udp_item.append('UDP')
        udp_item.append(udp_session['wh_energy'])
        udp_list.append(udp_item)
    return udp_list


def tcp_ecache():
    table_service = TableService(connection_string=CONNECTION_STRING)
    tcp_requests = list(table_service.query_entities(TCP_TABLE, filter="Url eq '"+UNIT_URL+"'", select='Request'))
    ecaches = []
    for request in tcp_requests:
        if 'ecache' in json.loads(request['Request']):
            ecaches.append(json.loads(request['Request'])['ecache'])
    ecache_list = []
    for ecache in ecaches:
        #assuming 15-min intervals
        ecache_day_key = list(ecache['intervals'].keys())[0]
        ecache_day = ecache['intervals'][ecache_day_key]
        for i in range(0, len(ecache_day)):
            if ecache_day[i] > 0:
                ecache_datetime_start = datetime.strptime(ecache_day_key, '%Y-%m-%dZ').replace(hour=i*15//60, minute=i*15%60, second=0)
                ecache_datetime_end = ecache_datetime_start + timedelta(minutes=15)
                timestamp = datetime.timestamp(ecache_datetime_start)
                ecache_list.append([timestamp, ecache_datetime_start, ecache_datetime_end, 'TCP', ecache_day[i]])
    ecache_list.sort()
    return list(ecache_list for ecache_list, _ in itertools.groupby(ecache_list)) #removes duplicates


def udp_ecache(timestamp):
    #timestamp format '2019-03-03T11:45:19Z'
    table_service = TableService(connection_string=CONNECTION_STRING)
    udp_requests = list(table_service.query_entities(UDP_TABLE, filter="Timestamp gt datetime'"+timestamp+"' and PartitionKey eq '"+DEVICE_ID+"'", select='Request'))
    udp_list = []
    for udp_request in udp_requests:
        etag = datetime.strptime(udp_request['etag'][:35], 'W/"datetime\'%Y-%m-%dT%H%%3A%M%%3A%S')
        request = udp_request['Request'].split(',')
        if len(request) >= 12:
            e = request[11]
            if request[11][0] == 'e':
                e = request[11][1:]
                if int(e) > 0:
                    i = int(request[10][1:])
                    ecache_datetime_start = etag.replace(hour=i*15//60, minute=i*15%60, second=0)
                    if ecache_datetime_start > etag: # if it shows previous day
                        ecache_datetime_start = ecache_datetime_start - timedelta(days=1)
                    ecache_datetime_end = ecache_datetime_start + timedelta(minutes=15)
                    timestamp = datetime.timestamp(ecache_datetime_start)
                    udp_list.append([timestamp, ecache_datetime_start, ecache_datetime_end, 'UDP', e])
    return udp_list


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
    local_session.append(energy_delta)
    date = datetime.now().strftime("%Y-%m-%d")
    export_csv([local_session], 'sessions_' + date + '.csv')
    export_csv(local_ecache, 'ecaches_' + date + '.csv')
    return [[local_session], local_ecache]


def export_csv(input_list, name):
    with open(STORE_FOLDER + name, 'a', newline='') as csvfile:
        csvwriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
        for line in input_list:
            csvwriter.writerow(line)


def import_csv(name):
    with open(STORE_FOLDER + name, 'r', newline='') as csvfile:
        csvreader = csv.reader(csvfile, delimiter=',', quotechar='|')
        out_list = []
        for row in csvreader:
            row[0] = int(row[0])
            row[1] = datetime.fromisoformat(row[1])
            row[2] = datetime.fromisoformat(row[2])
            row[4] = int(row[4])
            out_list.append(row)
        return out_list


def compare_sessions(loc, ext):
    sessions = []
    success = 0
    allow_error = 10
    for loc_session in loc:
        session = [e for e in loc_session]
        ext_field = 'NF'
        for ext_session in ext:
            if (ext_session[0] - 2) <= loc_session[0] <= (ext_session[0] + 2):
                #found the session
                energy_diff = int(loc_session[4]) - int(ext_session[4])
                if energy_diff > allow_error or energy_diff < -allow_error:
                    ext_field = energy_diff
                else:
                    ext_field = 'OK'
                    success += 1
                    break
        session.append(ext_field)
        sessions.append(session)
    errors = len(sessions) - success
    return sessions, errors


def datetime_shift(data, hour_diff):
    seconds = hour_diff*60*60
    for session in data:
        session[0] += seconds
        session[1] += timedelta(seconds=seconds)
        session[2] += timedelta(seconds=seconds)
    return data


def make_report(date):
    loc = import_csv('sessions_' + date + '.csv')
    udp = datetime_shift(udp_sessions(), 7)
    tcp = datetime_shift(tcp_sessions(), 24)
    session_tcp_report, session_tcp_errors = compare_sessions(loc, tcp)
    session_udp_tcp_report, session_udp_errors = compare_sessions(session_tcp_report, udp)
    errors = session_tcp_errors + session_udp_errors
    loc = import_csv('ecaches_' + date + '.csv')
    udp = datetime_shift(udp_ecache(date+'T00:00:00Z'), 7)
    tcp = datetime_shift(tcp_ecache(), 24)
    ecache_tcp_report, ecache_tcp_errors = compare_sessions(loc, tcp)
    ecache_udp_tcp_report, ecache_udp_errors = compare_sessions(ecache_tcp_report, udp)
    errors = session_tcp_errors + session_udp_errors + ecache_tcp_errors
    header = [['Timestamp', 'Start', 'End', 'Tag', 'Energy','TCP STAT', 'UDP STAT']]
    udp_tcp_session_report = header + [['SESSIONS']] + session_udp_tcp_report + [['ECACHES']] + ecache_udp_tcp_report
    export_csv(udp_tcp_session_report, 'session_report_' + date + '.csv')
    send_email('session_report_' + date + '.csv', errors)


def send_email(filename, errors):
    body = 'Sessions + Ecache report'
    receivers = [EMAIL_TO, EMAIL_CC]
    message = MIMEMultipart()
    message['From'] = EMAIL_SENDER
    message['To'] = EMAIL_TO
    message['Subject'] = 'JB validate, errors: ' + str(errors)
    message.attach(MIMEText(body, 'plain'))
    with open(STORE_FOLDER + filename, 'rb') as attachment:
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(attachment.read())
    encoders.encode_base64(part)
    part.add_header("Content-Disposition", 'attachment', filename='report.csv')
    message.attach(part)
    text = message.as_string()
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=context) as server:
        server.login(EMAIL_SENDER, EMAIL_PASS)
        server.sendmail(EMAIL_SENDER, receivers, text)


def planned_scenario():
    init()
    energy_session(330)
    time.sleep(60)
    energy_session(330) 


def random_scenario():
    init()
    while 1:
        use_time = random.randint(1, 11000)  #from 1sec to 3h
        print('use_time ', use_time)
        energy_session(use_time)
        idle_time = random.randint(1, 11000)
        print('idle_time', idle_time)
        time.sleep(idle_time)



random_scenario()
#make_report('2019-03-13')