import os
import sys
import serial
import time
import pytz
import json
import pickle
import requests

from texttable import Texttable
from datetime import datetime
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
    sessions = []
    if os.path.exists(STORE_FILE_ALL):
        with open(STORE_FILE_ALL, 'rb') as outfile:
            data = pickle.load(outfile)
            sessions = sessions + data
    return(sessions)


def tcp_data():
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

def tcp_ecache():
    table_service = TableService(connection_string=CONNECTION_STRING)
    requests = list(table_service.query_entities(TCP_TABLE, filter="Url eq '"+UNIT_URL+"'", select='Request'))
    eventlogs = []
    ecaches = []
    for request in requests:
        if 'ecache' in json.loads(request['Request']):
            ecaches.append(json.loads(request['Request'])['ecache'])
    tcp_ecache = []
    for ecache in ecaches:
        #assuming 15-min intervals
        ecache_day_key = list(ecache['intervals'].keys())[0]
        ecache_day = ecache['intervals'][ecache_day_key]
        #print(ecache_day_key, ecache_day)
        for i in range(0, len(ecache_day)):
            if ecache_day[i] > 0:
                ecache_datetime_start = datetime.strptime(ecache_day_key, '%Y-%m-%dZ').replace(hour=i*15//60, minute=i*15%60, second=0)
                ecache_datetime_end = ecache_datetime_start + timedelta(minutes=15)
                timestamp = datetime.timestamp(ecache_datetime_start)
                tcp_ecache.append([timestamp, ecache_datetime_start, ecache_datetime_end, 'TCP', ecache_day[i]])
    tcp_ecache.sort()
    return list(tcp_ecache for tcp_ecache,_ in itertools.groupby(tcp_ecache)) #removed duplicates

def udp_ecache(timestamp):
    table_service = TableService(connection_string=CONNECTION_STRING)
    udp_requests = list(table_service.query_entities(UDP_TABLE, filter="Timestamp gt datetime'"+timestamp+"' and PartitionKey eq '"+DEVICE_ID+"'", select='Request'))
    udp_list = []
    for udp_request in udp_requests:
        etag = datetime.strptime(udp_request['etag'][:35], 'W/"datetime\'%Y-%m-%dT%H%%3A%M%%3A%S')
        request = udp_request['Request'].split(',')
        if len(request) >=12:
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
    possible_time = datetime.now(pytz.utc).replace(hour=0,minute=0,second=0,microsecond=0)
    for t in range (0,96):
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
    ecache_timestamp = datetime.timestamp(ecache_datetime_start)
    measurement_times.append(datetime_end)
    time_chunks = []
    time_start = datetime_start
    for m in measurement_times:
        time_end = m
        chunk = time_end - time_start
        time_start = m
        time_chunks.append(chunk.seconds)
    time.sleep(1.2) #energy meter bugging
    port.write(ENERGY_REQ)
    resp = port.read(1000)
    energy_start = int.from_bytes(resp[-6:-2], byteorder=sys.byteorder) / 100.0
    session_energy_start = energy_start
    local_ecache = []
    for chunk in time_chunks:
        time.sleep(chunk)
        port.write(ENERGY_REQ)
        resp = port.read(1000)
        energy_end = int.from_bytes(resp[-6:-2], byteorder=sys.byteorder) / 100.0
        ecache_datetime_end = ecache_datetime_start + timedelta(minutes=15)
        ecache_energy_delta = energy_end - energy_start
        local_ecache.append([ecache_timestamp, ecache_datetime_start, ecache_datetime_end, 'LOC', ecache_energy_delta])
        print(energy_end)
        ecache_datetime_start = datetime.now(pytz.utc)
        ecache_timestamp = int(time.time())
    os.system('echo "0" > /sys/class/gpio/gpio15/value')
    os.system('echo "0" > /sys/class/gpio/gpio18/value')
    datetime_end = datetime.now(pytz.utc)
    energy_delta = energy_end - session_energy_start
    local_session = []
    local_session.append(session_timestamp)
    local_session.append(session_datetime_start)
    local_session.append(datetime_end)
    local_session.append('LOC')
    local_session.append(energy_delta)
    return [local_session, local_ecache]


def loop(sessions):
    while 1:
        udp = udp_data()
        tcp = tcp_data()
        show_list(udp+tcp)
        time.sleep(300)


def energy_scenario():
    ecaches=[]
    sessions = []
    e = energy_session(60)
    sessions.append(e[0])
    ecaches = ecaches + e[1]
    time.sleep(5)
    file_name = datetime.now().strftime("%Y-%m-%dZ%H:%M:%S")
    with open(STORE_FOLDER + 'pickle_' + file_name + '.pickle', 'wb') as outfile:
        pickle.dump(e, outfile)
    export_csv(sessions, 'sessions_'+file_name)
    export_csv(ecaches, 'ecaches_'+file_name)


def udp_data():
    r = requests.post(DEV_API_SECURE_URL, json = API_HISTORY_REQUEST)
    udp_sessions = r.json()['sessions']
    print(udp_sessions)
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


def show_list(input_list):
    s_table = sorted(input_list, key=lambda k: k[0])
    s_table.insert(0, ['Timestamp', 'Datetime start', 'Datetime end', 'Type', 'Energy'])
    table = Texttable()
    table.set_deco(Texttable.HEADER)
    table.set_cols_dtype(['i',  # int
                          'a',  # auto
                          'a',  
                          't',  # text
                          'a'])
    table.add_rows(s_table)
    table_draw = table.draw()
    print(table_draw)
    return table_draw

def export_csv(input_list, name):
    with open(STORE_FOLDER + name + '.csv', 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile, delimiter=',',
                                quotechar='|', quoting=csv.QUOTE_MINIMAL)
        csvwriter.writerow(['Timestamp', 'Datetime start', 'Datetime end', 'Type', 'Energy'])
        for line in input_list:
            csvwriter.writerow(line)

#sessions = init()
#print(sessions)
#energy_scenario()
#udp = udp_data()
#udp_e = udp_ecache('2019-02-18T11:45:19Z')
#tcp = tcp_data()
#tcp_e = tcp_ecache()
#show_list(udp)
#export_csv(tcp+udp)

