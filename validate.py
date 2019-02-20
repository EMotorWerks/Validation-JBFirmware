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
STORE_FOLDER = '/home/pi/jb_test_rig/'
#STORE_FOLDER = '/Users/stepanmoiseev/Code/jb_test_rig/'

STORE_FILE_ALL = STORE_FOLDER + 'all.pickle'


#to be extracted from a config file - connection data deleted
CONNECTION_STRING = ''
UNIT_URL = ''
DEV_API_SECURE_URL = ''
ACCOUNT_TOKEN = ''
TOKEN = ''
DEVICE_ID = ''
API_HISTORY_REQUEST = {'cmd':'get_history','device_id': DEVICE_ID,'token': TOKEN,'account_token': ACCOUNT_TOKEN}


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
    requests = list(table_service.query_entities('XCelApiCalls', filter="Url eq '"+UNIT_URL+"'", select='Request'))
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
            #tbd - check if we have a start session
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


def energy_session(duration):
    port = serial.Serial(SERIAL_PORT, SERIAL_SPEED)
    port.timeout = 1
    os.system('echo "1" > /sys/class/gpio/gpio15/value')
    os.system('echo "1" > /sys/class/gpio/gpio18/value')
    timestamp = int(time.time())
    datetime_start = datetime.now(pytz.utc)
    time.sleep(1.2) #energy meter bugging
    port.write(ENERGY_REQ)
    resp = port.read(1000)
    energy_initial = int.from_bytes(resp[-6:-2], byteorder=sys.byteorder) / 100.0
    print(energy_initial)
    time.sleep(duration)
    port.write(ENERGY_REQ)
    resp = port.read(1000)
    energy_end = int.from_bytes(resp[-6:-2], byteorder=sys.byteorder) / 100.0
    print(energy_end)
    os.system('echo "0" > /sys/class/gpio/gpio15/value')
    os.system('echo "0" > /sys/class/gpio/gpio18/value')
    datetime_end = datetime.now(pytz.utc)
    energy_delta = energy_end - energy_initial
    local_data = []
    local_data.append(timestamp)
    local_data.append(datetime_start)
    local_data.append(datetime_end)
    local_data.append('LOC')
    local_data.append(energy_delta)
    return local_data


def energy_scenario():
    sessions.append(energy_session(60))
    time.sleep(30)
    sessions.append(energy_session(300))
    file_name = datetime.now().strftime("%Y-%m-%dZ%H:%M:%S")
    with open(STORE_FILE_ALL, 'wb') as outfile:
        pickle.dump(sessions, outfile)
    with open(STORE_FOLDER + file_name + '.pickle', 'wb') as outfile:
        pickle.dump(sessions, outfile)
    with open(STORE_FOLDER + file_name + '.log', 'wb') as outfile:
        pickle.dump(show_list(sessions), outfile)


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


#sessions = init()
#print(sessions)
#energy_scenario()
#udp = udp_data()
tcp = tcp_data()
show_list(tcp)
