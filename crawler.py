import os
import sys
import time
import json
import csv
import itertools
import requests
import pytz
import pprint

from datetime import datetime, timedelta
from azure.cosmosdb.table.tableservice import TableService


CONNECTION_STRING = ''
DEV_API_SECURE_URL = ''
ACCOUNT_TOKEN = ''
TOKEN = ''
DEVICE_LIST = ['']
TCP_TABLE = ''
UDP_TABLE = ''

EMAIL_CC = ''

STORE_FOLDER = ''



def tcp_sessions(device_id):    
    device_url1 = 'https://beta-v1.emotorwerks.com/v1/unit/'+device_id+'/data'
    device_url2 = 'https://beta-v1.emotorwerks.com/v2/unit/'+device_id+'/data'
    table_service = TableService(connection_string=CONNECTION_STRING)
    requests = list(table_service.query_entities(TCP_TABLE, filter="Url eq '"+device_url1+"' or Url eq '"+device_url2+"'", select='Request'))
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
                    tcp_session.append(int(energy))
                    tcp_sessions.append(tcp_session)
    return tcp_sessions


def udp_sessions(device_id):
    api_get_token = {"cmd":"get_account_units", "device_id":device_id, "account_token":ACCOUNT_TOKEN}
    req = requests.post(API_PIN_URL, json=api_get_token)
    units = req.json()['units']
    print(units)
    token = 0
    for u in units:
        if u['unit_id'] == device_id:
            token = u['token']
    print(token)
    api_history_request = {'cmd':'get_history', 'device_id': device_id, 'token': token, 'account_token': ACCOUNT_TOKEN}
    req = requests.post(DEV_API_SECURE_URL, json=api_history_request)
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


def tcp_ecache(device_id):
    device_url1 = 'https://beta-v1.emotorwerks.com/v1/unit/'+device_id+'/data'
    device_url2 = 'https://beta-v1.emotorwerks.com/v2/unit/'+device_id+'/data'
    table_service = TableService(connection_string=CONNECTION_STRING)
    tcp_requests = list(table_service.query_entities(TCP_TABLE, filter="Url eq '"+device_url1+"' or Url eq '"+device_url2+"'", select='Request'))
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
                ecache_list.append([timestamp, ecache_datetime_start, ecache_datetime_end, 'TCP', int(ecache_day[i])])
    ecache_list.sort()
    return list(ecache_list for ecache_list, _ in itertools.groupby(ecache_list)) #removes duplicates



def udp_ecache(timestamp, device_id):
    #timestamp format '2019-03-03T11:45:19Z'
    table_service = TableService(connection_string=CONNECTION_STRING)
    udp_requests = list(table_service.query_entities(UDP_TABLE, filter="Timestamp gt datetime'"+timestamp+"' and PartitionKey eq '"+device_id+"'", select='Request'))
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
                    udp_list.append([timestamp, ecache_datetime_start, ecache_datetime_end, 'UDP', int(e)])
    udp_list.sort()
    return list(udp_list for udp_list, _ in itertools.groupby(udp_list)) #removes duplicates




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
                ext_field = ext_session[4]
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

def crawl(timestamp):
    for device_id in DEVICE_LIST:
        print('DEVICE ID ', device_id)
        tcp_sessions_list = datetime_shift(tcp_sessions(device_id), 0)
        udp_sessions_list = datetime_shift(udp_sessions(device_id), 7)
        tcp_ecache_list = datetime_shift(tcp_ecache(device_id), 0)
        pprint.pprint(tcp_ecache_list)
        print(len(tcp_ecache_list))
        udp_ecache_list = datetime_shift(udp_ecache(timestamp, device_id), 7)
        sessions_report, errors = compare_sessions(udp_sessions_list, tcp_sessions_list)
        ecache_report, errors = compare_sessions(udp_ecache_list, tcp_ecache_list)
        for line in sessions_report:
            line[0] = line[1].strftime("%Y-%m-%d")
            line[3] = str(line[2] - line[1])
            line[1] = line[1].strftime("%H:%M:%S")
            line[2] = line[2].strftime("%H:%M:%S")
        for line in ecache_report:
            line[0] = line[1].strftime("%Y-%m-%d")
            line[3] = str(line[2] - line[1])
            line[1] = line[1].strftime("%H:%M:%S")
            line[2] = line[2].strftime("%H:%M:%S")
        header = ['Date', 'Start time', 'End time', 'Duration', 'UDP Wh', 'TCP Wh']
        sessions_report.insert(0, header)
        ecache_report.insert(0, header)
        export_csv(sessions_report, device_id + '_sessions.csv')
        export_csv(ecache_report, device_id + '_ecache.csv')

timestamp = '2019-03-01T00:00:00Z'
crawl(timestamp)


