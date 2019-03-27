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


STORE_FOLDER = ''

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
                    tcp_session.append(int(energy))
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
                ecache_list.append([timestamp, ecache_datetime_start, ecache_datetime_end, 'TCP', int(ecache_day[i])])
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
            row[0] = int(float(row[0]))
            row[1] = datetime.fromisoformat(row[1])
            row[2] = datetime.fromisoformat(row[2])
            row[4] = int(row[4])
            out_list.append(row)
        return out_list


def compare_sessions(loc, ext):
    sessions = []
    success = 0
    time_error = 10
    for loc_session in loc:
        session = [e for e in loc_session]
        ext_field = 'NF'
        for ext_session in ext:
            if (ext_session[0] - time_error) <= loc_session[0] <= (ext_session[0] + time_error):
                #found the session
                ext_field = ext_session[4]
                success = success + 1
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
    loc = []
    try:
        loc = import_csv('sessions_' + date + '.csv')
    except Exception as e:
        pass
    udp = datetime_shift(udp_sessions(), 7)
    tcp = datetime_shift(tcp_sessions(), 0)
    session_tcp_report, session_tcp_errors = compare_sessions(loc, tcp)
    session_udp_tcp_report, session_udp_errors = compare_sessions(session_tcp_report, udp)
    loc = []
    try:
        loc = import_csv('ecaches_' + date + '.csv')
    except Exception as e:
        pass
    timestamp = 0
    for i in range(0, len(loc)):
        if loc[i][0] - timestamp < 10:
            loc[i][4] = loc[i][4]+loc[i-1][4]
            loc[i-1][1] = 'del'
        timestamp = loc[i][0]
    loc_merged = []
    for i in range(0, len(loc)):
        if loc[i][1] != 'del':
            loc_merged.append(loc[i])
    udp = datetime_shift(udp_ecache(date+'T00:00:00Z'), 7)
    tcp = datetime_shift(tcp_ecache(), 0)
    ecache_tcp_report, ecache_tcp_errors = compare_sessions(loc_merged, tcp)
    ecache_udp_tcp_report, ecache_udp_errors = compare_sessions(ecache_tcp_report, udp)
    errors = session_tcp_errors + session_udp_errors + ecache_tcp_errors + ecache_udp_errors
    header = [['Date', 'Start time', 'End time', 'Duration', 'Local Wh','TCP Wh', 'UDP Wh']]
    for line in session_udp_tcp_report:
        line[0] = line[1].strftime("%Y-%m-%d")
        line[3] = str(line[2] - line[1])
        line[1] = line[1].strftime("%H:%M:%S")
        line[2] = line[2].strftime("%H:%M:%S")
    for line in ecache_udp_tcp_report:
        line[0] = line[1].strftime("%Y-%m-%d")
        line[3] = str(line[2] - line[1])
        line[1] = line[1].strftime("%H:%M:%S")
        line[2] = line[2].strftime("%H:%M:%S")
    udp_tcp_session_report = header + [['SESSIONS']] + session_udp_tcp_report + [['ECACHES']] + ecache_udp_tcp_report
    export_csv(udp_tcp_session_report, 'report_' + date + '.csv')
    print(errors)
    send_email('report_' + date + '.csv', date)


def send_email(filename, date):
    body = 'Sessions + Ecache report'
    receivers = [EMAIL_TO, EMAIL_CC]
    message = MIMEMultipart()
    message['From'] = EMAIL_SENDER
    message['To'] = EMAIL_TO
    message['Subject'] = 'JB validate ' + date
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


date = (datetime.now() - timedelta(days=4)).strftime("%Y-%m-%d")
#make_report('2019-03-19')
#make_report(date)

