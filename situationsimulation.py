#!/usr/bin/env python
# -*- coding: utf-8 -*-
import csv
from datetime import datetime, timedelta
import os
import requests
import sys
import pandas
import logging
import math
import statistics
import time

csv.field_size_limit(10000000)
debug = True
write = True
classical = True
logging.basicConfig(level=logging.DEBUG)

data_source_rootfolder = "./"
data_source_istmeldungen = "20170901-20171019_Alle_Istmeldungen_S-Bahn_Stuttgart.csv"
url_source_istmeldungen = "http://download-data.deutschebahn.com/static/datasets/sbahn_stuttgart/20170901-20171019_Alle_Istmeldungen_S-Bahn_Stuttgart.csv"

train_ids = set()
trains = set()
invalid_trains = set()

class Train:

    def __init__(self, train_id, rows, situation):
        self.train_id = train_id
        self.rows = rows
        self.situation = situation
        self.orderedRows = dict()

        self.avg_stop_time = 0
        self.wst_case_stop_time = 0
        self.bst_case_stop_time = 0
        self.deviation_stop_time = 0

        self.bst_case_delay_time = 0
        self.wst_case_delay_time = 0
        self.avg_delay_time = 0
        self.deviation_delay_time = 0

        self.stop_times = list()
        self.stops = list()
        self.delays = list()
        self.valid = True
        self.order_rows()
        self.calc_stop_times()
        self.calc_average_delays()

        logging.debug('Found stop times for train ' + str(self.train_id))
        logging.debug('Average stop time: ' + str(self.avg_stop_time))
        logging.debug('Best stop time: ' + str(self.bst_case_stop_time))
        logging.debug('Worst stop time: ' + str(self.wst_case_stop_time))
        logging.debug('Average delay time: ' + str(self.avg_delay_time))

    def get_stop_times(self):
        return self.stop_times

    def get_delay_times(self):
        return self.delays

    def getbestcasestoptime(self):
        return self.bst_case_stop_time

    def getworstcasestoptime(self):
        return self.wst_case_stop_time

    def getavgcasestoptime(self):
        return self.avg_stop_time

    def get_avg_delay_time(self):
        return self.avg_delay_time

    def getTrainId(self):
        return self.train_id

    def getRows(self):
        return self.rows

    def isValid(self):

        for r in self.rows:
            if r[1] != self.train_id:
                return False

        for stop in self.stops:
            foundStation = None
            foundStopAtStationRow = False
            foundStartAtStationRow = False
            for r in stop.values() :
                if foundStation == None:
                    foundStation = r[2]
                elif foundStation != r[2]:
                    return False
                if r[3] == '10' or r[3] == '30' or r[3] == '40':
                    foundStartAtStationRow = True
                elif r[3] == '20':
                    foundStopAtStationRow = True

            if (not foundStopAtStationRow) or (not foundStartAtStationRow):
                return False

        return self.valid

    def execute(self):
        self.situation.createSituation()

        for key in sorted(self.sortedTime.keys()):
            movement = None;
            eventtype = self.orderedRows[key][3]
            if eventtype == '10' or eventtype == '30' or eventtype == '40':
                movement = True
            else:
                movement = False
            self.situation.setActive(movement)

    def toDateTime(self, str):
        try:
            return datetime.strptime(str, '%d.%m.%Y %H:%M:%S')
        except Exception as exp:
            logging.debug(exp)
            return None

    def order_rows(self):
        for r in self.rows:
            try:
                eventtime = r[5]
                dateTime = datetime.strptime(eventtime, '%d.%m.%Y %H:%M:%S')
                self.orderedRows[dateTime] = r
            except Exception as exp:
                logging.debug(exp)
                self.valid = False
                logging.debug(r)
                pass

    def haveSameDate(self, datetimes):
        year = datetimes[0].year
        month = datetimes[0].month
        day = datetimes[0].day

        for datetime in datetimes:
            if datetime.year != year:
                return False
            if datetime.month != month:
                return False
            if datetime.day != day:
                return False

        return True

    def calc_average_delays(self):
        orderedTimeKeys = sorted(self.orderedRows.keys())
        delays = list()

        for key in orderedTimeKeys:
            row = self.orderedRows[key]
            if row[3] != '20':
                continue
            planned_time = self.toDateTime(row[4])
            actual_time = self.toDateTime(row[5])
            if planned_time is None or actual_time is None:
                self.valid = False
                continue
            delay_time = (planned_time - actual_time).total_seconds()

            if delay_time >= 0:
                delays.append(delay_time)

        if len(delays) != 0:
            self.delays = delays
            self.avg_delay_time = sum(delays) / len(delays)

            squared_delays = list()

            for delay in delays:
                squared_delays.append((delay - self.avg_delay_time)**2)

            self.deviation_delay_time = math.sqrt(sum(squared_delays) / len(squared_delays))



    def calc_stop_times(self):
        orderedTimeKeys = sorted(self.orderedRows.keys())

        stop_durations = list()
        current_stop_duration = dict()
        stop_station = None
        foundStoppingRow = False
        for key in orderedTimeKeys:
            row = self.orderedRows[key]

            if row[3] == '20' and foundStoppingRow:
                current_stop_duration = dict()
                stop_station = None
                foundStoppingRow = False
                continue

            if row[3] == '20' and len(current_stop_duration.keys()) == 0:
                current_stop_duration[key] = row
                stop_station = row[2]
                foundStoppingRow = True
            if row[3] == '40' and len(current_stop_duration.keys()) == 1 and row[2] == stop_station:
                current_stop_duration[key] = row
                stop_durations.append(current_stop_duration)
                current_stop_duration = dict()
                stop_station = None
                foundStoppingRow = False

        durations = list()

        for stop_duration in stop_durations:
            duration_times = stop_duration.keys()
            if not self.haveSameDate(list(duration_times)):
                continue
            start_time = min(duration_times)
            end_time = max(duration_times)
            duration = (end_time - start_time).total_seconds()
            #if duration > 120 and debug:
            #    print('More than 2 minutes two wait at this stop: ')
            #    print(stop_duration[start_time])
            #    print(stop_duration[end_time])
            #    continue
            #print('Found duration: ' + str(duration))
            #print(stop_duration)
            durations.append(duration)

        if len(durations) == 0:
            self.valid = False
            return


        avg = sum(durations) / len(durations)

        self.stops = stop_durations
        self.stop_times = durations
        self.avg_stop_time = avg
        self.wst_case_stop_time = max(durations)
        self.bst_case_stop_time = min(durations)

        squared_stops = list()

        for duration in durations:
            squared_stops.append((duration  - self.avg_stop_time)**2)


        self.deviation_stop_time = math.sqrt(sum(squared_stops)/len(squared_stops))

        if self.avg_stop_time > 200.0:
            logging.debug('Found stops: ')
            logging.debug(self.stops)
            logging.debug('Found rows: ')
            logging.debug(self.rows)
            
            
class TrainMovementSituation:
    xml_body = '<?xml version="1.0" encoding="UTF-8"?><Situation><ThingId>Train</ThingId><SituationTemplateId>InMovement</SituationTemplateId><Active>false</Active></Situation>'
    headers = {'Content-Type': 'application/xml'}
    
    def __init__(self, situationsapi_url, train_id):
        self.train_id = train_id
        self.situationsapi_url = situationsapi_url
        self.situation_id = None
        self.situation_url = None
        
    def createSituation(self):
        r = requests.post(self.situationsapi_url + '/situations', data=self.getXmlBody(None, False), headers=self.headers)
        self.situation_url = r.content[1:-1]
        self.situation_id = self.situation_url.decode().split('/')[-1]
    
    def setActive(self, value):
        logging.debug('Setting situation ' + str(self.situation_id) +' to ' + str(value))
        r = requests.put(self.situation_url, data=self.getXmlBody(self.situation_id, value), headers=self.headers)
        
    def getXmlBody(self, id, value):
        xml_body = '<?xml version="1.0" encoding="UTF-8"?><Situation '
        xml_part1 = '><ThingId>Train</ThingId><SituationTemplateId>InMovement</SituationTemplateId><Active>'
        xml_part2 = '</Active></Situation>'
        if id != None:
            xml_body += 'id="' + id + '"'
        xml_body += xml_part1
        if value:
            xml_body += 'true' + xml_part2
        else:
            xml_body += 'false' + xml_part2        
        return xml_body
        
def main(argv):
    if classical:
        r = fetch_istmeldung_data_reader()

        rows = list()
        for row in r:
            rows.append(row)

        for row in rows:
            if row[1].isdigit():
                train_ids.add(row[1])

        logging.debug('Found ' + str(len(train_ids)) + ' trains in  data')

        invalid = 0
        count = 0

        shortest_stop_time = 0
        longest_stop_time = 0

        for train_id in train_ids:
            #train_id = train_ids.pop()
            count = count + 1
            logging.debug('----------------------')
            logging.debug('Train ' + str(count) + ' with id ' + train_id)
            train_rows = fetch_rows_by_trainid(train_id, rows)
            trainMovSit = TrainMovementSituation('http://localhost:1337/situationsapi', train_id)
            train = Train(train_id, train_rows, trainMovSit)

            if train.isValid():
                trains.add(train)
            else:
                invalid_trains.add(train)
                invalid = invalid + 1


        delay_times = list()
        stop_times = list()

        for train in trains:
            delay_times.extend(train.get_delay_times())
            stop_times.extend(train.get_stop_times())

        logging.debug('-----------------------------')
        logging.debug('Number of invalid train data ' + str(invalid))

        min_stop_time = min(stop_times)
        max_stop_time = max(stop_times)
        mean_stop_time = statistics.mean(stop_times)
        median_stop_time = statistics.median(stop_times)
        stdev_stop_time = statistics.stdev(stop_times)

        logging.debug('-----------------------------')
        logging.debug("Shortest stop time: " + str(min_stop_time))
        logging.debug("Longest stop time: " + str(max_stop_time))
        logging.debug('Mean stop time: ' + str(mean_stop_time))
        logging.debug('Median stop time: ' + str(median_stop_time))
        logging.debug('Standard-deviation stop time: ' + str(stdev_stop_time))


        min_delay_time = min(delay_times)
        max_delay_time = max(delay_times)
        mean_delay_time = statistics.mean(delay_times)
        median_delay_time = statistics.median(delay_times)
        stdev_delay_time = statistics.stdev(delay_times)

        logging.debug('-----------------------------')
        logging.debug("Shortest delay time: " + str(min_delay_time))
        logging.debug("Longest delay time: " + str(max_delay_time))
        logging.debug('Mean delay time: ' + str(mean_delay_time))
        logging.debug('Median delay time: ' + str(median_delay_time))
        logging.debug('Standard-deviation delay time: ' + str(stdev_delay_time))

        if write:
            file = open('./results/stop_times.csv', 'w', newline='')
            writer = csv.writer(file)
            writer.writerow(['type', 'min', 'max', 'mean', 'median', 'std_dev'])
            writer.writerow(['stop', min_stop_time, max_stop_time, mean_stop_time, median_stop_time, stdev_stop_time])
            file.close()

            file = open('./results/delay_times.csv', 'w', newline='')
            writer = csv.writer(file)
            writer.writerow(['type', 'min', 'max', 'mean', 'median', 'std_dev'])
            writer.writerow(['delay', min_delay_time, max_delay_time, mean_delay_time, median_delay_time, stdev_delay_time])
            file.close()

            file = open('./results/train_times.csv', 'w', newline='')
            writer = csv.writer(file)
            writer.writerow(['id', 'avg_delay', 'deviation_delay', 'avg_stop', 'deviation_stop', 'max_stop', 'min_stop'])

            for train in trains:
                writer.writerow([train.train_id, train.avg_delay_time, train.deviation_delay_time, train.avg_stop_time, train.deviation_stop_time, train.getworstcasestoptime(), train.getbestcasestoptime()])

            file.close()


def fetch_istmeldung_data_reader():
    local_path = get_script_path()+ "/" + data_source_istmeldungen
    if(os.path.exists(local_path)):
        return csv.reader(open(get_script_path()+ "/" + data_source_istmeldungen,'r'), delimiter=';')
    else:
        r = requests.get(url_source_istmeldungen)
        f = open(get_script_path()+ "/" + data_source_istmeldungen, 'wb')
        f.write(r.content)
        return fetch_istmeldung_data_reader()
    
def fetch_rows(trainid, reader):
    rows = list()
    for row in reader:
        if row[1] == trainid:
            rows.append(row)
    return rows        

def fetch_rows_by_trainid(trainid, rows):
    foundRows = list()
    for row in rows:
        if row[1] == trainid:
            foundRows.append(row)
    return foundRows

# from https://stackoverflow.com/a/4943474
def get_script_path():
    return os.path.dirname(os.path.realpath(sys.argv[0]))        
        
if __name__ == "__main__":
   main(sys.argv[1:])