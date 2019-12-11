#!/usr/bin/python
########################################################################
# Filename    : power_meter_debug.py
# Description : Script to read and log all data
# Author      : Gjengedal
# modification: 10.12.2019
########################################################################
from __future__ import print_function
import serial
import sys
import datetime
import time
import os
import glob
import logging
import logging.config
import paho.mqtt.client as mqtt
import binascii
from crccheck.crc import CrcX25

# create logger
logging.config.fileConfig('logging.conf')
logger = logging.getLogger('rotatingLogger')

class MyMQTTClass:
    def __init__(self, clientid=None):
        logger.info("Broker client created.")
        self.brokerip = "192.168.12.114"
        self.brokerport = 32782
        self._mqttc = mqtt.Client(clientid)
        self._mqttc.on_message = self.mqtt_on_message
        self._mqttc.on_connect = self.mqtt_on_connect
        self._mqttc.on_publish = self.mqtt_on_publish
        self._mqttc.on_subscribe = self.mqtt_on_subscribe
    def mqtt_on_connect(self, mqttc, obj, flags, rc):
        logger.info("Connected, rc: "+str(rc))
    def mqtt_on_message(self, mqttc, obj, msg):
        logger.info(msg.topic+" "+str(msg.qos)+" "+str(msg.payload))
    def mqtt_on_publish(self, mqttc, obj, mid):
        #logger.info("published: mid: "+str(mid))
        pass
    def mqtt_on_subscribe(self, mqttc, obj, mid, granted_qos):
        logger.info("Subscribed: "+str(mid)+" "+str(granted_qos))
    def mqtt_on_log(self, mqttc, obj, level, string):
        logger.info(string)
    def disconnect(self):
        self._mqttc.loop_stop()
        logger.info("Disconnected")
    def publish(self, topic, payload):
        self._mqttc.publish(topic, payload)
    def run(self):
        try:
            logger.info("Connecting to broker:")
            self._mqttc.connect(self.brokerip, self.brokerport)
            self._mqttc.loop_start()
        except Exception as e:
            logger.error("Error in run function")
            logger.error(e)
            time.sleep(30)
            self.run()

class Power_meter():
    def __init__(self):
        self.ser = serial.Serial(
            port='/dev/ttyUSB0',
            baudrate=2400,
            parity=serial.PARITY_NONE,
            stopbits=serial.STOPBITS_ONE,
            bytesize=serial.EIGHTBITS,
            timeout=3) # 10 original
        logger.info("connected to: " + self.ser.portstr)

    def test_data(self, data):
        # Preform Crc x25 check on data.
        x = bytearray.fromhex(data[0])
        y = CrcX25.calchex(data=x, byteorder="little")
        if str.upper(y) == data[1]:
            logger.debug("%s Recieved %s bytes of true data" % (datetime.datetime.now().isoformat(), len(data[0])))
            print("%s Recieved %s bytes of true data" % (datetime.datetime.now().isoformat(), len(data[0])))
            return True
        logger.warning("%s Recieved %s bytes of false data" % (datetime.datetime.now().isoformat(), len(data[0])))
        print("%s Recieved %s bytes of false data" % (datetime.datetime.now().isoformat(), len(data[0])))
        return False

    def trim_data(self, data):
        # Trim data for start and end flag.
        #Returns data and 16bit CRC as a tuple.
        print("Data: %s" % data)
        print("trimming data:")
        x = [x.strip(' ') for x in data]
        bytestring = "".join(x)
        datastring = bytestring[2:-6]
        crc = bytestring[-6:-2]
        print("Datastring: %s" % datastring)
        print("Crc: %s" % crc)
        return datastring, crc

    def parse_data(self, bytestring):

        meter_id_i = bytestring.find("0101000005FF") + 16
        meter_type_i = bytestring.find("0101600101FF") + 16
        act_pwr1_i = bytestring.find("0101010700FF") + 14 # Active import power
        act_pwr2_i = bytestring.find("0101020700FF") + 14 # Active export power
        react_pwr1_i = bytestring.find("0101030700FF") + 14 # Reactive import power
        react_pwr2_i = bytestring.find("0101040700FF") + 14 # Reactive export power
        cur_l1_i = bytestring.find("01011F0700FF") + 14
        cur_l2_i = bytestring.find("0101330700FF") + 14
        cur_l3_i = bytestring.find("0101470700FF") + 14
        vol_l1_i = bytestring.find("0101200700FF") + 16
        vol_l2_i = bytestring.find("0101340700FF") + 16
        vol_l3_i = bytestring.find("0101480700FF") + 16
        # Coment out what you dont need in bs_list:
        bs_list = {
            "meter_id" : bytestring[meter_id_i : meter_id_i + 32],
            "meter_type" : bytestring[meter_type_i : meter_type_i + 36],
            "act_pwr_in" : bytestring[act_pwr1_i : act_pwr1_i + 8],
            "act_pwr_out" : bytestring[act_pwr2_i : act_pwr2_i + 8],
            "react_pwr_in" : bytestring[react_pwr1_i : react_pwr1_i + 8],
            "react_pwr_out" : bytestring[react_pwr2_i : react_pwr2_i + 8],
            "cur_l1" : bytestring[cur_l1_i : cur_l1_i + 8],
            "cur_l2" : bytestring[cur_l2_i : cur_l2_i + 8],
            "cur_l3" : bytestring[cur_l3_i : cur_l3_i + 8],
            "vol_l1" : bytestring[vol_l1_i : vol_l1_i + 2],
            "vol_l2" : bytestring[vol_l2_i : vol_l2_i + 2],
            "vol_l3" : bytestring[vol_l3_i : vol_l3_i + 2],
        }

        if len(bytestring) > 500: # long list
            sum_KWH_in_i = bytestring.find("0101010800FF") + 14
            sum_KWH_out_i = bytestring.find("0101020800FF") + 14
            bs_list["sum_kwh_in"] = bytestring[sum_KWH_in_i : sum_KWH_in_i + 8]
            bs_list["sum_kwh_out"] = bytestring[sum_KWH_out_i : sum_KWH_out_i + 8]
        return bs_list

    def format_data(self, data):
        new_format = {}
        for i, y in data.items():
            if len(y) > 8:
                new_format[i] = y.decode("hex")
            elif "cur_l" in i: # format current
                x = int(y, 16)
                new_format[i] = float(x) / 100
            elif "sum" in i: # format total consumtion to WH
                z = int(y, 16)
                new_format[i] = float(z) * 10
            else:
                new_format[i] = int(y, 16)       
        return new_format

    def print_data(self, data):
        for i, y in data.items():
            print(i, y)
        print("---------------------")
        logger.info("Data printed to screen")

    def publish_data(self, data):
        counter = 0
        for i, y in data.items():
            mqttc.publish(i, y)
            counter = counter + 1
        logger.debug("%s data points published" % counter)

    def read_bytes(self):
        #New read function:
        #byteCounter = 0
        #bytelist = []
        while True:
            a = self.ser.read(1000) # reads up to 1000 bytes. Times out after 1 sec?
            if a:
                b = binascii.hexlify(a)
                print(b)
                #byteCounter = byteCounter + 1
                return b
            else:
                print("Timeout")
                logger.error("No data, check wiring!")
                time.sleep(1)


if __name__ == '__main__':
    #flow:
    logger.info("Starting script in debug mode")
    mqttc = MyMQTTClass()
    mqttc.run()
    time.sleep(10)
    app = Power_meter()
    logger.info("Starting loop.")
    while True:
        try:
            raw_bytes = app.read_bytes()
            trimmed_data = app.trim_data(raw_bytes)
            if app.test_data(trimmed_data):
                clean_bytes = app.parse_data(trimmed_data[0])
                formated_bytes = app.format_data(clean_bytes)                
                #app.print_data(formated_bytes) # uncomment if you want to print data to screen
                app.publish_data(formated_bytes)
                logger.debug(formated_bytes)
        except KeyboardInterrupt:
            logger.info("exit from keyboard")
            mqttc.disconnect()
            app.ser.close()
            break
        except Exception as e:
            logger.error(e)
            time.sleep(5)
