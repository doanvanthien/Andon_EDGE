import threading
import time
from datetime import datetime
import json
import os
import pika
import serial
import sqlite3
from sqlite3 import Error

os.system('sudo chmod 666 /dev/hidraw*')

# ######## UART ################

import RPi.GPIO as GPIO
GPIO.setmode(GPIO.BCM)
GPIO.setwarnings(False)

for i in range(1, 14):
    s = GPIO.setup(i, GPIO.OUT)
    GPIO.output(i, GPIO.HIGH)
for i in range(16, 28):
    GPIO.setup(i, GPIO.OUT)
    GPIO.output(i, 1)

ser = serial.Serial(
    port = '/dev/ttyS0',
    baudrate = 115200,
    parity = serial.PARITY_NONE,
    stopbits = serial.STOPBITS_ONE,
    bytesize = serial.EIGHTBITS,
    timeout = 0.1
)  


# ######## SQL ###########################
database = r"data_err.db"

# create a database connection
def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)

    return conn

def insertErrData(priority):
    conn = create_connection(database)
    cur = conn.cursor()
    try:
        with conn:
            cur.execute("INSERT INTO ERRORDATA VALUES(?)", (priority,))
    except:
        print("Cannot insert into DB")

# ########################################
# connect rabbitMQ

def connect_Rabbit(message):
    connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                '192.168.0.105', 5672, 
                '/', 
                pika.PlainCredentials('avani', 'avani'),
                blocked_connection_timeout=1000,
                heartbeat=0,))
    channel = connection.channel()
    channel.queue_declare(queue='test')
    channel.basic_publish(
        exchange='', routing_key='test', body=message)
    print(message)

def check_connect_Rabbit(RB_MSG):
    try:
        connect_Rabbit(RB_MSG)    
    except:
        insertErrData(RB_MSG)


#  READ _ DEVICE #########################

def device():
    while True:
        s        = ser.readline()
        data     = s.decode()			# decode s
        dev_data = data.rstrip()			# cut "\r\n" at last of string
        if(data):
            print("DEVICE: " + data)				# print string
            dev_msg = {
                "type": "barcode",
                "time": datetime.now().strftime('%Y%m%d%H%M%S'),
                "data": dev_data
            }
            check_connect_Rabbit(dev_msg)

# ########################################
# for barcode

def filter(input):
    b = ''
    for c in input:
        if c.isprintable():
            b = b + str(c)
    return b

# main thread function


def read(f_name):
    while True:
        try:
            os.system('sudo chmod 666 /dev/hidraw*')
            fp = open(f_name, 'rb')
            try:
                print("READING...")
                code = fp.read(64)
                fp.close()
                try:
                    bar_mess = 'BARCODE:' + filter(code.decode())
                    print(bar_mess)
                    bar_mess = {
                        "type": "barcode",
                        "time": datetime.now().strftime('%Y%m%d%H%M%S'),
                        "data": filter(code.decode())
                    }

                    bar_msg = json.dumps(bar_mess)
                    check_connect_Rabbit(bar_msg)
                    
                except:
                    tag = code.hex()
                    print("rfid:" + tag[38:42])
                    rfid_mess = {
                        "type": "rfid",
                        "time": datetime.now().strftime('%Y%m%d%H%M%S'),
                        "data": tag[38:42]
                    }

                    rfid_msg = json.dumps(rfid_mess)
                    check_connect_Rabbit(rfid_msg)
            except:
                print("close")
                fp.close()  
        except:
            time.sleep(1)
            pass


#  function for handle error data

def handleErrData():
    conn = create_connection(database)
    cur = conn.cursor()
    try:
        print("Thread 4") 
        while True:
            with conn:
                start_time = time.time()
                cur.execute("SELECT * FROM ERRORDATA LIMIT 1", ())
                rows = cur.fetchall()
                for row in rows:
                    print(row)
                    try:
                        connect_Rabbit(row)
                        cur.execute("DELETE FROM ERRORDATA LIMIT 1", ())
                        print(time.time() - start_time)
                        time.sleep(1)
                    except:
                        print("connect fail")
                        pass
    except:
        # print("Handle error !!!")
        pass


try:
    threads = list()

    threads.append(threading.Thread(target=read, args=("/dev/hidraw0",)))
    threads.append(threading.Thread(target=read, args=("/dev/hidraw1",)))
    threads.append(threading.Thread(target=read, args=("/dev/hidraw2",)))
    threads.append(threading.Thread(target=device, args=()))
    threads.append(threading.Thread(target=handleErrData, args=()))

    [thread.start() for thread in threads]
    [thread.join() for thread in threads]

except:
    print ("error")