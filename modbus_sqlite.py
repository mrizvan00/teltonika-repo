import sqlite3
from pymodbus.client import ModbusSerialClient #initialize a serial RTU client instance
from pymodbus.exceptions import ModbusIOException, ModbusException
from pymodbus import (ExceptionResponse,Framer,ModbusException,pymodbus_apply_logging_config)
import time
import threading
import paho.mqtt.client as mqttclient 
import time




#define Modbus client
client_rtu = ModbusSerialClient(method = "rtu", port="COM5",stopbits = 1, bytesize = 8, parity = 'N', baudrate= 9600, timeout=3)


def write_request(writeAdd, writeValue, writeSlave):
    try:
        # Connect to the Modbus server
        if client_rtu.connect():
            try:
                writeRequestData = f"Address: {writeAdd}, Value: {writeValue}, SlaveID: {writeSlave}"
                print(f"writeRequestData: {writeRequestData}")
                writeType = "Write"
                # Write to a holding register
                result = client_rtu.write_register(writeAdd, writeValue, writeSlave)

                # Check if the write operation was successful
                if result.isError():
                    writeResponseData = str(result)
                    print(f"writeResponseData = {writeResponseData}")
                else:
                    writeResponseData = "OK"   
                    print(f"writeResponseData: {writeResponseData}")
                  

            except ModbusIOException as e:
                print(f"Modbus IO Exception: {e}")

        else:
            print("Unable to connect to Modbus server")

    except ModbusException as e:
        print(f"Modbus Exception: {e}")

    finally:
        # Close the Modbus TCP connection
        client_rtu.close()

    return writeRequestData, writeResponseData, writeType


def read_request(readAdd, readCount, readSlave):
    try:
        if client_rtu.connect():
            readRequestData = f"Address: {readAdd}, Count: {readCount}, SlaveID: {readSlave}"
            print(f"readRequestData: {readRequestData}")
            readType = "Read"
            result = client_rtu.read_holding_registers(readAdd, readCount, readSlave)
            

            if result.isError():
                readResponseData = str(result)
                print(f"readResponsData = {readResponseData}")
            else:
                result_raw = result.registers #raw response format: list of integers
                readResponseData = "".join(str(result_raw)) #list needs to be converted to string     
                
                print(f"readResponseData: {readResponseData}")
                      
    finally:
        client_rtu.close()
    
    return readRequestData, readResponseData, readType
   


# writeResult = write_request(1, 500, 1)
# print(f"Write reuslt: {writeResult} and result type is {type(writeResult)}")
# time.sleep(1)
# readResult = read_request(1, 20, 1)
# print(f"Read result: {writeResult} and result type is {type(readResult)}")


    
#create db file and connect to db
conn = sqlite3.connect('local_DB.db')
print ("Connected to DB")
#create a cursor that would execute SQL commands
c = conn.cursor() #start of querry

querry_createTable = """CREATE TABLE IF NOT EXISTS request_response(
                    request_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    type TEXT,
                    request_data TEXT,
                    response_data TEXT,    
                    time_stamp datetime)"""

c.execute(querry_createTable)
conn.commit() 
#conn.close() #Next querry raise error after closing connection: Cannot operate on a closed database! 





def SQL_querry():

    #conn = sqlite3.connect('local_DB.db')
    writeData = write_request(1,1100,1) # a tuple od strigns returned by function
    querry_insertWriteData = """INSERT INTO request_response (type, request_data, response_data, time_stamp) 
                                VALUES (?, ?, ?, datetime('now', 'localtime'))""",(writeData[2], writeData[0], writeData[1])
    
    c.execute(*querry_insertWriteData) #When using placeholders ?, tupples are being passed and needs to be unpacked by inserting * before querry 
    conn.commit()


    readData = read_request(1,1,1) # a tuple od strigns returned by function
    querry_insertReadData = """INSERT INTO request_response (type, request_data, response_data, time_stamp) 
                                VALUES (?, ?, ?, datetime('now', 'localtime'))""",(readData[2], readData[0], readData[1])
    
    c.execute(*querry_insertReadData) #When using placeholders ?, tupples are being passed and needs to be unpacked by inserting * before querry 
    conn.commit()



for i in range (20):
    SQL_querry()



# def writeHandle():
#     write_request()
#     writeToSQL()


# def writeHandle():
#     write_request()
#     readToSQL()



# mqtt_thread = threading.Thread(target=mqtt_loop)
# mqtt_thread.start()


# another_thread = threading.Thread(target=another_task)
# another_thread.start()



