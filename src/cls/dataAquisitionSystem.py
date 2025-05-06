import os
from pymodbus.client import ModbusSerialClient
from pymodbus import ModbusException
import pandas as pd
from datetime import datetime 

from src.func.utils import (
    getComList,
    getModbusClient, 
    getFileName
    )


class dataAquisitionSystem:

    def __init__(self):
        self.comPort = None
        self.comList = []
        self.successProcess = False
        self.dataFrame = None
        self.processTimer = None
        self.modbusClient = None

'''
 ---------------------------------------------------------
                    Fist state machine
 ---------------------------------------------------------
'''

    def PROCESS_1(self):
        pass        

    def state1(self):
        
        match (self.comPort, len(self.comList)):
            case (None, x) if x is not 0:
                self.comPort = self.comList[0]     # assign com port
                self.comList.remove(self.comPort)  # delete the assigned com port
                print(f'Got all the serial ports: {self.comList}')
                print(f'Will create client with {self.comPort}')
            case _:
                self.comList = getComList()        # Get list of com ports
                self.comPort = self.comList[0]     # assign com port
                self.comList.remove(self.comPort)  # delete the assigned com port

        try:
            self.modbusClient = genModbusClient(self.comPort)
            self.comPort = None
            self.sucessProcess = True
            print(f'Created the client :3')
        except Exception as e:
            print(f'Hi, there is an error in state 1: {e}')
            self.sucessProcess = False


    def state2(self):
        
        try:
            self.sucessProcess = self.modbusClient.connect():
        except Exception as e:
            print(f'Hi, error in state2: {e}')
            self.sucessProcess = False


    def state3(self):

        try:
            temp = self.modbusClient.read_input_registers(
                address=0x0,
                count=10,
                slave=0x1
                )
            currentDateTime = datetime.now().strftime(
                    '%Y-%m-%d %H:%M:%S'
                    )
            self.dataFrame = pd.DataFrame({
                'timestamp': [current_datetime],
                'windspeed': temp.registers[0]/100,
                'wind_direction': temp.registers[1]/10,
                'avg_temperature': temp.registers[4]/10,
                'relative_humidity': temp.registers[6]/10,
                'barometric_pressure': temp.registers[7]/10,
                'solar': temp.registers[9]
                })
            self.sucessProcess = True
        except Exception as e:
            print(f'Error in state3: {e}')
            self.sucessProcess = False

    def state4(self):

        fileName = getFileName()
        self.dataFrame.to_csv(
            fileName,
            mode='a',
            header=not os.path.exists(fileName),
            index=False
            )
