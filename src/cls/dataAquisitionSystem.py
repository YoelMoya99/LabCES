import os
from pymodbus.client import ModbusSerialClient
from pymodbus import ModbusException
import pandas as pd
from datetime import datetime 
from time import monotonic, sleep

from src.func.utils import (
    getComList,
    genFileName
    )
from src.func.mainModbusFunc import genModbusClient


class dataAquisitionSystem:

    def __init__(self):
        self.comPort        = None
        self.comList        = []
        self.successProcess = False
        self.dataFrame      = None
        self.processTimer   = None
        self.modbusClient   = None
        self.presentState   = None
        self.retriesCounter = None

    '''
     ---------------------------------------------------------
                        Fist state machine
     ---------------------------------------------------------
    '''

    def state1(self):
        
        match (self.comPort, len(self.comList)):
            case (None, x) if x != 0:
                self.comPort = self.comList[0]     # assign com port
                self.comList.remove(self.comPort)  # delete the assigned com port
            case _:
                
                self.comList = getComList()        # Get list of com ports
                
                print(f'Got all the serial ports: {self.comList}...')

                self.comPort = self.comList[0]     # assign com port
                self.comList.remove(self.comPort)  # delete the assigned com port

        print(f'Will create client with {self.comPort}...')

        try:
            self.modbusMstr = genModbusClient("COM4")
            self.comPort = None
            self.successProcess = True
            print(f'Created the client :3')
        except Exception as e:
            print(f'Hi, there is an error in state 1: {e}')
            self.successProcess = False


        match self.successProcess:
            case True:
                self.presentState = self.state2
            case _:
                self.presentState = self.state1


    def state2(self):
        
        try:
            self.sucessProcess = self.modbusMstr.connect()
        except Exception as e:
            print(f'Hi, error in state2: {e}')
            self.successProcess = False

        self.retriesCounter = 10

        match (self.successProcess, self.retriesCounter):
            case (True, _):
                self.presentState = self.state3
            case (False, x) if x > 0:
                self.presentState = self.state2
                self.retriesCounter -=1
                print('Connection failed u.u')
                print(f'{self.retriesCounter} retries left!')
            case _:
                self.presentState = self.state1
                print('failed all in connection, back to state1')


    def state3(self):
        
        self.processTimer = monotonic()
        try:
            temp = self.modbusClient.read_input_registers(
                address=0x0,
                count=10,
                slave=0x1
                )
            print('Read imput registers response:')
            print(temp)
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

        self.retriesCounter = 10

        match (self.successProcess, self.retriesCounter):
            case (True, _):
                self.presentState = self.state4
            case (False, x) if x > 0:
                self.presentState = self.state3
                print('Data request failed u.u')
                print(f'{self.retriesCounter} retries left!')
            case _:
                self.presentState = self.state2

    def state4(self):

        #fileName = genFileName()
        #self.dataFrame.to_csv(
        #    fileName,
        #    mode='a',
        #    header=not os.path.exists(fileName),
        #    index=False
        #    )
        
        self.presentState = self.state5
        print('\n\nSuccessfully retreieved data!')
        print(self.dataFrame)

    def state5(self):
        deltaTime = monotonic() - self.processTimer
        waitTime = 5 - deltaTime
        print(f'Waiting for {waitTime} seconds')
        sleep(waitTime)

        self.presentState = self.state3
