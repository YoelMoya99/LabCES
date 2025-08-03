import asyncio
from pymodbus.client import ModbusSerialClient
from pymodbus import ModbusException


class modbusComunication:
    def __init__(self):

        # Modbus protocol information
        self.serialPort = 'COM4'
        self.baudRate = 19200
        self.stopBits = 1
        self.byteSize = 8
        self.parity = 'E'
        self.timeout = 2
        
        # Modbus slave information
        self.address = 0x0
        self.count = 10
        self.slave = 0x1

        # Modbus Object
        self.modbusMaster = None

        # Utilitarian
        self.processSuccess = False
        

    async def modbusConfiguration(self):
        try:
            self.modbusMaster = ModbusSerialClient(
                port = self.serialPort,
                baudrate = self.baudRate,
                stopbits = self.stopBits,
                bytesize = self.byteSize,
                parity = self.parity,
                timeout = self.timeout
            )
            self.processSuccess = True
            print('modbus client created successfully...')

        except Exception:
            self.processSuccess = False
            print('modbus client wasnt created...')

        
    async def modbusConnect(self):
        try:
            self.processSuccess = self.modbusMaster.connect()
            self.processSuccess = True
            print('connection successful...')
        except Exception:
            self.processSuccess = False
            print('unsuccessful connection...')


    async def modbusDataRequest(self):
        try:
            temp = self.modbusMaster.read_input_registers(
                address = self.address,
                count = self.count,
                slave = self.slave
            )
            self.processSuccess = True
            print('succesful request of data...')
        except Exception:
            self.processSuccess = False
            print('unsuccessful request of data...')
        
        if self.processSuccess:
            return temp.registers


    async def modbusDisconnect(self):
        self.modbusMaster.close() 


