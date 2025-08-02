from pymodbus.server import StartSerialServer
from pymodbus.datastore import (
    ModbusSlaveContext,
    ModbusServerContext, 
    ModbusSequentialDataBlock
)

inputRegisters = ModbusSequentialDataBlock(0, [33]*100)

store = ModbusSlaveContext(
    hr = None,
    ir = inputRegisters,
    co = None,
    di = None
)

context = ModbusServerContext(slaves=store, single=True)

'''
Modbus specifications taken from the metheorological station's documentation
'''

StartSerialServer(
    context  = context,
    port     = "COM5",
    baudrate = 19200,
    parity   = 'E',
    stopbits = 1,
    bytesize = 8,
)

