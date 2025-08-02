from pymodbus.client import ModbusSerialClient


def genModbusClient(serialPort):

    master = ModbusSerialClient(
        port     = serialPort,  # Nombre del puerto serial a utilizar. COM en windows, tty en Linux
        timeout  = 2, # Tiempo de espera de reintento de conexión (s)
        retries  = 10, # Cantidad de reintentos en caso de reconexión fallida
        baudrate = 19200, # Definido por la documentación de la estación meteorologica
        bytesize = 8, # Definido por la documentación de la estación meteorologica
        parity   = "E", # Definido por la documentación de la estación meteorologica
        stopbits = 1, # Definido por la documentación de la estación meteorologica
    )
    return master
