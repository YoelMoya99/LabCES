from pymodbus.client import ModbusSerialClient
import time

client = ModbusSerialClient(
    port='COM4',        # Change to match your COM port
    baudrate=19200,
    stopbits=1,
    bytesize=8,
    parity='E',
    timeout=2           # Optional but recommended
)

if client.connect():
    while True:
        result = client.read_input_registers(address=0, count=4)
        if result.isError():
            print("Read error:", result)
        else:
            print("Registers:", result.registers)
        time.sleep(10)
else:
    print("Failed to connect to the slave")

client.close()

