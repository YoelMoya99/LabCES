from serial.tools.list_ports import comports
from datetime import datetime


def getComList():

    '''
    Goal: Regardless of OS, get the name of the serial
        ports avaliable. For hopefully automate the
        connection or reconnection of the app
    '''

    ports = comports()
    rawPortList = [port.device for port in ports]

    portList = []
    for port in rawPortList:
        if port.upper().startswith("COM") and int(port[3:]) >= 10:
             portList.append('\\\\.\\' + port)
        else:
            portList.append(port)
    
    return portList

def genFileName(base_name='station_data'):

    '''
    Goal: Generate automatically the name of a file
        based on the date and time for autosaving in
        separate files
    '''
    
    current_time = datetime.now().strftime('%Y-%m-%d')
    return f'{base_name}_{current_time}.csv' 

