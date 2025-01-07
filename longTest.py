import time
from pymodbus.client import ModbusSerialClient
from pymodbus import ModbusException
import pandas as pd
from datetime import datetime 

''' 
------------------------------------------------------------------
 Variables de configuración del sistema
------------------------------------------------------------------
'''
file_name = 'prueba_consistencia_tiempo.csv' # nombre base del archivo a escribir
error_log_file_name = 'error_log.txt'        # Archivo de registro de errores de todo tipo
TimerRequestPeriod = 3                       # tiempo entre transacciónes MODBUS


def CreateMaster():
    master = ModbusSerialClient(
        port="/dev/ttyUSB0",  # Nombre del puerto serial a utilizar. COM en windows, tty en Linux
        timeout=2, # Tiempo de espera de reintento de conexión (s)
        retries=10, # Cantidad de reintentos en caso de reconexión fallida
        baudrate=19200, # Definido por la documentación de la estación meteorologica
        bytesize=8, # Definido por la documentación de la estación meteorologica
        parity="E", # Definido por la documentación de la estación meteorologica
        stopbits=1, # Definido por la documentación de la estación meteorologica
    )
    return master

def get_test_file_name(base_name='station_data'):
    current_time = datetime.now().strftime('%Y-%m-%d')
    return f'{base_name}_{current_time}.csv' 

def main():

    while True:
        try:
            # begin try except outer loop ---------------------------------------------------------------------------------------
            Station = CreateMaster()
            if Station.connect(): # Connection to slave device
                print("Connection Successful")
                
                while True:
                    try:
                        # begin try except inner loop ---------------------------------------------------------------------------
                        register = Station.read_input_registers(address=0x0, count=10, slave=0x1)
                        print(register.registers)
                        current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        new_data = pd.DataFrame({
                             'timestamp': [current_datetime],
                             'windspeed': register.registers[0]/100,
                             'wind_direction': register.registers[1]/10,
                             'avg_temperature': register.registers[4]/10,
                             'relative_humidity': register.registers[6]/10,
                             'barometric_pressure': register.registers[7]/10,
                             'solar': register.registers[9]
                        })
                        file_name = get_test_file_name()
                        new_data.to_csv(file_name, mode='a', header=not pd.io.common.file_exists(file_name), index=False)
                        print(f'Appended data with timesamp {current_datetime}')
                        time.sleep(TimerRequestPeriod)
                    
                        # end try except inner loop ---------------------------------------------------------------------------
                    except Exception as e:
                        ''' Manejo de exepciones de conexión: se realiza dentro de un bucle infinito para que este
                            siga conectando aunque falle del todo el sistema, por lo que crea de nuevo un objeto
                            MODBUS. 
                        '''
                        current_date_time_error = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        message = f'There was a failed to connect error on this date -> {current_date_time_error}; and the error -> {e}'
                        print(message)
                        print('inner loop')
                        #time.sleep(TimerRequestPeriod)
 
            else:
                print("Failed to connect to Modbus device")
                Station.close()
                time.sleep(5)

            # end try-except outer loop -----------------------------------------------------------------------------------------
        except Exception as e:
            ''' Manejo de exepciones de conexión: se realiza dentro de un bucle infinito para que este
                siga conectando aunque falle del todo el sistema, por lo que crea de nuevo un objeto
                MODBUS. 
            '''
            current_date_time_error = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            message = f'There was a failed to connect error on this date -> {current_date_time_error}; and the error -> {e}'
            print(message)
            
            with open(error_log_file_name, 'a') as file:
                file.write(message + '\n')
                

if __name__ == "__main__":
    	
    main()
