# OPC-UA-Client-with-Mainflux-IoT-Integration
Scripts to dynamically retrieve data from an OPC - UA server with MySQL local storage functionality and support to push data onto Mainflux IoT cloud platform.

## Prerequisites
- Python 3.7+
- MySQL for Visual Studio
- Visual Studio 2019
- Mainflux

## Instructions
1. To begin the program, perform : ``` python3 Driver.py ```
2. MQTT.py must run in the background, perform : ``` python3 MQTT.py ```

## Note
- Driver.py is the driver script to initialise all the functions and obtain user inputs. Driver.py generates a JSON configuration file 'config.json' which contains all the credentials entered by the user.

- GUI.py is solely responsible for all GUI-related operations.

- DB.py performs the DB related functions to write / read from MySQL databases and tables.

- MQTT.py is a standalone script intended to run in the background. Performs the function of dynamically collecting data from the MySQL database and publishing the data to Mainflux IoT cloud via MQTT.

**NOTE: Driver.py must be executed before MQTT.py. This is because MQTT.py requires details from 'config.json' which is created only by Driver.py**

- The data retrieved from the OPC UA server is stored in MySQL databases and .csv files ( local backup ).

- Refer to 'references.txt' for more information.
