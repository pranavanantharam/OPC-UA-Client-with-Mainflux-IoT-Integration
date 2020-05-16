import logging
logging.basicConfig(level=logging.ERROR)
import datetime
from datetime import timezone
from opcua.common import events
import opcua.common
import sys
import os
import time
import json
import string
from opcua import ua
from opcua import Client
from os import system, name 
from time import sleep
import mysql.connector
from mysql.connector import Error
import paho.mqtt.client as mqtt
import requests


OPC_list=list()
MYSQL_list=list()
MAINFLUX_list=list()
MQTT_list=list()


DATA_CONFIG="DATA_CONFIG_TABLE"
DATA_CONFIG=str(DATA_CONFIG)


configlist=list()
dir_path = os.path.dirname(os.path.realpath(__file__))
dir_path=str(dir_path)


def startup():
    config_exist=False
    for file in os.listdir(dir_path):
        if (file=="config.json"):
            configlist.append(os.path.join(dir_path, file))
            config_exist=True
            break
    if(config_exist==False):
        print('Configuration file does not exist.')
        return False



    configpath=configlist[0]

    with open(configpath,'r') as f:
        data=json.load(f)

        for x in data['Settings']:
            OPCname=x['OPC Server Name']
            OPCurl=x['OPC Server URL']
            sqlhost=x['SQL Host']
            sqluser=x['SQL Username']
            sqlpwd=x['SQL Password']
            dbname=x['Database Name']
            flux_ip=x['Mainflux IP']
            flux_port=x['Mainflux Port']
            flux_email=x['Mainflux Email']
            flux_pwd=x['Mainflux Password']


    try:
        client=Client(OPCurl)
        client.connect()
        OPC_list.append(client)

        mydb=mysql.connector.connect(host=sqlhost,user=sqluser,passwd=sqlpwd,database=dbname)
        mycursor=mydb.cursor()
        MYSQL_list.append(mydb)
    
        mqclient=mqtt.Client("P1")       
        MQTT_list.append(mqclient)
    
        MAINFLUX_list.append(flux_ip)
        MAINFLUX_list.append(flux_port)

        print('Connection successful')
        
        return True

    except:
        print('Error - could not load configuration file')
        configlist.clear()
        return False
    




def convertTuple(tup): 
    string =  ''.join(tup) 
    return string



# to get all parentnode id's
def get_all_parents():
    
    PARENT_ID=list() 
    try:
        mydb=MYSQL_list[0]
        mycursor=mydb.cursor()
        qry="SELECT ParentNodeID FROM "+DATA_CONFIG+" GROUP BY ParentNodeID ORDER BY Parameter"
        qry=str(qry)
    
        mycursor.execute(qry)
        myresult=mycursor.fetchall()
        
    except:
        return False

    if(len(myresult)==0):
        return False
    else:
        for x in myresult:
            string=convertTuple(x)
            PARENT_ID.append(string)
            
        PARENT_ID=list(dict.fromkeys(PARENT_ID))        # remove duplicates
        return PARENT_ID



def p_name(obj):
    output=str(obj.get_display_name())
    inter=str(output.split("Text:",1)[1])
    parent_name=str(inter.split(")",1)[0])
    parent_name=str(parent_name)
    return parent_name  



def create_parent_table(parent_name):
    try:                                                        # if table doesnt exist
        mydb=MYSQL_list[0]
        mycursor=mydb.cursor()
        temp="(Timestamp VARCHAR(255) NOT NULL, Parameter VARCHAR(255) NOT NULL, Value VARCHAR(255))"
    
        table="CREATE TABLE "+parent_name+temp

        table=str(table)
        mycursor.execute(table)
        print("\nNew Table created\n")
    
    except:                                                     # table already exists
        print("\nTable already exists\n")




        
def convert_to_senml(para_name,value,timestamp):

    d='[{"n":"'+str(para_name)+'","ut":'+str(timestamp)+',"v":'+str(value)+'}]'  
    d=str(d)
    return d       





def mqtt_publish(senml_data,thing_id,thing_key,channel_id):
    mqtopic="channels/"+channel_id+"/messages"
    mqtopic=str(mqtopic)

    print('MQtopic: ',mqtopic)
    print(senml_data)

    ip=MAINFLUX_list[0]
    ip=str(ip)

    payload=senml_data
    
    x=MQTT_list[0]

    x.username_pw_set(username=thing_id,password=thing_key)
    x.connect(ip,port=1883)
    x.loop_start()
    x.publish(mqtopic,payload,qos=2)
    x.loop_stop()
    print('DATA PUBLISHED')
                                              




# to get all nodeids and parameters for each parent id and create a table for each parent and insert a row of data into it.
def PUSH():
    while True:
        ALL_PARENT_ID=get_all_parents()
        
        if(ALL_PARENT_ID==False):
            print('DATA_CONFIG_TABLE is empty, waiting for data..\n')
            time.sleep(5)
            
        else:
            
            for pid in ALL_PARENT_ID:
            
                client=OPC_list[0]
                opc_parent=client.get_node(str(pid))
                parent_name=p_name(opc_parent)
        
                pid_query="\'"+pid+"\'"
                mydb=MYSQL_list[0]
                mycursor=mydb.cursor()
                qry="SELECT NodeID, Parameter, ThingID, ThingKey, ChannelID FROM "+DATA_CONFIG+" WHERE ParentNodeID= "+str(pid_query)
                qry=str(qry)
    
                mycursor.execute(qry)
                myresult=mycursor.fetchall()

                create_parent_table(parent_name)

                # get timestamp string
                ts=time.time()
                timestamp=datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
                timestamp=str(timestamp)
            
                date_time_str=timestamp
                date_time_obj=datetime.datetime.strptime(date_time_str, '%Y-%m-%d %H:%M:%S')
                
                sen_ts=time.mktime(date_time_obj.timetuple())
                sen_ts=int(sen_ts)
            

                for x in myresult: 
                    nid=str(x[0])
                    para_name=str(x[1])
                
                    a=client.get_node(nid)
                    value=a.get_value()         # getting value from opc ua node

                    sql="INSERT INTO "+parent_name+" (Timestamp, Parameter, Value) VALUES(%s,%s,%s)"
                    val=(timestamp,para_name,value)
                
                    mydb=MYSQL_list[0]
                    mycursor=mydb.cursor()
                    mycursor.execute(sql,val)
                    mydb.commit()
                    print('\nrecord inserted')
            
                    thing_id=str(x[2])
                    thing_key=str(x[3])
                    channel_id=str(x[4])
                  
                    senml_data=convert_to_senml(para_name,value,sen_ts)
                    time.sleep(1)
                    mqtt_publish(senml_data,thing_id,thing_key,channel_id)

            # time.sleep(5)
                
  
    
       


# main function
if __name__ == "__main__":
    while True:
        check1=startup()
        a=get_all_parents()
        check2=False
        
        if(a==False):
            check2=False
        elif(len(a)!=0):
            check2=True
            
        if(check1==True and check2==True):
            break
        
    print('STARTING PUSH TO MQTT:\n')
    PUSH()












