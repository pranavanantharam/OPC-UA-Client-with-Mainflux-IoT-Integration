import logging
logging.basicConfig(level=logging.ERROR)
from datetime import datetime
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

import GUI
import DB


opclist=list()
MYSQL_list=list()

ALL_NODES=list()                # stores only nodes with data

types=['BOOLEAN','BYTE_ARRAY','INTEGER','FLOAT','DOUBLE','LONG','STRING']




def config_setup():

    data={}
    configdict=dict()
    
    # OPC UA Initialisation
    while True:
        opcdata=GUI.opc_initialise()
        a=list()
        for num,val in opcdata.items():
            a.append(val)
        
        OPCname=str(a[0])
        OPCurl=str(a[1])
    
        client=Client(OPCurl)
        try:
            
            # Authentication requires copy of certificate and private key in local system.
            # need to add full path of private key and certificate file as parameters (not available at the moment)
            # client.set_security_string(security_algorithm , authentication_mode , path_of_certificate , path_of_private_key)) -> syntax
            # client.set_security_string("Basic256Sha256,SignAndEncrypt,server_cert.der,server_key.pem")  # need to add cert and key path
            
            client.connect()
            opclist.append(client)
            DB.get_opc(client)
            
            GUI.opc_pop()
            
            configdict.update({"OPC Server Name":OPCname})
            configdict.update({"OPC Server URL":OPCurl})
            break

        except:
            # Connecting to OPC UA Server without authentication
            try:
                client.connect()
                opclist.append(client)
                DB.get_opc(client)
                
                GUI.opc_pop()
            
                configdict.update({"OPC Server Name":OPCname})
                configdict.update({"OPC Server URL":OPCurl})
                break
        
            except:
                GUI.opc_error()



    # MySQL Initialisation

    while True:
        sqldata=GUI.sql_initialise()
        a=list()
        for num,val in sqldata.items():
           a.append(val)
       
        sqlhost=str(a[0])
        sqluser=str(a[1])
        sqlpwd=str(a[2])
        dbname=str(a[3])
    
        try:
            mydb=mysql.connector.connect(host=sqlhost,user=sqluser,passwd=sqlpwd,database=dbname)
            mycursor=mydb.cursor()
            GUI.sql_pop1()

            configdict.update({"SQL Host":sqlhost})
            configdict.update({"SQL Username":sqluser})
            configdict.update({"SQL Password":sqlpwd})
            configdict.update({"Database Name":dbname})

            DB.get_db(mydb)
            MYSQL_list.append(mydb)

            break
    
        except:
            try:
                mydb=mysql.connector.connect(host=sqlhost,user=sqluser,passwd=sqlpwd)
                mycursor=mydb.cursor()
                db="CREATE DATABASE "+dbname
                db=str(db)
                mycursor.execute(db)
            
                mydb=mysql.connector.connect(host=sqlhost,user=sqluser,passwd=sqlpwd,database=dbname)
                mycursor=mydb.cursor()
                GUI.sql_pop2()

                configdict.update({"SQL Host":sqlhost})
                configdict.update({"SQL Username":sqluser})
                configdict.update({"SQL Password":sqlpwd})
                configdict.update({"Database Name":dbname})

                DB.get_db(mydb)
                MYSQL_list.append(mydb)
                break
        
            except:
                GUI.sql_error()



    # Mainflux API initialization
    while True:

        mainflux_data=GUI.flux_initialise()
        a=list()
        for num,val in mainflux_data.items():
            a.append(val)
            
        flux_ip=str(a[0])
        flux_port=str(a[1])
        flux_email=str(a[2])
        flux_pwd=str(a[3])

        try:
            token=DB.get_token(flux_ip,flux_port,flux_email,flux_pwd)

            GUI.flux_pop()

            configdict.update({"Mainflux IP":flux_ip})
            configdict.update({"Mainflux Port":flux_port})
            configdict.update({"Mainflux Email":flux_email})
            configdict.update({"Mainflux Password":flux_pwd})
            break

        except:
            GUI.flux_error()
            
            
    data['Settings']=[]
    data['Settings'].append(configdict)

    dir_path = os.path.dirname(os.path.realpath(__file__))
    dir_path=str(dir_path)
    configpath=dir_path+"\config.json"
    configpath=str(configpath)
    
    with open(configpath,'w') as f:
        json.dump(data,f,indent=4)
        
        



# Configuration File Initialisation

configlist=list()
dir_path = os.path.dirname(os.path.realpath(__file__))
dir_path=str(dir_path)


for file in os.listdir(dir_path):
    if (file=="config.json"):
        configlist.append(os.path.join(dir_path, file))
        break
    else:
        configpath=dir_path+"\config.json"
        configpath=str(configpath)
      

if(len(configlist)==0):                     # config.json doesnt exist
    GUI.no_config()
    config_setup()

else:                                       # config.json exists

    option=GUI.config_option()
    configpath=configlist[0]

    if(option==False):

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
            opclist.append(client)
            DB.get_opc(client)

            mydb=mysql.connector.connect(host=sqlhost,user=sqluser,passwd=sqlpwd,database=dbname)
            mycursor=mydb.cursor()
            
            MYSQL_list.append(mydb)
            DB.get_db(mydb)
            
            token=DB.get_token(flux_ip,flux_port,flux_email,flux_pwd)


        except:
            GUI.config_error()
            sys.exit(0)
            
    elif(option==True):
        os.remove(configpath)                   # delete old config.json
        config_setup()
        


def create_table():
    tablename="DATA_CONFIG_TABLE"
    tablename=str(tablename)
    mydb=MYSQL_list[0]
    
    try:                                                        # if table doesnt exist
        mycursor=mydb.cursor()
        temp=("(NodeID VARCHAR(255) PRIMARY KEY NOT NULL, ParentNodeID VARCHAR(255) NOT NULL,"
            " Parameter VARCHAR(255) NOT NULL , ThingID VARCHAR(255) NOT NULL,"
            " ThingKey VARCHAR(255) NOT NULL, ChannelID VARCHAR(255) NOT NULL)")             #, Path VARCHAR(255))")
    
        table="CREATE TABLE "+tablename+temp

        table=str(table)
        mycursor.execute(table)
        print("\nNew Table created\n")
    
    
    except:                                                     # table already exists
        print("\nTable already exists\n")
        
    return tablename
    


                


# function to get all details of a node
def get_details(a):
        output1=str(a.get_attribute(1))                                         # Node ID string
        inter=str(output1.split("NodeId(",1)[1])
        node_id=str(inter.split(")",1)[0])
        if("ns=" not in node_id):
                node_id=";"+node_id

        if("i=" in output1):
                ntype="NUMERIC" 
        elif("s=" in output1):
                ntype="STRING"
        elif("g=" in output1):
                ntype="GUID"
        elif("b=" in output1):
                ntype="OPAQUE"  

        output2=str(a.get_display_name())                               # Display Name string
        inter=str(output2.split("Text:",1)[1])
        disp_name=str(inter.split(")",1)[0])


        output3=str(a.get_attribute(1))                         # Namespace Index string
        if("ns=" in output3):
                inter=str(output3.split("ns=",1)[1])
                nidx=int(inter.split(";",1)[0])
        else:
                nidx=0

                
        try:
                output4=str(a.get_data_type_as_variant_type())  # DataType string- Did not account for custom datatypes;
                                                                #  if any custom datatypes encountered, dtype="STRING"  
                inter1=str(output4.split("VariantType.",1)[1])
                inter2=str(inter1.split(":",1)[0])
                inter2=inter2.upper()
                if(inter2 in types):
                        dtype=inter2
                elif(inter2=='BYTE'):
                        dtype='INTEGER'
                elif("UINT" in inter2):
                        dtype='LONG'            
                else:
                        dtype='STRING'  
                        
                value=a.get_value()
        except:
                value=None
                dtype=None
                
        GUI.current_node(node_id,ntype,disp_name,nidx,value,dtype)



def check_node(b):
    try:
        b.get_value()
        b.get_data_type_as_variant_type()
        return True
    
    except:
        return False

    

def get_nodename(temp):
    output1=str(temp.get_display_name())
    inter=str(output1.split("Text:",1)[1])
    node_name=str(inter.split(")",1)[0])

    return node_name



def full_tree():         # GUI needs to run in the background while this process is running
    ALL_NODES.clear()
    queue=list()
    x=opclist[0]
    var=x.get_root_node()
    queue.append(var)

    while(len(queue)!=0):
        n=len(queue)
        while(n>0):
            temp=queue.pop(0)
            if(check_node(temp)):
                print(temp)
                ALL_NODES.append(temp)
                
            i=0
            if(get_nodename(temp)!='Types' and get_nodename(temp)!='Views' and get_nodename(temp)!='Server'):
                while(i<len(temp.get_children())):
                    var2=x.get_node(temp.get_children()[i])
                    queue.append(var2)
                    i=i+1
            n=n-1

    print("ALL_NODES length: ",len(ALL_NODES))




  

def divide_chunks(l, n):   
    for i in range(0, len(l), n):  
        yield l[i:i + n] 




def trav_push(c):

    x=opclist[0]
   
    num,val=GUI.trav_or_push()
    trav=val[0]
    push=val[1]
        
    if(trav==True and push==False):
        n=len(c.get_children())
        
        if(n>0):
            a=list()
        
            while True:
                traversedata=GUI.traversal_only(c,n,opclist)
                button=traversedata[0]
                values=traversedata[1]
                    
                if(button==None):
                    return c
                else:
                    check=0
                    for num,val in values.items():
                        if(val==True):
                            op=num
                            temp=x.get_node(c.get_children()[op])
                            GUI.traversal_done()
                            return temp
                            
                        elif(val==False):
                            check=check+1
                                
                        if(check==n):
                            GUI.option_error()
                                  
        else:
            GUI.no_children()
         
    elif(trav==False and push==True):
        nodes=list()
        nodes=ALL_NODES
        
        tablename=create_table()

        DB.start_csv()
            
        if(len(nodes)==0):
            var=x.get_root_node()
            full_tree()
            nodes=ALL_NODES
                
        while True:    
            i=0
            send_to_gui=list()
            
            send_to_gui=list(divide_chunks(nodes,5))
            n=len(send_to_gui)
            
            while(i<n):
                chunk=send_to_gui[i]
            
                if(i==n-1):
                    next_page=False
                else:
                    next_page=True
            
                i=i+1
            
                while True:
                    
                    inter=GUI.push_screen(chunk,next_page)
                    dtype=str(type(inter))

                    if('NoneType' in dtype):
                        return c
                    
                    elif('bool' in dtype):          # for next page
                        break

                    elif(len(inter)==0):
                        GUI.no_push()

                    else:
                        for x in inter:
                            DB.maindb(x,tablename)
                                
                        i=0
                        break

    else:
        GUI.option_error()
        return c

           
def convertTuple(tup): 
    string =  ''.join(tup) 
    return string


def delete_record():
    
    tablename="DATA_CONFIG_TABLE"
    tablename=str(tablename)
    
    mydb=MYSQL_list[0]
    mycursor=mydb.cursor()
    qry="SELECT * FROM DATA_CONFIG_TABLE"
    qry=str(qry)

    client=opclist[0]
    
    mycursor.execute(qry)
    myresult=mycursor.fetchall()

    if(len(myresult)==0):
        GUI.table_empty()
        return False
    
    num,val=GUI.device_or_para()
    device=val[0]
    para=val[1]

    if(device==True and para==False):
        
        all_devices=list()
            
        mycursor=mydb.cursor()
        qry="SELECT ParentNodeID FROM DATA_CONFIG_TABLE GROUP BY ParentNodeID ORDER BY Parameter"
        qry=str(qry)
    
        mycursor.execute(qry)
        myresult=mycursor.fetchall()
        
        for x in myresult:
            string=convertTuple(x)
            temp=client.get_node(string)
            all_devices.append(temp)             # all parent nodes IDs stored in all_devices list

        all_devices=list(dict.fromkeys(all_devices))    # remove duplicates
                
        i=0
        send_to_gui=list()
            
        send_to_gui=list(divide_chunks(all_devices,5))
        n=len(send_to_gui)
            
        while(i<n):
            chunk=send_to_gui[i]
            
            if(i==n-1):
                next_page=False
            else:
                next_page=True
            
            i=i+1
            
            while True:
                    
                inter=GUI.delete_device_screen(chunk,next_page)
                dtype=str(type(inter))

                if('NoneType' in dtype):
                    return True
                    
                elif('bool' in dtype):          # for next page
                    break

                elif(len(inter)==0):
                    GUI.no_device()

                else:
                    for x in inter:
                        DB.delete_device(x,tablename)
                            
                    GUI.device_deleted()
                    return True        
            
    
    elif(device==False and para==True):
        
        all_parameters=list()
            
        mycursor=mydb.cursor()
        qry="SELECT NodeID FROM DATA_CONFIG_TABLE ORDER BY Parameter"
        qry=str(qry)
    
        mycursor.execute(qry)
        myresult=mycursor.fetchall()

        for x in myresult:
            string=convertTuple(x)
            temp=client.get_node(string)
            all_parameters.append(temp)             # all leaf nodes IDs stored in all_parameters list

        all_parameters=list(dict.fromkeys(all_parameters))    # remove duplicates

        i=0
        send_to_gui=list()
            
        send_to_gui=list(divide_chunks(all_parameters,5))
        n=len(send_to_gui)

        while(i<n):
            chunk=send_to_gui[i]
            
            if(i==n-1):
                next_page=False
            else:
                next_page=True
            
            i=i+1
            
            while True:
                    
                inter=GUI.delete_parameter_screen(chunk,next_page)
                dtype=str(type(inter))

                if('NoneType' in dtype):
                    return True
                    
                elif('bool' in dtype):          # for next page
                    break

                elif(len(inter)==0):
                    GUI.no_parameter()

                else:
                    for x in inter:
                        DB.delete_parameter(x,tablename)
        
                    GUI.para_deleted()
                    return True

    else:
        GUI.option_error()
        return False



            

if __name__ == "__main__":
    x=opclist[0]
    var=x.get_root_node()
    try:
        if(var==None):                                         # No root
            GUI.server_empty()

        else:                                                  # General case                                                  
            while True:
                option=GUI.main_menu()
                check=0
                for ch,val in option.items():
                    if(val==True):
                        op=ch
                    elif(val==False):
                        check=check+1
                    if(check==5):
                        op=5
                        break

                if(op==0):
                    get_details(var)

                elif(op==1):
                    var=trav_push(var)
  
                elif(op==2):
                    full_tree()

                elif(op==3):
                    delete_record()
                        
                elif(op==4):
                    GUI.exit()
                        
                elif(op==5):
                        GUI.option_error()
    
    finally:
        x.disconnect()

