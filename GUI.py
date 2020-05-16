import PySimpleGUI as sg
from opcua.common import events
import opcua.common
import string
import os
import time
import json
from opcua import ua
from opcua import Client
import sys


sg.ChangeLookAndFeel('Dark')
sg.SetOptions(element_padding=(5,10))


queue=list()


def no_config():
    sg.PopupOK('No previous configuration settings found. Please enter the required credentials.')
    

def config_error():
    sg.PopupError('Error - could not load configuration settings.')


def opc_pop():
    sg.PopupOK('Connected to OPC UA Server')
    

def opc_error():
    sg.PopupError('Error - could not connect to OPC UA Server')
    
    
def sql_pop1():
    sg.PopupOK('Connected to MySQL Server Database')
    
    
def sql_pop2():
    sg.PopupOK('New database created. Connected to MySQL Server Database')
    

def sql_error():
    sg.PopupError('Error - could not connect to MySQL Server')
       

def server_empty():
    sg.PopupError('Error - the server is empty')


def option_error():
    sg.PopupError('Error - Please choose an option')
    

def no_children():
    sg.PopupOK('Node has no children')
    

def traversal_done():
    sg.PopupOK('Node traversal successful')


def no_push():
    sg.PopupError('Error - no nodes selected to push to MQTT')
    

def flux_pop():
    sg.PopupOK('Connected to Mainflux')
    

def flux_error():
    sg.PopupError('Error - could not connect to Mainflux')
    
    
def table_empty():
    sg.PopupOK('No records present in DATA_CONFIG_TABLE')

def device_deleted():
    sg.PopupOK('Device/s deleted')

def para_deleted():
    sg.PopupOK('Parameter/s deleted')

def no_device():
    sg.PopupError('Error - no devices selected')

def no_parameter():
    sg.PopupError('Error - no parameters selected')
    

def exit():
    exitlayout=[
                [sg.Text('Are you sure you want to close the application?')],
                [sg.Button(button_text="Yes"),sg.Button(button_text="No")]]

    exitwindow=sg.Window('OPC UA Client for MQTT',exitlayout).Finalize()
    button,values=exitwindow.Read()
    exitwindow.Close()
    if(button=='Yes'):
        sys.exit(0)

    

def config_option():
    layout=[
           [sg.Text('Previous configuration settings will be loaded in'),sg.Text('',key='text')],
           [sg.Text('Click on the button below to setup new configuration settings')],
           [sg.Button(button_text="New setup")]]

    window=sg.Window('Configuration Settings',layout).Finalize()

    current_time=0
    paused=False
    start_time=int(round(time.time()*100))

    while True:
        
        button,values=window.Read(timeout=10)
        current_time=int(round(time.time()*100))-start_time
        
        if(((current_time//100)%60)==5):
            window.Close()
            return False
        
        if(button=="New setup"):
            window.Close()
            return True
        
        if(button==None):
            window.Close()
            sys.exit(0)
        window.FindElement('text').Update('{}'.format(5-(current_time//100)%60))
        






def opc_initialise():
    opclayout= [
              [sg.Text('Please enter OPC UA Server details')],
              [sg.Text('Server Name ', size=(25, 1)), sg.InputText()],
              [sg.Text('Endpoint URL (opc.tcp URL)', size=(25, 1)), sg.InputText()],
              [sg.Submit()]]

    opcwindow= sg.Window('OPC UA Server',opclayout).Finalize()
    button, values = opcwindow.Read()
    opcwindow.Close()
    if(button==None):
        sys.exit(0)
    else:
        return values



def sql_initialise():
    sqllayout=[
            [sg.Text('Please enter MySQL Server details')],
            [sg.Text('Host',size=(25,1)),sg.InputText()],  
            [sg.Text('Username',size=(25,1)),sg.InputText()],
            [sg.Text('Password',size=(25,1)),sg.InputText()],
            [sg.Text('Database name',size=(25,1)),sg.Input()],   
            [sg.Submit()]]

    sqlwindow=sg.Window('MySQL Server',sqllayout).Finalize()
    button, values = sqlwindow.Read()
    sqlwindow.Close()
    if(button==None):
        sys.exit(0)
    else:
        return values




def flux_initialise():
    fluxlayout=[
                [sg.Text('Please enter Mainflux details')],
                [sg.Text('IP address',size=(25,1)),sg.InputText()],
                [sg.Text('Port',size=(25,1)),sg.InputText()],
                [sg.Text('Email',size=(25,1)),sg.InputText()],
                [sg.Text('Password',size=(25,1)),sg.InputText()],
                [sg.Submit()]]

    fluxwindow=sg.Window('Mainflux',fluxlayout).Finalize()
    button,values=fluxwindow.Read()
    fluxwindow.Close()

    if(button==None):
        sys.exit(0)
    else:
        return values



def main_menu():
    mainlayout=[[sg.Text('Choose option')],
                [sg.Radio(' Display Current Node','mainmenu')],
                [sg.Radio(' Traverse / Push to MQTT','mainmenu')],
                [sg.Radio('Traverse entire tree','mainmenu')],
                [sg.Radio('Delete record','mainmenu')],
                [sg.Radio(' Exit','mainmenu')],
                [sg.Submit()]]


    mainwindow=sg.Window('Main Menu',mainlayout,size=(400,400)).Finalize()
    button,values=mainwindow.Read()
    mainwindow.Close()
    if(button==None):
        sys.exit(0)
    else:
        return values



def current_node(node_id,ntype,disp_name,nidx,value,dtype):
    if(value==None):
        val="N/A"
    else:
        val=value
            
    if(dtype==None):
        datatype="N/A"
    else:
        datatype=dtype

    currentlayout=[
                    [sg.Text('Details: ')],
                    [sg.Text('Node ID: {}'.format(node_id))],
                    [sg.Text('Node ID Type: {}'.format(ntype))],
                    [sg.Text('Display Name: {}'.format(disp_name))],
                    [sg.Text('Namespace Index: {}'.format(nidx))],
                    [sg.Text('Value: {}'.format(val))],
                    [sg.Text('Datatype: {}'.format(datatype))],
                    [sg.OK()]]
    
    currentwindow=sg.Window('Current Node Details',currentlayout,size=(400,400)).Finalize()
    button,values=currentwindow.Read()
    currentwindow.Close()    




def trav_or_push():
    layout=[
            [sg.Text('Choose operation to perform')],
            [sg.Radio('Traverse','operation')],
            [sg.Radio('Push to MQTT','operation')],
            [sg.Submit()]]

    window=sg.Window('Traverse / Push to MQTT',layout)
    button,values=window.Read()
    window.Close()
    return (button,values)


def path_node(z):
    i=0
    a=list()
    a=z.get_path()
    path_str="("
    
    while(i<len(a)):
        z=a[i]
        
        output=str(z.get_display_name())
        inter=str(output.split("Text:",1)[1])
        disp_name=str(inter.split(")",1)[0])
        if(i==len(a)-1):
            path_str=path_str+disp_name
            path_str=str(path_str)
        else:
            path_str=path_str+disp_name+'\\'
        i=i+1
    path_str=path_str+")"
    # print("Path string: ",path_str)
    return path_str




# returns node ID and display name of a node
def extra(z):
        path=str(path_node(z))
    
        output2=str(z.get_display_name())                                       # Display Name string
        inter=str(output2.split("Text:",1)[1])
        disp_name=str(inter.split(")",1)[0])
        disp_name=str(disp_name)
        
        return (disp_name,path)
        



def traversal_only(c,n,opclist):
    col=[]
    i=0
    x=opclist[0]                                        # important/opclist contains client object/needed to obtain other nodes

    output=str(c.get_display_name())
    inter=str(output.split("Text:",1)[1])
    node_name=str(inter.split(")",1)[0])

    
    while(i<n):
        temp=x.get_node(c.get_children()[i])
        name,path=extra(temp)
        string1=name+"   "+path
        col.append([sg.Radio(string1,'traverse')])              
        i=i+1
         
    layout=[
            [sg.Text('Which node do you want to traverse? ')],
            [sg.Text('Children of '+str(node_name))],
            [sg.Column(col,size=(500,500),scrollable=True)],                                                                              
            [sg.Submit()]]
    
    window=sg.Window('Node Traversal',layout).Finalize()
    button,values=window.Read()
    window.Close()
    return (button,values)




inter=list()

def push_screen(chunk,next_page):
    i=0
    col=[]
    
    while(i<len(chunk)):
        temp=chunk[i]
        name,path=extra(temp)
        string1=name+"   "+path
        col.append([sg.Checkbox(string1,key=i)])
        i=i+1
    

    if(next_page==True):
        layout=[
                [sg.Text('Please select the nodes to send to the MQTT Broker')],
                [sg.Button('Select All'),sg.Button('Deselect All')],
                *col,                                                              
                [sg.Button(button_text='Submit'),sg.Button(button_text='Next Page'),sg.Button(button_text='Confirm')]]

    elif(next_page==False):
        layout=[
                [sg.Text('Please select the nodes to send to the MQTT Broker')],
                [sg.Button('Select All'),sg.Button('Deselect All')],
                *col,                                                              
                [sg.Button(button_text='Submit'),sg.Button(button_text='Confirm')]]
        

    window=sg.Window('Push to MQTT',layout).Finalize()

    
    while True:
        button,values=window.Read()
        if(button=='Select All'):
            for x in range(0,len(chunk)):
                window.FindElement(x).Update(True)
                
        elif(button=='Deselect All'):
            for x in range(0,len(chunk)):
                window.FindElement(x).Update(False)
                
        elif(button=='Submit'):
            check=0
            for num,val in values.items():
                if(val==True):
                    op=num
                    inter.append(chunk[op])
                elif(val==False):
                    check=check+1
            if(check==len(chunk)):
                    window.Hide()
                    option_error()
                    window.UnHide()
                    
        elif(button=='Next Page'):
            window.Close()
            return True

            
        elif(button=='Confirm'):
            window.Close()
            pushlist=list(dict.fromkeys(inter)) # to remove duplicates
            inter.clear()                       # clear the list
            return pushlist

        elif(button==None):
            break




def device_or_para():
    layout=[
            [sg.Text('Choose operation to perform')],
            [sg.Radio('Delete device/s','operation')],
            [sg.Radio('Delete parameter/s','operation')],
            [sg.Submit()]]

    window=sg.Window('Delete records',layout)
    button,values=window.Read()
    window.Close()
    return (button,values)





def delete_device_screen(chunk,next_page):
    i=0
    col=[]
    
    while(i<len(chunk)):
        temp=chunk[i]
        name,path=extra(temp)
        string1=name+"   "+path
        col.append([sg.Checkbox(string1,key=i)])
        i=i+1
    

    if(next_page==True):
        layout=[
                [sg.Text('Please select the devices to be deleted')],
                [sg.Button('Select All'),sg.Button('Deselect All')],
                *col,                                                              
                [sg.Button(button_text='Submit'),sg.Button(button_text='Next Page'),sg.Button(button_text='Confirm')]]

    elif(next_page==False):
        layout=[
                [sg.Text('Please select the devices to be deleted')],
                [sg.Button('Select All'),sg.Button('Deselect All')],
                *col,                                                              
                [sg.Button(button_text='Submit'),sg.Button(button_text='Confirm')]]
        

    window=sg.Window('Delete devices',layout).Finalize()

    
    while True:
        button,values=window.Read()
        if(button=='Select All'):
            for x in range(0,len(chunk)):
                window.FindElement(x).Update(True)
                
        elif(button=='Deselect All'):
            for x in range(0,len(chunk)):
                window.FindElement(x).Update(False)
                
        elif(button=='Submit'):
            check=0
            for num,val in values.items():
                if(val==True):
                    op=num
                    inter.append(chunk[op])
                elif(val==False):
                    check=check+1
            if(check==len(chunk)):
                    window.Hide()
                    option_error()
                    window.UnHide()
                    
        elif(button=='Next Page'):
            window.Close()
            return True

            
        elif(button=='Confirm'):
            window.Close()
            pushlist=list(dict.fromkeys(inter)) # to remove duplicates
            inter.clear()                       # clear the list
            return pushlist

        elif(button==None):
            break




def delete_parameter_screen(chunk,next_page):
    i=0
    col=[]
    
    while(i<len(chunk)):
        temp=chunk[i]
        name,path=extra(temp)
        string1=name+"   "+path
        col.append([sg.Checkbox(string1,key=i)])
        i=i+1
    

    if(next_page==True):
        layout=[
                [sg.Text('Please select the parameters to be deleted')],
                [sg.Button('Select All'),sg.Button('Deselect All')],
                *col,                                                              
                [sg.Button(button_text='Submit'),sg.Button(button_text='Next Page'),sg.Button(button_text='Confirm')]]

    elif(next_page==False):
        layout=[
                [sg.Text('Please select the parameters to be deleted')],
                [sg.Button('Select All'),sg.Button('Deselect All')],
                *col,                                                              
                [sg.Button(button_text='Submit'),sg.Button(button_text='Confirm')]]
        

    window=sg.Window('Delete parameters',layout).Finalize()

    
    while True:
        button,values=window.Read()
        if(button=='Select All'):
            for x in range(0,len(chunk)):
                window.FindElement(x).Update(True)
                
        elif(button=='Deselect All'):
            for x in range(0,len(chunk)):
                window.FindElement(x).Update(False)
                
        elif(button=='Submit'):
            check=0
            for num,val in values.items():
                if(val==True):
                    op=num
                    inter.append(chunk[op])
                elif(val==False):
                    check=check+1
            if(check==len(chunk)):
                    window.Hide()
                    option_error()
                    window.UnHide()
                    
        elif(button=='Next Page'):
            window.Close()
            return True

            
        elif(button=='Confirm'):
            window.Close()
            pushlist=list(dict.fromkeys(inter)) # to remove duplicates
            inter.clear()                       # clear the list
            return pushlist

        elif(button==None):
            break

        
            
        
        







