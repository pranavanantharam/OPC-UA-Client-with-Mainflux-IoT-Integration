import requests
import csv

initialrow=['NodeID','ParentNodeID','Parameter','ThingID','ThingKey','ChannelID']
csv_name="DATA_CONFIG_TABLE.csv"


dblist=list()
fluxlist=list()
opcdata=list()


def get_opc(x):
    opcdata.append(x)
    
    
def get_db(x):
    dblist.append(x)
    

def get_token(ip,port,email,pwd):
    fluxlist.append(ip)
    fluxlist.append(port)
    url='http://'+ip+':'+port+'/tokens'
    url=str(url)
    details={"email":email,"password":pwd}
    
    r=requests.post(url,json=details)
    if r.status_code != 201:
        raise ApiError('POST /tokens {}'.format(r.status_code))
    file=r.json()
    x=file['token']
    x=str(x)

    fluxlist.append(x)
    return x


# function to write first row into the csv file
def start_csv():
    with open(csv_name,'w') as csvFile:
        writer=csv.writer(csvFile)
        writer.writerow(initialrow)
    csvFile.close()
    

# function to append a row into csv file
def write_csv(nid,pid,param,thid,thkey,chid):
        row=[nid,pid,param,thid,thkey,chid]
        with open(csv_name,'a') as csvFile:
                writer=csv.writer(csvFile)
                writer.writerow(row)
        csvFile.close()



def get_nodeid(a):
    output1=str(a.get_attribute(1))                                         # Node ID string
    inter=str(output1.split("NodeId(",1)[1])
    nid=str(inter.split(")",1)[0])
    if("ns=" not in nid):
        nid=";"+nid

    return nid



def get_parentid(a):
    x=opcdata[0]
    temp=x.get_node(a.get_parent())
    output1=str(temp.get_attribute(1))                                      # Node ID string
    inter=str(output1.split("NodeId(",1)[1])
    p_id=str(inter.split(")",1)[0])
    if("ns=" not in p_id):
        p_id=";"+p_id

    return p_id



def get_CTname(a):
    x=opcdata[0]
    temp=x.get_node(a.get_parent())
    output1=str(temp.get_attribute(1))                                      
    inter=str(output1.split("NodeId(",1)[1])
    p_id=str(inter.split(")",1)[0])
    if("ns=" not in p_id):
        p_id=";"+p_id

    output2=str(temp.get_display_name())
    inter2=str(output2.split("Text:",1)[1])
    p_name=str(inter2.split(")",1)[0])
    p_name=str(p_name)
    
    CTname=str(p_name)+" ("+str(p_id)+")"
    CTname=str(CTname)
    return CTname


def disp_name(a):
    output1=str(a.get_display_name())
    inter=str(output1.split("Text:",1)[1])
    display_name=str(inter.split(")",1)[0])
    display_name=str(display_name)

    return display_name

    


def parameter_name(a):
    x=opcdata[0]
    output1=str(a.get_display_name())
    inter=str(output1.split("Text:",1)[1])
    post=str(inter.split(")",1)[0])

    temp=x.get_node(a.get_parent())
    output2=str(temp.get_display_name())
    inter2=str(output2.split("Text:",1)[1])
    pre=str(inter2.split(")",1)[0])

    para=str(pre)+"_"+str(post)
    return para    





def new_channel(ct_name):                  # create a channel and return its channel ID
    ip=str(fluxlist[0])
    port=str(fluxlist[1])
    token=str(fluxlist[2])

    url='http://'+ip+':'+port+'/channels'
    url=str(url)
    
    cname=str(ct_name)
    
    details={"name":cname}
    
    r=requests.post(url,headers={'Authorization':token,'Content-Type':'application/json'},json=details)
    if r.status_code!= 201:
        raise ApiError('POST /channels {}'.format(r.status_code))
    
    x=r.headers
    data=str(x)
    inter=str(data.split("/channels/",1)[1])
    channel_id=str(inter.split("',",1)[0])
    channel_id=str(channel_id)

    print('Channel ID: ',channel_id)
    return channel_id




def new_thingid(ct_name):               # create a thing and return its thingID and thingKey
    ip=str(fluxlist[0])
    port=str(fluxlist[1])
    token=str(fluxlist[2])
    url='http://'+ip+':'+port+'/things'
    url=str(url)

    thingname=str(ct_name)
    
    thingtype='device'
    details={"type":thingtype,"name":thingname}

    r=requests.post(url,headers={'Authorization':token,'Content-Type':'application/json'},json=details)
    if r.status_code != 201:
        raise ApiError('POST /things {}'.format(r.status_code))

    x=r.headers
    data=str(x)
    inter=str(data.split("/things/",1)[1])
    inter_id=str(inter.split("',",1)[0])
    inter_id=str(inter_id)

    print('Thing ID: ',inter_id)
    return inter_id



def new_thingkey(thid):
    
    ip=str(fluxlist[0])
    port=str(fluxlist[1])
    token=str(fluxlist[2])
    url='http://'+ip+':'+port+'/things/'+thid
    url=str(url)

    r=requests.get(url,headers={'Authorization':token,'Content-Type':'application/json'})
    if r.status_code != 200:
        raise ApiError('GET /things {}'.format(r.status_code))
    
    thing=r.json()

    key=thing['key']
    key=str(key)
    
    print('Thing key: ',key)
    return key



def add_thing_to_channel(chid,thid):
    ip=str(fluxlist[0])
    port=str(fluxlist[1])
    token=str(fluxlist[2])
    url='http://'+ip+':'+port+'/channels/'+chid+'/things/'+thid
    url=str(url)

    r=requests.put(url,headers={'Authorization':token,'Content-Type':'application/json'})
    if r.status_code!=200:
        raise ApiError('PUT /channels/things {}'.format(r.status_code))

    

def remove_thing_to_channel(chid,thid):
    ip=str(fluxlist[0])
    port=str(fluxlist[1])
    token=str(fluxlist[2])
    url='http://'+ip+':'+port+'/channels/'+chid+'/things/'+thid
    url=str(url)

    r=requests.delete(url,headers={'Authorization':token})
    if r.status_code!=204:
        raise ApiError('DELETE /channels/things {}'.format(r.status_code))
    print("Removed thing to channel")


def delete_channel(chid):
    ip=str(fluxlist[0])
    port=str(fluxlist[1])
    token=str(fluxlist[2])
    url='http://'+ip+':'+port+'/channels/'+chid
    url=str(url)

    r=requests.delete(url,headers={'Authorization':token})
    if r.status_code!=204:
        raise ApiError('DELETE /channels {}'.format(r.status_code))
    print("Deleted channel")



def delete_thing(thid):
    ip=str(fluxlist[0])
    port=str(fluxlist[1])
    token=str(fluxlist[2])
    url='http://'+ip+':'+port+'/things/'+thid
    url=str(url)
    
    r=requests.delete(url,headers={'Authorization':token})
    if r.status_code!=204:
        raise ApiError('DELETE /things {}'.format(r.status_code))
    print("Deleted thing")

  

TEMP=list()

def maindb(select_node,tablename):
    
    mydb=dblist[0]
    mycursor=mydb.cursor()
    qry="SELECT * FROM "+tablename
    qry=str(qry)
    
    mycursor.execute(qry)
    myresult=mycursor.fetchall()

    
    n=len(myresult)             # no. of records in sql table                    
    
    if(n==0):
        
        pid=get_parentid(select_node)
        nid=get_nodeid(select_node)
        parameter=parameter_name(select_node)

        ct_name=get_CTname(select_node)
        
        chid=new_channel(ct_name)
        thid=new_thingid(ct_name)
        thkey=new_thingkey(thid)

        add_thing_to_channel(chid,thid)
        print('Added thing to channel')
        
        TEMP.append(pid)
        TEMP.append(thid)
        TEMP.append(thkey)
        TEMP.append(chid)

                    
        sql="INSERT INTO "+tablename+" (NodeID, ParentNodeID, Parameter, ThingID, ThingKey, ChannelID) VALUES(%s,%s,%s,%s,%s,%s)"
        val=(nid,pid,parameter,thid,thkey,chid)
    
        mycursor=mydb.cursor()
        mycursor.execute(sql,val)
        mydb.commit()
        
        write_csv(nid,pid,parameter,thid,thkey,chid)
        print('record inserted')
        
        return True


        
    else:
        i=0
        node_check=0
        parent_check=0
        
        while(i<n):
            row=myresult[i]
            
            if(get_nodeid(select_node)==str(row[0])):
                node_check+=1
                
            if(get_parentid(select_node)==str(row[1])):
                parent_check+=1        
            i=i+1

        if(node_check>0):
            node_status=True
        else:
            node_status=False
            
        if(parent_check>0):
            parent_status=True
        else:
            parent_status=False

        
        if(node_status==True):                          # return to function and send next leaf node
            return True
            
        elif(node_status==False and parent_status==True):
            nid=get_nodeid(select_node)
            pid=TEMP[0]
            parameter=parameter_name(select_node)
            thid=TEMP[1]
            thkey=TEMP[2]
            chid=TEMP[3]

            sql="INSERT INTO "+tablename+" (NodeID, ParentNodeID, Parameter, ThingID, ThingKey, ChannelID) VALUES(%s,%s,%s,%s,%s,%s)"
            val=(nid,pid,parameter,thid,thkey,chid)
    
            mycursor=mydb.cursor()
            mycursor.execute(sql,val)
            mydb.commit()
            
            print('record inserted')
            write_csv(nid,pid,parameter,thid,thkey,chid)
            return True


            
        elif(node_status==False and parent_status==False):
            TEMP.clear()
        
            pid=get_parentid(select_node)
            nid=get_nodeid(select_node)
            parameter=parameter_name(select_node)

            ct_name=get_CTname(select_node)
            
            chid=new_channel(ct_name)
            thid=new_thingid(ct_name)
            thkey=new_thingkey(thid)

            add_thing_to_channel(chid,thid)
            print('Added thing to channel')
            
            TEMP.append(pid)
            TEMP.append(thid)
            TEMP.append(thkey)
            TEMP.append(chid)
                    
            sql="INSERT INTO "+tablename+" (NodeID, ParentNodeID, Parameter, ThingID, ThingKey, ChannelID) VALUES(%s,%s,%s,%s,%s,%s)"
            val=(nid,pid,parameter,thid,thkey,chid)
            
            mycursor=mydb.cursor()
            mycursor.execute(sql,val)
            mydb.commit()
            
            write_csv(nid,pid,parameter,thid,thkey,chid)
            print('record inserted')
            return True





def delete_parameter_row(parameter_nid,tablename):
    mydb=dblist[0]
    mycursor=mydb.cursor()
    sql="DELETE FROM "+tablename+" WHERE NodeID= %s"
    val=(parameter_nid,)
    mycursor.execute(sql,val)
    mydb.commit()

    new_rows=list()
    
    with open("DATA_CONFIG_TABLE.csv", 'r') as csvfile:
        csvReader = csv.reader(csvfile)
        for row in csvReader:
            if(len(row)!=0):
                if(row[0]!=parameter_nid):
                    new_rows.append(row)
        
            
    with open("DATA_CONFIG_TABLE.csv",'w') as csvFile:
        for row in new_rows:
            writer=csv.writer(csvFile)
            writer.writerow(row)
        csvFile.close()




 
def delete_device_row(device_nid,tablename):
    mydb=dblist[0]
    mycursor=mydb.cursor()
    sql="DELETE FROM "+tablename+" WHERE ParentNodeID= %s"
    val=(device_nid,)
    mycursor.execute(sql,val)
    mydb.commit()

    new_rows=list()
    
    with open("DATA_CONFIG_TABLE.csv", 'r') as csvfile:
        csvReader = csv.reader(csvfile)
        for row in csvReader:
            if(len(row)!=0):
                if(row[1]!=device_nid):
                    new_rows.append(row)
        
            
    with open("DATA_CONFIG_TABLE.csv",'w') as csvFile:
        for row in new_rows:
            writer=csv.writer(csvFile)
            writer.writerow(row)
        csvFile.close()
    

    

        
def delete_device(select_node,tablename):
    device_nid=str(get_nodeid(select_node))
    mydb=dblist[0]
    mycursor=mydb.cursor()
    
    pid_query="\'"+str(device_nid)+"\'"
    qry="SELECT ThingID,ChannelID FROM "+tablename+" WHERE ParentNodeID= "+str(pid_query)+" ORDER BY Parameter"
    qry=str(qry)
    
    mycursor.execute(qry)
    myresult=mycursor.fetchall()

    temp=myresult[0]
    thid=str(temp[0])
    chid=str(temp[1])

    remove_thing_to_channel(chid,thid)
    delete_channel(chid)
    delete_thing(thid)

    delete_device_row(device_nid,tablename)
    print('\nDevice deleted\n')

   


def delete_parameter(select_node,tablename):
    
    parameter_nid=str(get_nodeid(select_node))
    parent_id=str(get_parentid(select_node))
    
    mydb=dblist[0]
    mycursor=mydb.cursor()
    
    pid_query="\'"+str(parent_id)+"\'"
    qry="SELECT ThingID,ChannelID FROM "+tablename+" WHERE ParentNodeID= "+str(pid_query)+" ORDER BY Parameter"
    qry=str(qry)
    
    mycursor.execute(qry)
    myresult=mycursor.fetchall()

    
    if(len(myresult)==1):
        row=myresult[0]
        thid=str(row[0])
        chid=str(row[1])        
        remove_thing_to_channel(chid,thid)
        delete_channel(chid)
        delete_thing(thid)

    delete_parameter_row(parameter_nid,tablename)
    print('\nParameter deleted\n') 

    




