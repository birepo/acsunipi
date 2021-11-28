# Databricks notebook source
import uuid
import asyncio 
import os
import math
import datetime
import time
import utils as u
from azure.servicebus.management import ServiceBusAdministrationClient
test = 'test git'

# COMMAND ----------

dbutils.widgets.text("processID", "", "processID")
dbutils.widgets.text("GUID", "", "GUID")
ProcToElab = dbutils.widgets.get("processID")
GuidForLog = dbutils.widgets.get("GUID")

# COMMAND ----------

# RECUPERO DATI PER IL LA GESTIONE DEL FLUSSO 
# DATI INTERESSATI
#  Iid_RecordID 
#  Cod_ProcessID
#  Cod_SourceCode
#  Cod_SourceType
#  Des_ContainerName
#  Des_DirectoryInput
#  Des_DirectoryOutput
#  Param1              -- TOPIC
#  Param2              -- SUBSCRIPTION
#  Param3              -- NOME DELLA PRIMA PROPRIETA' (msg_name) + VALORI
#  Param4              -- NOME DELLA SECONDA PROPRIETA' (msg_version) + VALORE
#  Des_FileName        -- RADICE NOME FILE (es. company_data<yyymmdd> che sarà company_data_2020111815072500.json o company_data_2020111815072500.csv 
#  Des_SchemaName      -- SCHEMA DI DESTINAZIONE (in base a quello si saprà anche lo schema di partenza es. se è presente silver il db di partenza sarà bronze, per gold sarà silver; se presente bronze non si ha un db di partenza ma avremo semplicemente il file json/ fonte dati centrico o altre fonte dati
#  Des_TableName       -- TABELLA DI DESTINAZIONE 

print(ProcToElab)
#assign default return status
toRet = "0"
#retrieve configuration for the notebook
df_procFlow = sqlContext.sql("select B.*  from AUD.cf_schedulation A inner join AUD.cf_flowregistry B on A.Cod_ProcessID = B.Cod_ProcessID where A.Cod_ProcessID ={}".format(ProcToElab)).toPandas()

if df_procFlow.empty:
    print('DataFrame is empty!')
    toRet = "1"
    #INSERT LOG
    u.insertLog(level = 2, type = "ERR", toRet = toRet, guid = GuidForLog, processID = ProcToElab, fileNameTab = "null", codFlow = "null", errDetails = "Configuration dataframe is empty.")
    #return status to master notebook
    dbutils.notebook.exit(toRet)

# COMMAND ----------

df_procFlow

# COMMAND ----------

#set ServiceBus settings

access_key = "Endpoint=sb://sb-etl.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=pZn+nSE+O5azuhiXWCOK8K8Fk8GiRcneur88Ido9lkM="
#initialize service bus client
servicebus_client = u.set_client(access_key)
#initialize service bus management client
servicebus_client_management = ServiceBusAdministrationClient.from_connection_string(conn_str = access_key)

#initialize parameters receiver parameters
dest_raw_folder = "/dbfs/mnt" + df_procFlow["Des_DirectoryOutput"][0]
topic_name = df_procFlow["Param1"][0]
subscription_name = df_procFlow["Param2"][0]

# retrieve subscription runtime properties
sub_properties = servicebus_client_management.get_subscription_runtime_properties(topic_name = topic_name,subscription_name = subscription_name)
# retrieve number of active messagise in a subscription
sub_msg_count = sub_properties.active_message_count
print('Total files to receive: ' + str(sub_msg_count))
# number of messages to load in a single receiver function call
max_message = 100
# calculate number of iterations 
iterNum = math.ceil(sub_msg_count/100)
print('Total iterations: ' + str(iterNum))
# max_time  a receiver should wait the next message to receive after successfully downloading the previous one. Should not exceed 100, as default spark timeout is 120s
max_time = 30
# sleep period in seconds
sleep = 0.02
msg_config = df_procFlow["Param3"][0]
version_config = df_procFlow["Param4"][0]
#list to collect isjson function status
int_status = []
ext_status = ''

try:  
  i = 0
  while i < iterNum:
    print('Current iteration: ' + str(i))
    #run receiver function, ext_status returns receiver function status, int_status returns list of json statuses, whether json messages are valid or not
    ext_status, int_status = asyncio.run(u.file_topic_receiver_lab(servicebus_client, dest_raw_folder, topic_name, subscription_name, msg_config, version_config, max_message, max_time, int_status, sleep))
    i = i + 1
  
  #INSERT LOG
  log_files_name = [i.split('$')[-1] for i in int_status]
  ok_files = len([i.rsplit("$")[1] for i in int_status if i.rsplit("$")[1] == "OK"])
  ko_files = len([i.rsplit("$")[1] for i in int_status if i.rsplit("$")[1] == "KO"])
  
  if len(int_status)>0:
     u.insertLog(level = 1, type = "INFO", toRet = toRet, guid = GuidForLog, processID = ProcToElab, fileNameTab = ', '.join(log_files_name), codFlow = "null", errDetails = " Topic receiver received in total {} msg: {} valid json, {} non valid.".format(len(log_files_name), ok_files, ko_files))
  else:
    
    u.insertLog(level = 1, type = "WARN",toRet = toRet, guid = GuidForLog, processID = ProcToElab, fileNameTab = "null", codFlow = "null", errDetails = " No files to receive")
 
  #/INSERT LOG 
  print(ext_status)
  print(int_status)
except Exception as e:
  #INSERT LOG
  toRet = "1"
  u.insertLog(level = 1, type = "ERR",toRet= toRet, guid = GuidForLog, processID = ProcToElab, fileNameTab = "null", codFlow = "null", errDetails = " Topic receiver failed to receive messages, {}.".format(str(e).replace("'", r"\'")))
  print(e) 

# COMMAND ----------

dbutils.notebook.exit(toRet)
