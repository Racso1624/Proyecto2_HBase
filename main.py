# Proyecto 2
# Simulacion de HBase

from HBaseUI import *
from data_definition import *

#HBASEUI()

exit = True
while(exit):
    command = input(">>> ")

    if(command.startswith("create")):
        create(command)   
    elif(command.startswith("put")):
        put(command)
    elif(command.startswith("get")):
        get(command)
    elif(command.startswith("scan")):
        scan(command)
    elif(command.startswith("enable")):
        enable(command)
    elif(command.startswith("disable")):
        disable(command)
    elif(command.startswith("count")):
        count(command)
    elif(command.startswith("alter")):
        alter(command)
    elif(command.startswith("describe")):
        describe(command)
    elif(command.startswith("truncate")):
        truncate(command)
    elif(command == "exit"):
        exit = False