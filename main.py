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
    elif(command.startswith("count")):
        count(command)

    elif(command == "exit"):
        exit = False