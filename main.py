# Proyecto 2
# Simulacion de HBase

from functions import *
from HBaseUI import *

#HBASEUI()

exit = True
while(exit):
    command = input(">>> ")

    if(command.startswith("create")):
        if("create " in command):
            command = command.replace("create ", "")
            command_split = command.split(',')
            if(len(command_split) >= 2):
                table_name = scanWord(command_split[0])
                column_families = []
                for i in range(1, len(command_split)):
                    column_families.append(scanWord(command_split[i]))
                if("ERROR" not in table_name):
                    if(not checkFile(table_name)):
                        createFile(table_name, column_families)    

    if(command == "exit"):
        exit = False