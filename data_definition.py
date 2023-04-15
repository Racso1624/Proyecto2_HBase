from utils import *
from time import *

def create(command):
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

def put(command):
    if("put " in command):
        command = command.replace("put ", "")
        command_split = command.split(',')
        if(len(command_split) >= 4):
            table_name = scanWord(command_split[0])
            row_id = scanWord(command_split[1])
            column = scanWord(command_split[2])
            timestamp = int(time())

            if(checkFile(table_name)):
                with open(f"./HFiles/{table_name}.json") as file:
                    data_table = json.load(file)

                if(checkEnabled(data_table)):
                    column_family, qualifier = column.split(':')
                    