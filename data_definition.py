# Proyecto 2
# Simulacion de HBase

from utils import *
from time import *
import os

# Funcion para el comando create
def create(command):
    # Se limpia la entrada del comando
    if("create " in command):
        command = command.replace("create ", "")
        command_split = command.split(',')
        # Se verifica y separa el comando
        if(len(command_split) >= 2):
            # Se obtienen los valores del comando
            table_name = scanWord(command_split[0])
            column_families = []
            # Se agregan todas las column families que puedan existir
            for i in range(1, len(command_split)):
                column_families.append(scanWord(command_split[i]))
            # Se verifica que el archivo no exista
            if("ERROR" not in table_name):
                if(not checkFile(table_name)):
                    # Se crea el archivo
                    createFile(table_name, column_families) 

# Funcion para el comando put
def put(command):
    # Se limpia la entrada del comando
    if("put " in command):
        command = command.replace("put ", "")
        command_split = command.split(',')
        # Se verifica y separa el comando
        if(len(command_split) >= 4):
            # Se obtienen los valores del comando
            table_name = scanWord(command_split[0])
            row_id = scanWord(command_split[1])
            column = scanWord(command_split[2])
            value = scanWord(command_split[3])
            timestamp = int(time())

            # Se verifica que el archivo exista
            if(checkFile(table_name)):
                with open(f"./HFiles/{table_name}.json") as file:
                    data_table = json.load(file)
                # Se verfica que la tabla este enable
                if(checkEnabled(data_table)):
                    column_family, qualifier = column.split(':')
                    # Se verifica que la column family exista
                    if(checkColumn(data_table, column_family)):
                        # Se verifica que el row id exista para crearla
                        if(not checkRowId(data_table, row_id)):
                            data_table["Rows"][row_id] = {}
                        # Se crea la nueva row
                        data_table["Rows"][row_id][column] = {}
                        # Se guarda la celda para la row
                        data_table["Rows"][row_id][column] = {"value":value, "timestamp":timestamp}
                        # Se ordenan los datos dentro de sus rows
                        data_table["Rows"][row_id] = dict(sorted(data_table["Rows"][row_id].items()))
                        # Se ordenan los datos dentro de los row ids
                        data_table["Rows"] = dict(sorted(data_table["Rows"].items()))
                        # Se reescribe el archivo
                        with open(f"./HFiles/{table_name}.json", "w") as file:
                            json.dump(data_table, file, indent=4)
                else:
                    print("Table not enable")
                    
def get(command):
    if("get " in command):
        command = command.replace("get ", "")
        command_split = command.split(',')
        if(len(command_split) == 2):
            table_name = scanWord(command_split[0])
            row_id = scanWord(command_split[1])
            if(checkFile(table_name)):
                with open(f"./HFiles/{table_name}.json") as file:
                    data_table = json.load(file)

                if(checkEnabled(data_table)):
                    if(checkRowId(data_table, row_id)):
                        row_info = data_table["Rows"][row_id]
                        for i in row_info:
                            print(i + " timestamp=" + str(row_info[i]["timestamp"]) + ", value=" + row_info[i]["value"])
                else:
                    print("Table not enable")
                    
def scan(command):
    if("scan " in command):
        command = command.replace("scan ", "")
        table_name = scanWord(command)
        if(checkFile(table_name)):
            with open(f"./HFiles/{table_name}.json") as file:
                data_table = json.load(file)
            if(checkEnabled(data_table)):
                rows_info = data_table["Rows"]
                for i in rows_info:
                    for j in data_table["Rows"][i]:
                        print(i + " column=" + j +", timestamp=" + str(rows_info[i][j]["timestamp"]) + ", value=" + rows_info[i][j]["value"])
            else:
                print("Table not enable")
                
def enable(command):
    if("enable " in command):
        command = command.replace("enable ", "")
        table_name = scanWord(command)
        if(checkFile(table_name)):
            with open(f"./HFiles/{table_name}.json") as file:
                data_table = json.load(file)
            if(not checkEnabled(data_table)):
                data_table["Is_enable"] = True

                with open(f"./HFiles/{table_name}.json", "w") as file:
                    json.dump(data_table, file, indent=4)
            else:
                print("Table not enable")

def disable(command):
    if("disable " in command):
        command = command.replace("disable ", "")
        table_name = scanWord(command)
        if(checkFile(table_name)):
            with open(f"./HFiles/{table_name}.json") as file:
                data_table = json.load(file)
            if(checkEnabled(data_table)):
                data_table["Is_enable"] = False

                with open(f"./HFiles/{table_name}.json", "w") as file:
                    json.dump(data_table, file, indent=4)
            else:
                print("Table not enable")

def count(command):
    if("count " in command):
        command = command.replace("count ", "")
        table_name = scanWord(command)
        if(checkFile(table_name)):
            with open(f"./HFiles/{table_name}.json") as file:
                data_table = json.load(file)
            if(checkEnabled(data_table)):
                print(len(data_table["Rows"]))
            else:
                print("Table not enable")
                
def alter(command):
    if "alter " in command:
        command = command.replace("alter ", "")
        command_split = command.split(',')
        if len(command_split) == 2:
            table_name = scanWord(command_split[0])
            column_action = command_split[1].split()
            if len(column_action) == 2:
                action = column_action[0]
                column_family = scanWord(column_action[1])
                if checkFile(table_name):
                    with open(f"./HFiles/{table_name}.json") as file:
                        data_table = json.load(file)
                    if checkEnabled(data_table):
                        if action == "drop":
                            if checkColumn(data_table, column_family):
                                data_table["Column Families"].remove(column_family)
                                for row_id in data_table["Rows"]:
                                    if column_family in data_table["Rows"][row_id]:
                                        del data_table["Rows"][row_id][column_family]
                                with open(f"./HFiles/{table_name}.json", "w") as file:
                                    json.dump(data_table, file, indent=4)
                        elif action == "modify":
                            new_column_family = scanWord(column_action[1].split()[1])
                            if checkColumn(data_table, column_family):
                                if not checkColumn(data_table, new_column_family):
                                    data_table["Column Families"][data_table["Column Families"].index(column_family)] = new_column_family
                                    for row_id in data_table["Rows"]:
                                        if column_family in data_table["Rows"][row_id]:
                                            data_table["Rows"][row_id][new_column_family] = data_table["Rows"][row_id].pop(column_family)
                                    with open(f"./HFiles/{table_name}.json", "w") as file:
                                        json.dump(data_table, file, indent=4)
                    else:
                        print("Table not enable")
                        
def describe(command):
    if "describe " in command:
        table_name = scanWord(command.replace("describe ", ""))
        if checkFile(table_name):
            with open(f"./HFiles/{table_name}.json") as file:
                data_table = json.load(file)
            if checkEnabled(data_table):
                print("Table Name: ", data_table["Table Name"])
                print("Column Families: ")
                for cf in data_table["Column Families"]:
                    print(" - ", cf)
                print("Is_enable: ", data_table["Is_enable"])
                print("Rows: ")
                for row_id in data_table["Rows"]:
                    print(" - ", row_id)
                    for col in data_table["Rows"][row_id]:
                        print("   - ", col)
            else:
                print("Table not enable")
    else:
        print("Invalid command")
                   
def truncate(command):
    if "truncate " in command:
        command = command.replace("truncate ", "")
        table_name = scanWord(command)
        if checkFile(table_name):
            disable(f"disable {table_name}")
            print("La tabla paso a disable")
            drop(f"drop {table_name}")
            print("Se hizo drop a la tabla")
            create(f"create '{table_name}','column_family1','column_family2'") #aqui solo me falta acceder a las column families para mandarlas al create
            print("Se realizo el create")
        else:
            print("Table does not exist.")

def drop(command):
    if "drop " in command:
        command = command.replace("drop ", "")
        table_name = scanWord(command)
        if checkFile(table_name):
            os.remove(f"./HFiles/{table_name}.json")
            print(f"Dropped table {table_name}.")
        else:
            print(f"Table {table_name} does not exist.")


def drop_all(command):
    if "drop_all" in command:
        tables = os.listdir("./HFiles")
        for table in tables:
            os.remove(f"./HFiles/{table}")

import json

