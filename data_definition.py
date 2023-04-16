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
                # Se obtienen los datos del HFile
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

# Funcion para el comando get       
def get(command):
    # Se limpia la entrada del comando
    if("get " in command):
        command = command.replace("get ", "")
        command_split = command.split(',')
        # Se verifica y separa el comando
        if(len(command_split) == 2):
            # Se obtienen los valores del comando
            table_name = scanWord(command_split[0])
            row_id = scanWord(command_split[1])
            # Se verifica que el archivo exista
            if(checkFile(table_name)):
                # Se obtienen los datos del HFile
                with open(f"./HFiles/{table_name}.json") as file:
                    data_table = json.load(file)
                # Se verfica que la tabla este enable
                if(checkEnabled(data_table)):
                    # Se verifica que el row id exista
                    if(checkRowId(data_table, row_id)):
                        # Se itera en todas las rows para ese row id
                        row_info = data_table["Rows"][row_id]
                        for i in row_info:
                            print(i + " timestamp=" + str(row_info[i]["timestamp"]) + ", value=" + row_info[i]["value"])
                else:
                    print("Table not enable")

# Funcion para el comando scan               
def scan(command):
    # Se limpia la entrada del comando
    if("scan " in command):
        # Se verifica y separa el comando
        # Se obtienen los valores del comando
        command = command.replace("scan ", "")
        table_name = scanWord(command)
        # Se verifica que el archivo exista
        if(checkFile(table_name)):
            # Se obtienen los datos del HFile
            with open(f"./HFiles/{table_name}.json") as file:
                data_table = json.load(file)
            # Se verfica que la tabla este enable
            if(checkEnabled(data_table)):
                # Se itera en todas las rows de la tabla
                rows_info = data_table["Rows"]
                for i in rows_info:
                    for j in data_table["Rows"][i]:
                        print(i + " column=" + j +", timestamp=" + str(rows_info[i][j]["timestamp"]) + ", value=" + rows_info[i][j]["value"])
            else:
                print("Table not enable")

# Funcion para el comando enable            
def enable(command):
    # Se limpia la entrada del comando
    if("enable " in command):
        # Se verifica y separa el comando
        # Se obtienen los valores del comando
        command = command.replace("enable ", "")
        table_name = scanWord(command)
        # Se verifica que el archivo exista
        if(checkFile(table_name)):
            # Se obtienen los datos del HFile
            with open(f"./HFiles/{table_name}.json") as file:
                data_table = json.load(file)
            # Si la tabla esta disable entonces la cambia a enable
            if(not checkEnabled(data_table)):
                data_table["Is_enable"] = True

                # Se reescribe el archivo
                with open(f"./HFiles/{table_name}.json", "w") as file:
                    json.dump(data_table, file, indent=4)
            else:
                print("Table is enable")

# Funcion para el comando disable 
def disable(command):
    # Se limpia la entrada del comando
    if("disable " in command):
        # Se verifica y separa el comando
        # Se obtienen los valores del comando
        command = command.replace("disable ", "")
        table_name = scanWord(command)
        # Se verifica que el archivo exista
        if(checkFile(table_name)):
            # Se obtienen los datos del HFile
            with open(f"./HFiles/{table_name}.json") as file:
                data_table = json.load(file)
            # Si la tabla esta enable entonces la cambia a disable
            if(checkEnabled(data_table)):
                data_table["Is_enable"] = False

                with open(f"./HFiles/{table_name}.json", "w") as file:
                    json.dump(data_table, file, indent=4)
            else:
                print("Table not enable")

# Funcion para el comando count 
def count(command):
    # Se limpia la entrada del comando
    if("count " in command):
        # Se verifica y separa el comando
        # Se obtienen los valores del comando
        command = command.replace("count ", "")
        table_name = scanWord(command)
        # Se verifica que el archivo exista
        if(checkFile(table_name)):
            # Se obtienen los datos del HFile
            with open(f"./HFiles/{table_name}.json") as file:
                data_table = json.load(file)
            # Verifica si la tabla esta enable
            if(checkEnabled(data_table)):
                # Retorna el valor del count
                print(len(data_table["Rows"]))
            else:
                print("Table not enable")
                
def alter(command):
    if("alter " in command):
        command = command.replace("alter ", "")
        command_split = command.split(',')
        if(len(command_split) >= 3):
            table_name = scanWord(command_split[0])
            action = scanWord(command_split[1])
            column_family = scanWord(command_split[2])
            if(checkFile(table_name)):
                with open(f"./HFiles/{table_name}.json") as file:
                    data_table = json.load(file)
                if(checkEnabled(data_table)):
                    if(action == "delete"):
                        if(checkColumn(data_table, column_family)):
                            data_table["Column Families"].remove(column_family)
                            for row_id in data_table["Rows"]:
                                key_list =  list(data_table["Rows"][row_id].keys())
                                for i in key_list:
                                    if(column_family in i):
                                        del data_table["Rows"][row_id][i]
                                    
                            with open(f"./HFiles/{table_name}.json", "w") as file:
                                json.dump(data_table, file, indent=4)
                    elif action == "update":
                        new_column_family = scanWord(command_split[3])
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