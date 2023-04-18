# Proyecto 2
# Simulacion de HBase

import json
import os
from time import *


# Funcion para escanear cada palabra de un comando y devolvela sin quotes
def scanWord(word: str):
    # Se revisa que tipo de quote es
    if "'" in word:
        if word.count("'") >= 2:
            word = word.replace("'", "")
            # Se devuelve la palabra
            return word
    elif '"' in word:
        if word.count('"') >= 2:
            word = word.replace('"', "")
            # Se devuelve la palabra
            return word


# Funcion para crear cada archivo junto a su informacion principal
def createFile(table_name, column_families):
    # Se abre el HFile
    hfile = open(f"./HFiles/{table_name}.json", "w")
    # Se escribe el contenido
    hfile_content = {
        "Table Name": table_name,
        "Column Families": column_families,
        "Is_enabled": True,
        "Rows": {},
    }
    hfile_content = json.dumps(hfile_content, indent=4)
    # Se crea
    hfile.write(hfile_content)


# Funcion para verificar si el archivo existe en la carpeta
def checkFile(table_name):
    path = f"./HFiles/{table_name}.json"
    if os.path.exists(path):
        creation_time = os.path.getctime(path)
        current_time = time()
        if (current_time - creation_time) < 0.5:
            return False
        else:
            return True
    else:
        return False


# Funcion para verificar si el archivo esta enable
def checkEnabled(data_table):
    return data_table["Is_enabled"]


# Funcion para verificar si la columna esta en las column families
def checkColumn(data_table, column):
    if column in data_table["Column Families"]:
        return True
    else:
        return False


# Funcion para verificar si existe el row id en la tabla
def checkRowId(data_table, row_id):
    if row_id in data_table["Rows"]:
        return True
    else:
        return False