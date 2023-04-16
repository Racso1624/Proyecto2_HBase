import json
import os

def scanWord(word:str):
    if("'" in word):
        if(word.count("'") >= 2):
            word = word.replace("'", '')
            return word
    elif('"' in word):
        if(word.count('"') >= 2):
            word = word.replace('"', '')
            return word
        
def createFile(table_name, column_families):
    hfile = open(f"./HFiles/{table_name}.json", "w")
    hfile_content = {"Table Name":table_name, "Column Families":column_families, "Is_enable":True, "Rows":{}}
    hfile_content = json.dumps(hfile_content, indent=4)
    hfile.write(hfile_content)

def checkFile(table_name):
    path = f"./HFiles/{table_name}.json"
    return os.path.exists(path)

def checkEnabled(data_table):
    return data_table["Is_enable"]

def checkColumn(data_table, column):
    if(column in data_table["Column Families"]):
        return True 
    else:
        return False

def checkRowId(data_table, row_id):
    if(row_id in data_table["Rows"]):
        return True
    else:
        return False