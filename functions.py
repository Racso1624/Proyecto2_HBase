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
    hfile_content = {"Table Name":table_name, "Column Families":column_families}
    hfile_content = json.dumps(hfile_content)
    hfile.write(hfile_content)

def checkFile(table_name):
    path = f"./HFiles/{table_name}.json"
    return os.path.exists(path)