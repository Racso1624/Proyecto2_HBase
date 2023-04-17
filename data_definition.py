# Proyecto 2
# Simulacion de HBase

from utils import *
from time import *
import os
from prettytable import PrettyTable


def LimpiarInput(func):
    def wrapper(command):
        response = None
        if '"' in command and "'" in command:
            response = "Invalid command. Cannot mix single and double quotes."
        elif command.count('"') % 2 != 0 or command.count("'") % 2 != 0:
            response = "Invalid command. Unclosed quotes."
        elif wrapper is not None:
            if func(command) is None:
                response = "Invalid command."
            else:
                response = func(command)
        elif wrapper is None:
            response = "Invalid command."
        return response

    return wrapper


# Funcion para el comando create
@LimpiarInput
def create(command):
    # Se limpia la entrada del comando
    if "create " in command:
        command = command.replace("create ", "")
        command_split = command.split(",")
        # Se verifica y separa el comando
        if len(command_split) >= 2:
            # Se obtienen los valores del comando
            table_name = scanWord(command_split[0])
            column_families = []
            # Se agregan todas las column families que puedan existir
            for i in range(1, len(command_split)):
                column_families.append(scanWord(command_split[i]))
            # Se verifica que el archivo no exista
            if "ERROR" not in table_name:
                if not checkFile(table_name):
                    # Se crea el archivo
                    createFile(table_name, column_families)
                    if checkFile(table_name):
                        response = f"Table {table_name} already exists\n"
                    else:
                        response = f"Table {table_name} created\n"
                else:
                    response = f"Table {table_name} already exists\n"
            else:
                response = f"{table_name}: command not found\n"
        else:
            response = "Invalid command."
    else:
        response = "Invalid command."

    return response


# Funcion para el comando put
@LimpiarInput
def put(command):
    # Se limpia la entrada del comando
    if "put " in command:
        command = command.replace("put ", "")
        command_split = command.split(",")
        # Se verifica y separa el comando
        if len(command_split) >= 4:
            # Se obtienen los valores del comando
            table_name = scanWord(command_split[0])
            row_id = scanWord(command_split[1])
            column = scanWord(command_split[2])
            value = scanWord(command_split[3])
            timestamp = int(time())

            # Se verifica que el archivo exista
            if checkFile(table_name):
                # Se obtienen los datos del HFile
                with open(f"./HFiles/{table_name}.json") as file:
                    data_table = json.load(file)
                # Se verfica que la tabla este enable
                if checkEnabled(data_table):
                    column_family, qualifier = column.split(":")
                    # Se verifica que la column family exista
                    if checkColumn(data_table, column_family):
                        # Se verifica que el row id exista para crearla
                        if not checkRowId(data_table, row_id):
                            data_table["Rows"][row_id] = {}
                        # Se crea la nueva row
                        data_table["Rows"][row_id][column] = {}
                        # Se guarda la celda para la row
                        data_table["Rows"][row_id][column] = {
                            "value": value,
                            "timestamp": timestamp,
                        }
                        # Se ordenan los datos dentro de sus rows
                        data_table["Rows"][row_id] = dict(
                            sorted(data_table["Rows"][row_id].items())
                        )
                        # Se ordenan los datos dentro de los row ids
                        data_table["Rows"] = dict(sorted(data_table["Rows"].items()))
                        # Se reescribe el archivo
                        with open(f"./HFiles/{table_name}.json", "w") as file:
                            json.dump(data_table, file, indent=4)
                        return "0 row(s) in 0.0000 seconds"
                    else:
                        return f"Unknown column family {column_family}"
                else:
                    return "Table not enabled."
            else:
                return f"Table {table_name} does not exist."
        else:
            return "Invalid command."


# Funcion para el comando get
@LimpiarInput
def get(command):
    # Se limpia la entrada del comando
    if "get " in command:
        command = command.replace("get ", "")
        command_split = command.split(",")
        # Se verifica y separa el comando
        if len(command_split) == 2:
            # Se obtienen los valores del comando
            table_name = scanWord(command_split[0])
            row_id = scanWord(command_split[1])
            # Se verifica que el archivo exista
            if checkFile(table_name):
                # Se obtienen los datos del HFile
                with open(f"./HFiles/{table_name}.json") as file:
                    data_table = json.load(file)
                # Se verfica que la tabla este enable
                if checkEnabled(data_table):
                    # Se verifica que el row id exista
                    if checkRowId(data_table, row_id):
                        # Se itera en todas las rows para ese row id
                        row_info = data_table["Rows"][row_id]
                        table = PrettyTable()
                        table.field_names = ["COLUMN", "CELL"]
                        for column_key in row_info:
                            column_data = row_info[column_key]
                            table.add_row(
                                [
                                    column_key,
                                    f"timestamp={column_data['timestamp']}, value={column_data['value']}",
                                ]
                            )
                        return str(table)
                    else:
                        return f"Row {row_id} does not exist in table {table_name}."
                else:
                    return "Table not enabled."
            else:
                return f"Table {table_name} does not exist."
        else:
            return "Invalid command."


# Funcion para el comando scan
@LimpiarInput
def scan(command):
    if "scan " in command:
        command = command.replace("scan ", "")
        table_name = scanWord(command)
        if checkFile(table_name):
            with open(f"./HFiles/{table_name}.json") as file:
                data_table = json.load(file)
            if checkEnabled(data_table):
                table = PrettyTable()
                table.field_names = ["ROW", "COLUMN+CELL"]
                for row in data_table["Rows"]:
                    for column in data_table["Rows"][row]:
                        cell = f"{column}, timestamp={data_table['Rows'][row][column]['timestamp']}, value={data_table['Rows'][row][column]['value']}"
                        table.add_row([row, cell])
                return str(table)
            else:
                return "Table not enabled"
        else:
            return f"Table {table_name} does not exist."
    else:
        return "Invalid command"


# Funcion para el comando enable
@LimpiarInput
def enable(command):
    # Se limpia la entrada del comando
    if "enable " in command:
        # Se verifica y separa el comando
        # Se obtienen los valores del comando
        command = command.replace("enable ", "")
        table_name = scanWord(command)
        # Se verifica que el archivo exista
        if checkFile(table_name):
            # Se obtienen los datos del HFile
            with open(f"./HFiles/{table_name}.json") as file:
                data_table = json.load(file)
            # Si la tabla esta disable entonces la cambia a enable
            if not checkEnabled(data_table):
                data_table["Is_enabled"] = True

                # Se reescribe el archivo
                with open(f"./HFiles/{table_name}.json", "w") as file:
                    json.dump(data_table, file, indent=4)

                # Return the success string
                return f"Table {table_name} was enabled."
            else:
                # Return the error string
                return f"Table {table_name} is already enabled."
        else:
            # Return the error string
            return f"Table {table_name} does not exist."
    else:
        return "Invalid command."


# Funcion para el comando disable
@LimpiarInput
def disable(command):
    start_time = time()
    if "disable " in command:
        command = command.replace("disable ", "")
        table_name = scanWord(command)

        if checkFile(table_name):
            with open(f"./HFiles/{table_name}.json") as file:
                data_table = json.load(file)

            if checkEnabled(data_table):
                data_table["Is_enabled"] = False

                with open(f"./HFiles/{table_name}.json", "w") as file:
                    json.dump(data_table, file, indent=4)
            else:
                return "Table not enabled"

            time_elapsed = time() - start_time
            return f"0 row(s) in {time_elapsed:.2f} seconds"
        else:
            return "Table not found"
    return "ERROR: Wrong number of arguments: disable <table name>"


# Funcion para el comando count
@LimpiarInput
def count(command):
    # Se limpia la entrada del comando
    if "count " in command:
        # Se verifica y separa el comando
        # Se obtienen los valores del comando
        command = command.replace("count ", "")
        table_name = scanWord(command)
        # Se verifica que el archivo exista
        if checkFile(table_name):
            # Se obtienen los datos del HFile
            with open(f"./HFiles/{table_name}.json") as file:
                data_table = json.load(file)
            # Verifica si la tabla esta enable
            if checkEnabled(data_table):
                # Retorna el valor del count
                return (
                    "0 row(s)\n"
                    if len(data_table["Rows"]) == 0
                    else f"{len(data_table['Rows'])} row(s)\n"
                )
            else:
                return f"ERROR: Table {table_name} is disabled\n"
        else:
            return f"ERROR: Table {table_name} not found\n"


# Funcion para el comando alter
@LimpiarInput
def alter(command):
    # Se limpia la entrada del comando
    if "alter " in command:
        # Se verifica y separa el comando
        command = command.replace("alter ", "")
        command_split = command.split(",")
        # Se obtienen los valores del comando
        if len(command_split) >= 3:
            table_name = scanWord(command_split[0])
            action = scanWord(command_split[1])
            column_family = scanWord(command_split[2])
            # Se verifica que el archivo exista
            if checkFile(table_name):
                # Se obtienen los datos del HFile
                with open(f"./HFiles/{table_name}.json") as file:
                    data_table = json.load(file)
                # Verifica si la tabla esta enable
                if checkEnabled(data_table):
                    # Si la accion que se realiza es delete
                    if action == "delete":
                        # Verifica la columna en la tabla
                        if checkColumn(data_table, column_family):
                            # Remueve la columna de la lista
                            data_table["Column Families"].remove(column_family)
                            # Busca los valores de cada row id con esa columna
                            for row_id in data_table["Rows"]:
                                key_list = list(data_table["Rows"][row_id].keys())
                                for i in key_list:
                                    # Si encuentra la columna la elimina
                                    if column_family in i:
                                        del data_table["Rows"][row_id][i]
                            # Reescribe el archivo
                            with open(f"./HFiles/{table_name}.json", "w") as file:
                                json.dump(data_table, file, indent=4)
                            return f"Column family '{column_family}' deleted from table '{table_name}'."
                        else:
                            return f"Column family '{column_family}' does not exist in table '{table_name}'."
                    # Si la accion es update
                    elif action == "update":
                        new_column_family = scanWord(command_split[3])
                        if checkColumn(data_table, column_family):
                            if not checkColumn(data_table, new_column_family):
                                data_table["Column Families"][
                                    data_table["Column Families"].index(column_family)
                                ] = new_column_family
                                # Busca los valores de cada row id con esa columna
                                for row_id in data_table["Rows"]:
                                    key_list = list(data_table["Rows"][row_id].keys())
                                    for i in key_list:
                                        # Si encuentra la columna la elimina
                                        if column_family in i:
                                            value = data_table["Rows"][row_id][i]
                                            del data_table["Rows"][row_id][i]
                                            column, qualifier = i.split(":")
                                            new_column = (
                                                new_column_family + ":" + qualifier
                                            )
                                            data_table["Rows"][row_id][
                                                new_column
                                            ] = value
                                            # alter 'resiland','update','data','datos'
                                    # Se ordenan los datos dentro de sus rows
                                    data_table["Rows"][row_id] = dict(
                                        sorted(data_table["Rows"][row_id].items())
                                    )
                                # Reescribe el archivo
                                with open(f"./HFiles/{table_name}.json", "w") as file:
                                    json.dump(data_table, file, indent=4)
                                print("xd")
                                return f"Column family '{column_family}' updated to '{new_column_family}' in table '{table_name}'."
                            else:
                                print("xd2")
                                return f"New column family '{new_column_family}' already exists in table '{table_name}'."
                        else:
                            print("xd3")
                            return f"Column family '{column_family}' does not exist in table '{table_name}'."
                else:
                    print("xd4")
                    return f"Table '{table_name}' is not enable."
            else:
                print("xd5")
                return f"Table '{table_name}' does not exist."
        else:
            return "Invalid command."


@LimpiarInput
def describe(command):
    if "describe " in command:
        table_name = scanWord(command.replace("describe ", ""))
        if checkFile(table_name):
            with open(f"./HFiles/{table_name}.json") as file:
                data_table = json.load(file)
            if checkEnabled(data_table):
                result = []
                result.append(f"Table Name: {data_table['Table Name']}")
                result.append("Column Families:")
                for cf in data_table["Column Families"]:
                    result.append(f" - {cf}")
                result.append(f"Is_enabled: {data_table['Is_enabled']}")
                return "\n".join(result)
            else:
                return "Table not enabled"
        else:
            return f"Table {table_name} does not exist."
    else:
        return "Invalid command"


@LimpiarInput
def truncate(command):
    if "truncate " in command:
        command = command.replace("truncate ", "")
        table_name = scanWord(command)
        if checkFile(table_name):
            disable(f"disable {table_name}")
            print("La tabla paso a disable")
            drop(f"drop {table_name}")
            print("Se hizo drop a la tabla")
            create(
                f"create '{table_name}','column_family1','column_family2'"
            )  # aqui solo me falta acceder a las column families para mandarlas al create
            print("Se realizo el create")
        else:
            print("Table does not exist.")


@LimpiarInput
def drop(command):
    if "drop " in command:
        command = command.replace("drop ", "")
        table_name = scanWord(command)
        if checkFile(table_name):
            os.remove(f"./HFiles/{table_name}.json")
            return f"Table {table_name} dropped successfully."
        else:
            return f"Table {table_name} does not exist."


@LimpiarInput
def drop_all(command):
    if "drop_all" in command:
        tables = os.listdir("./HFiles")
        for table in tables:
            os.remove(f"./HFiles/{table}")
        return f"{len(tables)} row(s) in {len(tables)} table(s) deleted\n"
