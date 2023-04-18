import tkinter as tk
from tkinter import ttk
from utils import *
from data_definition import *
from Constants import *
from PIL import Image, ImageTk


"""
Funcionalidades:

- pasar texto a la consola
- cambiar tema
- mandar comandos a la consola con enter o botón
- simulación visual de HBase Shell
- autocomplete

Keywords:

- clear = limpiar la consola
- exit = salir del programa
- help = mostrar comandos
- create
- list
- disable
- Is_enabled
- alter
- drop
- drop all
- describe

"""


class HBASEUI:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("HBase")
        self.root.pack_propagate(0)
        self.root.geometry("800x700")
        self.root.minsize(800, 700)
        self.root.maxsize(800, 700)

        self.theme_button = ttk.Button(
            self.root, text="Cambiar tema", command=self.change_theme, style="TButton"
        )
        self.theme_button.place(x=650, y=20)

        self.style = ttk.Style()
        self.style.configure(
            "TEntry", foreground="black", background="black", font=("Arial", 12)
        )

        self.style.configure(
            "TButton", foreground="white", background="#007bff", font=("Arial", 12)
        )
        self.text_box = ttk.Entry(self.root, width=50, style="TEntry")
        self.text_box.pack(padx=20, pady=20)

        self.text_box.bind("<Return>", self.run_command)

        self.result_text = tk.Text(
            self.root,
            font=("Courier", 12),
            wrap="word",
            height=10,
            fg="green",
            bg="black",
        )
        self.result_text.pack(expand=True, fill="both", padx=20, pady=20)
        self.result_text.configure(state="disabled")
        self.text_box.pack(padx=20, pady=20)
        self.text_box.bind("<Return>", self.run_command)
        self.text_box.bind("<KeyRelease>", self.autocomplete)
        self.text_box.bind("<Tab>", self.tab_complete)
        self.tab_pressed = False
        self.suggestion_label = tk.Label(self.root, font=("Arial", 12), fg="gray")
        self.suggestion_label.place(x=20, y=70)
        self.current_autocomplete = None
        self.shell_started = False
        self.first_hbase_shell = True
        self.command_counter = 0
        self.root.mainloop()

    def init_message(self):
        self.result_text.configure(state="normal")
        self.result_text.insert(
            "end",
            "\nHBase Shell; enter 'help<RETURN>' for list of supported commands.\n"
            'Type "exit<RETURN>" to leave the HBase Shell\n'
            "Version Ale-Racso-Lam\n",
        )

    def show_help(self):
        help_text = ""
        for command in commands:
            help_text += f"{command}\n"
        return help_text

    def run_command(self, event=None):
        input_text = self.text_box.get().strip()
        self.text_box.delete(0, "end")
        if not self.shell_started:
            if input_text.lower() == "hbase shell" and self.first_hbase_shell:
                self.shell_started = True
                self.first_hbase_shell = False
                self.result_text.configure(state="normal")
                self.result_text.delete("1.0", "end")
                self.result_text.insert("end", f"AleRacsoLam> {input_text}\n")
                self.result_text.configure(state="disabled")
                self.init_message()
            else:
                self.result_text.configure(state="normal")
                self.result_text.insert(
                    "end", f"AleRacsoLam> use 'hbase shell' to start\n"
                )
                self.result_text.configure(state="disabled")
            return
        if input_text.lower() == "help":
            self.Execute(self.show_help())
            return
        if input_text.lower() == "clear" or input_text.lower() == "cls":
            self.result_text.configure(state="normal")
            self.result_text.delete("1.0", "end")
            self.result_text.configure(state="disabled")
            self.command_counter = 0
        elif input_text.lower() == "help":
            self.show_help()
        elif "create" in input_text.lower():
            self.Execute(create(input_text), input_text.lower())
        elif "put" in input_text.lower():
            self.Execute(put(input_text), input_text.lower())
        elif "get" in input_text.lower():
            self.Execute(get(input_text), input_text.lower())
        elif "scan" in input_text.lower():
            self.Execute(scan(input_text), input_text.lower())
        elif "enable" in input_text.lower():
            self.Execute(enable(input_text), input_text.lower())
        elif "disable" in input_text.lower():
            self.Execute(disable(input_text), input_text.lower())
        elif "count" in input_text.lower():
            self.Execute(count(input_text), input_text.lower())
        elif "alter" in input_text.lower():
            resultado = alter(input_text)
            print(resultado)
            self.Execute(resultado, input_text.lower())
        elif "list" == input_text.lower():
            self.Execute(list(), input_text.lower())
        elif "describe" in input_text.lower():
            self.Execute(describe(input_text), input_text.lower())
        elif "truncate" in input_text.lower():
            self.Execute(truncate(input_text), input_text.lower())
        elif "deleteall" in input_text.lower():
            self.Execute(deleteAll(input_text), input_text.lower())
        elif "drop" in input_text.lower():
            self.Execute(drop(input_text), input_text.lower())
        elif "drop_all" in input_text.lower():
            self.Execute(drop_all(input_text), input_text.lower())
        elif input_text.lower() == "exit":
            self.root.destroy()

        else:
            formatted_counter = f"{self.command_counter:03}"
            self.result_text.configure(state="normal")
            self.result_text.insert(
                "end", f"hbase(main):{formatted_counter}:0> {input_text}\n"
            )
            self.result_text.configure(state="disabled")
            self.result_text.see("end")

            self.command_counter += 1

    def change_theme(self):
        if not hasattr(self, "theme_index"):
            self.theme_index = 0

        theme = themes[self.theme_index]

        self.result_text.configure(
            fg=theme["result_text_fg"], bg=theme["result_text_bg"]
        )
        self.text_box.configure(
            foreground=theme["text_box_fg"], background=theme["text_box_bg"]
        )
        self.style.configure(
            "TButton", foreground=theme["button_fg"], background=theme["button_bg"]
        )

        self.theme_index = (self.theme_index + 1) % len(themes)

    def Execute(self, function, input_text=""):
        response = function
        formatted_counter = f"{self.command_counter:03}"
        self.result_text.configure(state="normal")
        self.result_text.insert(
            "end", f"hbase(main):{formatted_counter}:0> {input_text}\n{response}\n"
        )
        self.result_text.configure(state="disabled")
        self.result_text.see("end")
        self.command_counter += 1

    def autocomplete(self, event=None):
        if event.keysym != "Tab":
            self.tab_pressed = False
            input_text = self.text_box.get().strip().lower()
            if (
                len(input_text) >= 3
                and not self.tab_pressed
                and input_text[-1].isalpha()
                and self.shell_started == True
            ):
                matching_commands = [
                    cmd for cmd in commands if cmd.startswith(input_text)
                ]
                if matching_commands:
                    self.current_autocomplete = matching_commands[0]
                else:
                    self.current_autocomplete = None
            else:
                self.current_autocomplete = None
            if self.current_autocomplete:
                self.suggestion_label.config(text=self.current_autocomplete)
            else:
                self.suggestion_label.config(text="")
        else:
            self.tab_pressed = True

    def tab_complete(self, event=None):
        if self.current_autocomplete:
            self.text_box.delete(0, "end")
            self.text_box.insert(0, self.current_autocomplete)
            self.current_autocomplete = None
            self.suggestion_label.config(text="")
            return "break"
