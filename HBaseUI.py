import tkinter as tk
from tkinter import ttk
from functions import *

"""
Funcionalidades:

- pasar texto a la consola
- cambiar tema
- mandar comandos a la consola con enter o botón
- simulación visual de HBase Shell

Keywords:

- clear = limpiar la consola
- exit = salir del programa

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
            "TEntry", foreground="white", background="black", font=("Arial", 12)
        )

        self.style.configure(
            "TButton", foreground="white", background="#007bff", font=("Arial", 12)
        )
        self.text_box = ttk.Entry(self.root, width=50, style="TEntry")
        self.text_box.pack(padx=20, pady=20)

        self.text_box.bind("<Return>", self.run_command)

        self.submit_button = ttk.Button(
            self.root, text="RUN", command=self.run_command, style="TButton"
        )
        self.submit_button.pack(padx=20, pady=20)

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
        self.command_counter = 0
        self.root.mainloop()

    def run_command(self, event=None):
        input_text = self.text_box.get().strip()

        self.text_box.delete(0, "end")

        if input_text.lower() == "clear":
            self.result_text.configure(state="normal")
            self.result_text.delete("1.0", "end")
            self.result_text.configure(state="disabled")
            self.command_counter = 0
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
        themes = [
            {
                "result_text_fg": "white",
                "result_text_bg": "blue",
                "text_box_fg": "white",
                "text_box_bg": "blue",
                "button_fg": "white",
                "button_bg": "black",
            },
            {
                "result_text_fg": "black",
                "result_text_bg": "white",
                "text_box_fg": "black",
                "text_box_bg": "white",
                "button_fg": "white",
                "button_bg": "black",
            },
            {
                "result_text_fg": "yellow",
                "result_text_bg": "purple",
                "text_box_fg": "yellow",
                "text_box_bg": "purple",
                "button_fg": "white",
                "button_bg": "black",
            },
            {
                "result_text_fg": "green",
                "result_text_bg": "black",
                "text_box_fg": "white",
                "text_box_bg": "black",
                "button_fg": "white",
                "button_bg": "black",
            },
        ]

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
