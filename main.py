# Proyecto 2
# Simulacion de HBase
import tkinter as tk
from tkinter import *
from tkinter import ttk

class HBASEUI:
    def __init__(self,hbase):
        self.hbase=hbase
        self.root = tk.Tk()
        self.root.title("HBase")
        self.root.pack_propagate(0)
        self.root.geometry("800x700")
        self.root.minsize(800, 700)
        self.root.maxsize(800, 700)
        
        #Style
        self.style = ttk.Style()
        self.style.theme_use("clam")
        self.style.configure("TEntry", foreground="black",
                             background="white", font=("Arial", 12))

        #Textbox
        self.style.configure("TButton", foreground="white",
                             background="#007bff", font=("Arial", 12))
        self.text_box = ttk.Entry(self.root, width=50, style="TEntry")
        self.text_box.pack(padx=20, pady=20)
        #Boton
        self.submit_button = ttk.Button(
            self.root, text="RUN", command= , style="TButton"
        )
        self.submit_button.pack(padx=20, pady=20)
        
        #Cuadro de texto 
        self.result_text = tk.Text(self.root, font=(
            "Arial", 12), wrap="word", height=10)
        self.result_text.pack(expand=True, fill="both", padx=20)
        self.result_text.configure(state="disabled")

        self.scrollbar = tk.Scrollbar(self.root)
        self.scrollbar.pack(side="right", fill="y", anchor="e")

        self.result_text.configure(yscrollcommand=self.scrollbar.set)
        self.scrollbar.configure(command=self.result_text.yview)

        self.root.mainloop()

