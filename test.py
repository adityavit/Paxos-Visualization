#!/usr/bin/python
from Tkinter import *

canvas = Canvas(width=300, height=300, bg='white')  
canvas.pack(expand=YES, fill=BOTH)                  
     
canvas.create_line(100, 150, 200, 200)              
     
     
widget = Label(canvas, text='AAA', fg='white', bg='black')
widget.pack()
canvas.create_window(100, 100, window=widget)     
mainloop()
