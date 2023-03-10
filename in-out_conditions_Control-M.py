#!/usr/bin/env python
# -*- coding: utf-8 -*-
from threading import Thread
from tkinter import LabelFrame, Label, Tk, Text, StringVar, Entry
from tkinter import messagebox
from tkinter import ttk
from tkinter import filedialog
from pandas import merge, read_csv, DataFrame
from functools import partial
import datetime
import logging
from concurrent.futures import ThreadPoolExecutor, wait


class App:
    def __init__(self, root):
        self.window = root
        self.window.geometry('550x500')
        self.window.title('cruce jobs CONTROL-M * in-out conditions')
        self.inCond = DataFrame()
        self.outCond = DataFrame()
        self.resultMerge = DataFrame()
        self.id_incond = StringVar()
        self.id_outcond = StringVar()

        frame = LabelFrame(self.window, bg = 'gray26')
        frame.grid(row = 0, columnspan = 8, pady = 10)
        frame2 = LabelFrame(self.window, bg = 'gray26')
        frame2.grid(row = 1, columnspan = 8, pady = 10)
        frame3 = LabelFrame(self.window, bg = 'white')
        frame3.grid(row = 2, columnspan = 8, pady = 10, padx = 20)
        frame4 = LabelFrame(self.window, bg = 'white')
        frame4.grid(row = 3, columnspan = 8, pady = 10, padx = 20)
        frame5 = LabelFrame(self.window, bg = 'white')
        frame5.grid(row = 4, columnspan = 8, pady = 1)

        self.b_df1 = ttk.Button(frame, text='inCondition', command=partial(self.load, 1))
        self.b_df1.focus()
        self.b_df1.grid(row = 1, column = 0, columnspan = 2)
        self.b_df2 = ttk.Button(frame, text='outCondition', command=partial(self.load, 2))
        self.b_df2.grid(row = 3, column = 0, columnspan = 2)
        Label(frame, text= 'Folder: ').grid(row = 4, column = 0, columnspan = 2, pady = 5)
        self.folder = Entry(frame)
        self.folder.grid(row = 4, column = 1, columnspan = 6, pady = 5)

        ttk.Button(frame2, text='cargar', command=self.execute).grid(row = 5, column = 0, columnspan = 2)
        ttk.Button(frame2, text='ejecutar', command=self.execute_cross).grid(row = 5, column = 2, columnspan = 2)
        ttk.Button(frame2, text='guardar', command=self.save).grid(row = 5, column = 4, columnspan = 2)
        ttk.Button(frame5, text='Salir', command=self.window.destroy).grid(row = 1, column = 2, columnspan = 2)

        Label(frame3, text= 'inCondition').grid(row = 0, column = 0, columnspan = 2)
        self.textBox = Text(frame3, width=60, height=5)
        self.textBox.grid(row = 1, columnspan = 8)

        Label(frame3, text= 'outCondition').grid(row = 3, column = 0, columnspan = 2)
        self.textBox2 = Text(frame3, width=60, height=5)
        self.textBox2.grid(row = 4, columnspan = 8)

        self.indica = Label(frame, text = '', width=40,)
        self.indica.grid(row = 1, column = 2, columnspan = 6)
        self.indica2 = Label(frame, text = '', width=40,)
        self.indica2.grid(row = 3, column = 2, columnspan = 6)
         # Set up logging
        logging.basicConfig(filename='transferencia.log', level=logging.INFO, format='%(asctime)s - %(message)s')

    def load(self, button):
        file = filedialog.askopenfilename(initialdir ='/', 
                                            title = 'seleccione archivo',  
                                            filetype=(('csv files','*.csv'),('all files','*.*')))
        if button == 1:
            self.indica['text'] = file
        else:
            self.indica2['text'] = file

    def data_excel(self, loadFile):
        return read_csv(loadFile)

    def execute(self):
        loadFile = self.indica['text']
        loadFile2 = self.indica2['text']
        t = Thread(target=self.thread_pool, args=(loadFile, loadFile2))
        t.start()


    def thread_pool(self, loadFile, loadFile2):
        futures=[]
        # Create a thread pool with 5 workers
        with ThreadPoolExecutor(max_workers=2) as executor:
            # Transfer each file asynchronously
            future1 = executor.submit(self.data_excel, loadFile)
            self.id_incond = id(future1)
            futures.append(future1)
            future2 = executor.submit(self.data_excel, loadFile2)
            self.id_outcond = id(future2)
            futures.append(future2)
            logging.info(f'se identifica variable, las variables future: {future1} ## {future2}')
            # Log progress and wait for completion of each transfer
            done, not_done = wait(futures)
            # for future in as_completed(futures):
            #     filename = futures[future]
            logging.info(f'Transferred file: se identifica variable, done: {done}')
            for future in done:
                try:
                    idFuture = id(future)
                    self.inCond = future1.result()
                    self.outCond = future2.result()
                    self.textBox.insert("1.0", self.inCond.head(5))
                    self.textBox2.insert("1.0", self.outCond.head(5))
                    # transferred_files.add(filename)
                    # with open('transfer.log', 'a') as f:
                    #     f.write(filename + '\n')
                    logging.info(f'Transferred file: {self.id_incond}, tarea: {future} ## {idFuture}')
                    logging.info(f'Transferred file: {self.id_outcond}, tarea: {future}## {idFuture}')
                    # files_transferred += 1
                    # progress_text = f'{files_transferred} / {num_files} archivos transferidos'
                    # self.progress_var.set(progress_text)
                    # self.root.update_idletasks()
                except Exception as e:
                    logging.error(f'Error transferring file: , error: {e}, tarea: {future}')
                else:
                    logging.info(f'Transfer complete: {future} - idTarea: {idFuture}')

    def execute_cross(self):
        t = Thread(target=self.cross)
        t.start()

    def cross(self):
        logging.info(f'funcion cross: {self.id_incond}##\n {self.inCond}\n {self.id_outcond}##\n {self.outCond}\n')

        cross = merge(self.outCond, self.inCond, how = "left", on = "Event Name")
        logging.info(f'se realiza merge y se deja en la variable cross: {cross}\n')
        '''print(resPredecesor.shape)'''
        self.resultMerge = cross[['Server Name_x','Parent Folder_x','Job Name_x','Event Name','Job Name_y']]
        self.resultMerge = self.resultMerge.copy()
        self.resultMerge.rename(columns={'Parent Folder_x':'Parent_Folder_x'}, inplace=True)
        logging.info(f'antes de ingresar al if se cambia nombre de columna parent folderx: {self.resultMerge}\n ## valor de input folder: {self.folder.get()}')

        if not self.folder.get():
            print('sin filtro')
        else:
            self.resultMerge = self.resultMerge[self.resultMerge.Parent_Folder_x == self.folder.get()]

        logging.info(f'finaliza merge: {self.resultMerge}\n')
        self.resultMerge.rename(columns = {'Job Name_y':'sucesores'}, inplace = True)
        '''resultMerge.to_excel('resSucesores.xlsx', index = False)'''
        print('finaliza merge')
        messagebox.showinfo('ejecución', 'finalizó exitosamente')
    
    def save(self):
        t = Thread(target=self.fileSave)
        t.start()


    def fileSave(self):
        now = datetime.datetime.now()
        strNow = now.strftime('%d%m%Y-%H%M')
        fileName = 'in-out_final_' + strNow
        file = filedialog.asksaveasfilename(defaultextension = '.xlsx', initialfile = fileName)
        self.resultMerge.to_excel(file, index=False)
        logging.info(f'se guarda el resultado: {file}\n')

def main():
    root = Tk()
    mi_app = App(root)
    root.mainloop()

if __name__ == '__main__':
    main()
