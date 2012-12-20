#!/usr/bin/python
from Tkinter import *
import threading
import thread
import time
import subprocess
import os

class MainWindow:

    canvas = None
    arrows = {}
    portArray = ["10000","20000","30000","40000"]

    def __init__(self,master):
        self.lock = threading.Lock()
        ScreenSizeX = master.winfo_screenwidth()  #Get screen width [pixels]
        ScreenSizeY = master.winfo_screenheight() #Get screen height [pixels]
        # ScreenRatio = 0.8                              #Set the screen ratio for width and height
        # FrameSizeX  = int(ScreenSizeX * ScreenRatio)
        # FrameSizeY  = int(ScreenSizeY * ScreenRatio)
        # FramePosX   = (ScreenSizeX - FrameSizeX)/2 #Find left and up border of window
        # FramePosY   = (ScreenSizeY - FrameSizeY)/2
        # master.geometry("%sx%s+%s+%s"%(FrameSizeX,FrameSizeY,FramePosX,FramePosY))
        self.frame = Frame(master)
        self.frame.pack()
        self.client1 = Button(self.frame, text="Start Client 1",justify=LEFT,command = self.client1Click)
        self.client1.pack()
        self.client2 = Button(self.frame, text="Start Client 2",justify=LEFT,command = self.client2Click)
        self.client2.pack()
        self.client3 = Button(self.frame, text="Start Client 3",justify=LEFT,command = self.client3Click)
        self.client3.pack()
        self.client4 = Button(self.frame, text="Start Client 4",justify=LEFT,command = self.client4Click)
        self.client4.pack()
        self.canvas = Canvas(self.frame,width=800,height=800)
        os.system("rm *.log")
        # machine1 = Machine(self.canvas,1,"port1")
        # machine2 = Machine(self.canvas,2,"port2")
        # machine3 = Machine(self.canvas,3,"port3")
        # machine4 = Machine(self.canvas,4,"port4")
        # machine1.setLabel("Hello");
        # Arrow(self.canvas,machine4,machine1,"4-1")
        # Arrow(self.canvas,machine4,machine2,"4-2")
        # Arrow(self.canvas,machine4,machine3,"4-3")

    def client1Click(self):
        if(self.client1["text"] == "Start Client 1"):
            self.client1["text"] = "Kill Client 1"
            pipe = subprocess.Popen(["./lock_server",self.portArray[0],self.portArray[0]])
            self.client1Pid = pipe.pid
            print str(self.client1Pid)
        else:
            self.client1["text"] = "Start Client 1"
            os.kill(self.client1Pid,9)
        print 'click 1'

    def client2Click(self):
        if(self.client2["text"] == "Start Client 2"):
            self.client2["text"] = "Kill Client 2"
            pipe = subprocess.Popen(["./lock_server",self.portArray[0],self.portArray[1]])
            self.client2Pid = pipe.pid
        else:
            self.client2["text"] = "Start Client 2"
            os.kill(self.client2Pid,9)
        print 'click 2'

    def client3Click(self):
        if(self.client3["text"] == "Start Client 3"):
            self.client3["text"] = "Kill Client 3"
            pipe = subprocess.Popen(["./lock_server",self.portArray[0],self.portArray[2]])
            self.client3Pid = pipe.pid
        else:
            self.client3["text"] = "Start Client 3"
            os.kill(self.client3Pid,9)
        print 'click 3'

    def client4Click(self):
        if(self.client4["text"] == "Start Client 4"):
            self.client4["text"] = "Kill Client 4"
            pipe = subprocess.Popen(["./lock_server",self.portArray[0],self.portArray[3]])
            self.client4Pid = pipe.pid
        else:
            self.client4["text"] = "Start Client 4"
            os.kill(self.client4Pid,9)
        print 'click 4'

    def addMachine(self,machineNumber,label):
        return Machine(self.canvas,machineNumber,label)

    def addArrow(self,machine1,machine2,label):
        self.lock.acquire()
        keyStr = str(machine1) + "-" + str(machine2)
        for key,value in self.arrows.items():
            self.arrows[key].remove()
            del self.arrows[key]
        # key = str(machine1) + "-" + str(machine2)
        # revKey = str(machine2) + "-" + str(machine1)
        # if(key in self.arrows):
        # 	self.arrows[key].remove()
        # if(revKey in self.arrows):
        # 	self.arrows[revKey].remove()
        self.arrows[keyStr] = Arrow(self.canvas,machine1,machine2,label)
        self.lock.release()

    def updateInstanceValue(self,machine,instanceValue):
        machine.updateInstanceValue(instanceValue)

    def updateNodesValue(self,machine,nodes):
        machine.updateNodesValue(nodes)

    def updateNValue(self,machine,nValue):
        machine.updateNValue(nValue)

    def updateStatus(self,machine,status):
        machine.updateStatus(status)

    def updateDead(self,machine):
        machine.dead()





class Machine:
    """Creates a symbolic machine on the Canvas"""

    machineSize = 100
    machineSep =  210
    textDifference = 50
    textYDifference = 15
    startCordinate = None
    endCordinate = None
    labelElm = None
    number = None
    master = None
    n = None
    instance = None
    nodes = None
    label = None
    rectangle = None
    status = None


    def __init__(self,master,number,label,fill="green"):
        self.number = number
        self.master = master
        self.label = label

        if(number%2 == 0):
            self.startCordinate = (self.machineSize + 2*self.machineSep,self.machineSep + (self.machineSize + self.machineSep)*(number/2-1))
            self.endCordinate = (2*self.machineSize + 2*self.machineSep,self.machineSep + self.machineSize + (self.machineSize + self.machineSep)*(number/2-1))
            self.rectangle = master.create_rectangle(self.startCordinate[0],self.startCordinate[1],self.endCordinate[0],self.endCordinate[1],fill = "green")
            self.labelElm = master.create_text(self.startCordinate[0],self.startCordinate[1]-10,text=label)
            self.n = master.create_text(self.startCordinate[0]+self.machineSize+self.textDifference,self.startCordinate[1]+self.textYDifference,text="n:")
            self.instance = master.create_text(self.startCordinate[0]+self.machineSize+self.textDifference,self.startCordinate[1]+2*self.textYDifference,text="instance:")
            self.nodes = master.create_text(self.startCordinate[0]+self.machineSize+self.textDifference,self.startCordinate[1]+3*self.textYDifference,text="nodes:")
            self.status = master.create_text(self.startCordinate[0]+self.machineSize+self.textDifference,self.startCordinate[1]+4*self.textYDifference,text="status:")

        else:
            self.startCordinate = (self.machineSep,self.machineSep + (self.machineSize + self.machineSep)*(number-1)/2)
            self.endCordinate = (self.machineSize + self.machineSep,self.machineSep + self.machineSize + (self.machineSize + self.machineSep)*(number-1)/2)
            self.rectangle = master.create_rectangle(self.startCordinate[0], self.startCordinate[1],self.endCordinate[0],self.endCordinate[1],fill = "green")
            self.labelElm = master.create_text(self.startCordinate[0], self.startCordinate[1] -10,text=label)
            self.n = master.create_text(self.startCordinate[0]-2*self.textDifference,self.startCordinate[1]+self.textYDifference,text="n:")
            self.instance = master.create_text(self.startCordinate[0]-2*self.textDifference,self.startCordinate[1]+2*self.textYDifference,text="instance:")
            self.nodes = master.create_text(self.startCordinate[0]-2*self.textDifference,self.startCordinate[1]+3*self.textYDifference,text="nodes:")
            self.status = master.create_text(self.startCordinate[0]-2*self.textDifference,self.startCordinate[1]+4*self.textYDifference,text="status:")
        master.pack()

    def getStartCordinate(self):
        return self.startCordinate

    def getEndCordinate(self):
        return self.endCordinate

    def getNumber(self):
        return self.number

    def getHorizontalMid(self):
        return (int(self.endCordinate[0]-self.startCordinate[0])/2) + self.startCordinate[0]

    def getVerticalMid(self):
        return (int(self.endCordinate[1] - self.startCordinate[1])/2) + self.startCordinate[1]

    def setLabel(self,newLabel):
        self.master.itemconfigure(self.labelElm,text=newLabel)

    def dead(self):
        self.master.itemconfigure(self.rectangle,fill="red")

    def alive(self):
        self.master.itemconfigure(self.rectangle,fill="green")

    def updateInstanceValue(self,instanceValue):
        self.master.itemconfigure(self.instance,text="instance:"+instanceValue)

    def updateNValue(self,nValue):
        self.master.itemconfigure(self.n,text="n:"+nValue)

    def updateNodesValue(self,nodes):
        self.master.itemconfigure(self.nodes,text="nodes:"+nodes)

    def updateStatus(self,status):
        self.master.itemconfigure(self.status,text="status:"+status)

    def __str__(self):
        return self.label



class Arrow:
    arrowLabel = None
    lineUI = None
    master = None

    def __init__(self,master,machine1,machine2,label):
        self.master = master
        if((machine1.getNumber() + machine2.getNumber())%2 == 0):
            """If the sum of the numbers is even then draw the arrow vertically """
            if(machine2.getNumber() > machine1.getNumber()):
                arrowStartPoint = (machine1.getHorizontalMid(),machine1.getEndCordinate()[1])
                arrowEndPoint = (machine2.getHorizontalMid(),machine2.getStartCordinate()[1])
                self.arrowLabel = master.create_text(machine1.getHorizontalMid() + 5,(machine1.getEndCordinate()[1] + machine2.getStartCordinate()[1])/2 ,text=label)
            else:
                arrowStartPoint = (machine1.getHorizontalMid(),machine1.getStartCordinate()[1])
                arrowEndPoint = (machine2.getHorizontalMid(),machine2.getEndCordinate()[1])
                self.arrowLabel = master.create_text(machine2.getHorizontalMid() + 5,(machine1.getEndCordinate()[1] + machine2.getStartCordinate()[1])/2 ,text=label)
            self.lineUI = master.create_line(arrowStartPoint[0],arrowStartPoint[1],arrowEndPoint[0],arrowEndPoint[1],arrow=LAST)
        else:
            """Here arrows will be horizontally or diagonal"""
            if(machine2.getNumber() > machine1.getNumber()):
                if(machine2.getNumber() != 3 and machine1.getNumber() != 2):
                    arrowStartPoint = (machine1.getEndCordinate()[0],machine1.getVerticalMid())
                    arrowEndPoint = (machine2.getStartCordinate()[0],machine2.getVerticalMid())
                    self.arrowLabel = master.create_text((machine1.getEndCordinate()[0]+ machine2.getStartCordinate()[0])/2,(machine1.getEndCordinate()[1] + machine2.getStartCordinate()[1])/2 - 5,text=label)
                else:
                    arrowStartPoint = (machine1.getStartCordinate()[0],machine1.getVerticalMid())
                    arrowEndPoint = (machine2.getEndCordinate()[0],machine2.getVerticalMid())
                    self.arrowLabel = master.create_text((machine1.getStartCordinate()[0]+ machine2.getEndCordinate()[0])/2,(machine1.getEndCordinate()[1] + machine2.getStartCordinate()[1])/2 ,text=label)
            else:
                if(machine2.getNumber() != 2 and machine1.getNumber() != 3):
                    arrowStartPoint = (machine1.getStartCordinate()[0],machine1.getVerticalMid())
                    arrowEndPoint = (machine2.getEndCordinate()[0],machine2.getVerticalMid())
                    self.arrowLabel = master.create_text((machine1.getStartCordinate()[0]+ machine2.getEndCordinate()[0])/2,(machine1.getEndCordinate()[1] + machine2.getStartCordinate()[1])/2 - 5,text=label)
                else:
                    arrowStartPoint = (machine1.getEndCordinate()[0],machine1.getVerticalMid())
                    arrowEndPoint = (machine2.getStartCordinate()[0],machine2.getVerticalMid())
                    self.arrowLabel = master.create_text((machine1.getEndCordinate()[0]+ machine2.getStartCordinate()[0])/2,(machine1.getEndCordinate()[1] + machine2.getStartCordinate()[1])/2 ,text=label)
            self.lineUI = master.create_line(arrowStartPoint[0],arrowStartPoint[1],arrowEndPoint[0],arrowEndPoint[1],arrow=LAST,tags=["hello1"])
        master.pack()

    def remove(self):
        self.master.delete(self.lineUI)
        self.master.delete(self.arrowLabel)

mainWindow = None

class MainHandler:

    totalMachines = 0
    machines = {}
    mainWindow = []

    def __init__(self):
        thread.start_new_thread(createUI,(self.mainWindow,1))

    def parseSocketData(self,source,msg):
        print source
        print msg
        messageArray = msg.split("|")
        if(messageArray[0].lower() == "PORT".lower()):
            if(source in self.machines):
                self.machines[source].alive()
            else:
                self.totalMachines = self.totalMachines + 1
                self.machines[source] = self.mainWindow[0].addMachine(self.totalMachines,messageArray[1])
        else:
            if(messageArray[0].lower() == "JOIN_REQUEST".lower()):
                self.mainWindow[0].addArrow(self.machines[messageArray[1]],self.machines[source],"JOIN Request")
            else:
                if(messageArray[0].lower() == "PREPARE".lower()):
                    print self.machines
                    self.mainWindow[0].addArrow(self.machines[source],self.machines[messageArray[1]],"PREPARE:instance="+messageArray[2]+":n="+messageArray[3])
                    # self.mainWindow[0].updateInstanceValue(self.machines[source],messageArray[2])
                    # self.mainWindow[0].updateNValue(self.machines[source],messageArray[3])
                else:
                    if(messageArray[0].lower() == "VALUES".lower()):
                        self.mainWindow[0].updateInstanceValue(self.machines[messageArray[1]],messageArray[2])
                        self.mainWindow[0].updateNValue(self.machines[messageArray[1]],messageArray[3])
                        self.mainWindow[0].updateNodesValue(self.machines[messageArray[1]],messageArray[4])
                    else:
                        if(messageArray[0].lower() == "DEAD".lower()):
                            self.mainWindow[0].updateDead(self.machines[source])
                        else:
                            if(messageArray[0].lower() == "PREPARE-OK".lower()):
                                self.mainWindow[0].addArrow(self.machines[messageArray[1]],self.machines[source],"PREPARE_OK:value="+messageArray[2]+":n="+messageArray[3])
                            else:
                                if(messageArray[0].lower() == "PREPARE-REJECT".lower()):
                                    self.mainWindow[0].addArrow(self.machines[messageArray[1]],self.machines[source],"PREPARE_REJECT")
                                else:
                                    if(messageArray[0].lower() == "PREPARE-OLD".lower()):
                                        self.mainWindow[0].addArrow(self.machines[messageArray[1]],self.machines[source],"PREPARE_OLD:value="+messageArray[2])
                                    else:
                                        if(messageArray[0].lower() == "STATUS".lower()):
                                            if(messageArray[1] == "1"):
                                                self.mainWindow[0].updateStatus(self.machines[source],"Proposer,Acceptor")
                                            else:
                                                self.mainWindow[0].updateStatus(self.machines[source],"Acceptor")
                                        else:
                                            if(messageArray[0].lower() == "PROPOSE".lower()):
                                                self.mainWindow[0].addArrow(self.machines[source],self.machines[messageArray[1]],"Propose:value="+messageArray[2]+":n="+messageArray[3] + ":instance="+messageArray[4])
                                            else:
                                                if(messageArray[0].lower() == "DECIDE".lower()):
                                                    self.mainWindow[0].addArrow(self.machines[source],self.machines[messageArray[1]],"DECIDE:value="+messageArray[2]+":instance="+messageArray[3])
                                                else:
                                                    if(messageArray[0].lower() == "ACCEPT_OK".lower()):
                                                        if(messageArray[2] == "1"):
                                                            self.mainWindow[0].addArrow(self.machines[messageArray[1]],self.machines[source],"ACCEPT_OK")
                                                        else:
                                                            self.mainWindow[0].addArrow(self.machines[messageArray[1]],self.machines[source],"ACCEPT_REJECT")





    def __str__(self):
        return "MainHandler Class"

def createUI(mainWindow,args):
    root = Tk()
    mainWindow.append(MainWindow(root))
    root.mainloop()

def paxosUIFactory():
    return MainHandler()
