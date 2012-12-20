#!/usr/bin/python
import socket
import sys
from socket import *
import threading
import thread
import string
from paxosui import *
mainHandler = None

def handler(clientsock,addr):
    source = None
    while 1:
        try:
            data = clientsock.recv(BUFSIZ)
        except error, e:
            mainHandler.parseSocketData(source,"DEAD")
            break
        print data
        if not data:
            print 'connection break',addr
            mainHandler.parseSocketData(source,"DEAD")
            break
        dataSplit = data.split("|")
        if(dataSplit[0].lower() == "PORT".lower()):
            source = string.strip(dataSplit[1])
            print source
        mainHandler.parseSocketData(source,string.strip(data))
        #msg = 'OK'
        #clientsock.send(msg)
    print 'connection closed by ' , addr
    clientsock.close()

def createUI(mainHandler,args):
    mainHandler = paxosUIFactory()

if __name__=='__main__':
    HOST = 'localhost'
    PORT = 21567
    BUFSIZ = 1024
    ADDR = (HOST, PORT)
    serversock = socket(AF_INET, SOCK_STREAM)
    serversock.bind(ADDR)
    serversock.listen(2)
    mainHandler = paxosUIFactory()
    print mainHandler

    while 1:
        print 'waiting for connection...'
        clientsock, addr = serversock.accept()
        print '...connected from:', addr
        thread.start_new_thread(handler, (clientsock, addr))
