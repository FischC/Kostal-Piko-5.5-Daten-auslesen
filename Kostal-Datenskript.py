from datetime import datetime
import paho.mqtt.client as mqtt
from bs4 import BeautifulSoup
import requests
import time
import os
import re
import random
import certifi
import uuid
import sys

#Wechselrichter Anmeldedaten
url = 'http://piko.local/'
nutzername = 'pvserver'
pw = '********'

#Daten Ausgabe Komandozeile Ture/False
printValue = True

#Daten über MQTT Senden Ture/False
useMQTT = True

#MQTT Broker IP adresse Eingeben ohne Port!
mqttSSL = False
mqttBroker = "127.0.0.1"
mqttuser = ""
mqttpasswort = ""
mqttport = 1883
mqttprefix = "Wechselrichter/Kostal/"

#Interval 
intervalShort = 20
intervalLong = 180

#MQTT Init
if useMQTT:
    try:
        clientmqtt = mqtt.Client("Kostal_"+str(uuid.uuid1()))
        clientmqtt.username_pw_set(mqttuser, mqttpasswort)
        if mqttSSL:
            clientmqtt.tls_set(certifi.where())
        clientmqtt.connect(mqttBroker, mqttport)
        clientmqtt.loop_start()
    except:
        print("Die Ip Adresse des Brokers ist falsch!")
        sys.exit()

def str2float(str):
    try:
        return float(str.strip())
    except:
        return float(0)

def str2int(str):
    try:
        return int(str.strip())
    except:
        return int(0)

while 1:
    interval = intervalLong         
    try:
        page = requests.get(url,auth=(nutzername,pw),timeout=5)

        if re.match(".*<title>PV Webserver<\/title>.*",str(page.content)):
            soup = BeautifulSoup(page.content, 'html.parser')
            results = soup.find_all('td')
            
            Aktuelleleistung_W = str2int(results[14].get_text())
            Gesamtenergie_kWh  = str2int(results[17].get_text())
            Tagesenergie_kWh   = str2float(results[26].get_text())
            Status             = results[32].get_text().strip('\r\n ')
            String1Spannung_V  = str2int(results[56].get_text())
            String1Strom_A     = str2float(results[65].get_text())
            String2Spannung_V  = str2int(results[82].get_text())
            String2Strom_A     = str2float(results[91].get_text())
            String3Spannung_V  = str2int(results[108].get_text())
            String3Strom_A     = str2float(results[117].get_text())
            L1Spannung_V       = str2int(results[59].get_text())
            L1Leistung_W       = str2int(results[68].get_text())
            L2Spannung_V       = str2int(results[85].get_text())
            L2Leistung_W       = str2int(results[94].get_text())
            L3Spannung_V       = str2int(results[111].get_text())
            L3Leistung_W       = str2int(results[120].get_text())
            
            if printValue:
                print("\n\t\t*** Daten vom Wechselrichter ***\n\nBezeichnung\t\t\t Wert")
                print("Status \t\t\t\t "+Status)
                print("Aktuelleleistung \t\t " + str(Aktuelleleistung_W) + ' W')
                print("Gesamtenergie \t\t\t " + str(Gesamtenergie_kWh) + ' kWh')
                print("Tagesenergie \t\t\t " +  str(Tagesenergie_kWh) + ' kWh')
                print("String1 Spannung\t\t " + str(String1Spannung_V) + ' V')
                print("String1 Strom\t\t\t " + str(String1Strom_A) + ' A')
                print("String2 Spannung\t\t " + str(String2Spannung_V) + ' V')
                print("String2 Strom\t\t\t " + str(String2Strom_A) + ' A')
                print("String3 Spannung\t\t " + str(String3Spannung_V) + ' V')
                print("String3 Strom\t\t\t " + str(String3Strom_A) + ' A')
                print("Spannung L1\t\t\t " + str(L1Spannung_V) + ' V')
                print("Leistung L1\t\t\t " + str(L1Leistung_W) + ' W')
                print("Spannung L2\t\t\t " + str(L2Spannung_V) + ' V')
                print("Leistung L2\t\t\t " + str(L2Leistung_W) + ' W')
                print("Spannung L3\t\t\t " + str(L3Spannung_V) + ' V')
                print("Leistung L3\t\t\t " + str(L3Leistung_W) + ' W')

                if Aktuelleleistung_W > 0:
                    interval = intervalShort  
                
            if useMQTT:
                clientmqtt.publish(mqttprefix + "Aktuelleleistung",Aktuelleleistung_W)
                clientmqtt.publish(mqttprefix + "Gesamtenergie",(Gesamtenergie_kWh*1000))
                clientmqtt.publish(mqttprefix + "Tagesenergie",int(Tagesenergie_kWh*1000))
                clientmqtt.publish(mqttprefix + "String1/Spannung",String1Spannung_V)
                clientmqtt.publish(mqttprefix + "String1/Strom",String1Strom_A)
                clientmqtt.publish(mqttprefix + "String2/Spannung",String2Spannung_V)
                clientmqtt.publish(mqttprefix + "String2/Strom",String2Strom_A)
                clientmqtt.publish(mqttprefix + "String3/Spannung",String3Spannung_V)
                clientmqtt.publish(mqttprefix + "String3/Strom",String3Strom_A)
                clientmqtt.publish(mqttprefix + "L1/Spannung",L1Spannung_V)
                clientmqtt.publish(mqttprefix + "L1/Leistung",L1Leistung_W)
                clientmqtt.publish(mqttprefix + "L2/Spannung",L2Spannung_V)
                clientmqtt.publish(mqttprefix + "L2/Leistung",L2Leistung_W)
                clientmqtt.publish(mqttprefix + "L3/Spannung",L3Spannung_V)
                clientmqtt.publish(mqttprefix + "L3/Leistung",L3Leistung_W)
                if clientmqtt.publish(mqttprefix + "L3/Leistung",L3Leistung_W)[0] != 0:
                    print("Publish fehlgeschlagen!")                  
                    clientmqtt.connect(mqttBroker, mqttport)                    
        else:
            print("\n\t\t*** Daten vom Wechselrichter ***\n\nBezeichnung\t\t\t Wert")
            print("Status \t\t\t\t Ausgeschalten (oder nicht erreichbar!)")
            interval = intervalLong

    except Exception as e:
        print(str(e))             
    
    time.sleep(interval)
