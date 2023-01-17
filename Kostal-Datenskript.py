from influxdb import InfluxDBClient
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import time
import paho.mqtt.client as mqtt
import os

#Wechselrichter Anmeldedaten
url = 'http://192.168.1.21/'    #Ip des Wechselrichters im Heimnetzwerk 
nutzername = 'pvserver'
pw = 'Reit2196'

#Datenbank Ausgabe Definieren mir Ture/False
useDatabase = True
measurement = "wr"

#Setup database
if useDatabase:
    client = InfluxDBClient(host='192.168.1.99', port=8086, database='PvAnlage')

#Daten Ausgabe Komandozeile Ture/False
printValue = True

#Daten über MQTT Senden Ture/False
useMQTT = True
#MQTT Broker IP adresse Eingeben ohne Port!
mqttBroker = "192.168.1.99"
mqttuser = ""
mqttpasswort = ""
mqttport = 1883
mqttprefix = "Wechselrichter/Kostal/"

#MQTT Init
if useMQTT:
    try:
        clientmqtt = mqtt.Client("SmartMeter")
        clientmqtt.username_pw_set(mqttuser, mqttpasswort)
        clientmqtt.connect(mqttBroker, mqttport)
    except:
        print("Die Ip Adresse des Brokers ist falsch!")
        sys.exit()



while 1:

    page = requests.get(url,auth=(nutzername,pw ))
    soup = BeautifulSoup(page.content, 'html.parser')
    results = soup.find_all('td')
    
    
    Aktuelleleistung_W = results[14].get_text()
    Gesamtenergie_kWh  = results[17].get_text()
    Tagesenergie_kWh   = results[26].get_text()
    String1Spannung_V   = results[56].get_text()
    String1Strom_A      = results[65].get_text()
    String2Spannung_V   = results[82].get_text()
    String2Strom_A      = results[91].get_text()
    String3Spannung_V  = results[108].get_text()
    String3Strom_A     = results[117].get_text()
    L1Spannung_V       = results[59].get_text()
    L1Leistung_W       = results[68].get_text()
    L2Spannung_V       = results[85].get_text()
    L2Leistung_W       = results[94].get_text()
    L3Spannung_V       = results[111].get_text()
    L3Leistung_W       = results[120].get_text()
    Zeitpunkt          = datetime.now()
    

    #Setup Payload
    try:
        json_payload = []
        daten = {
            "measurement":"wr",
            "tags": {
                "wr": "kostal"
                },
            "fields": {
                'Aktuelleleistung_W': float(Aktuelleleistung_W),
                'Gesamtenergie_kWh' : float(Gesamtenergie_kWh),
                'Tagesenergie_kWh'  : float(Tagesenergie_kWh),
                'String1Spannung_V'  : float(String1Spannung_V),
                'String1Strom_A'     : float(String1Strom_A),
                'String2Spannung_V'  : float(String2Spannung_V),
                'String2Strom_A'     : float(String2Strom_A),
                'String3Spannung_V' : float(String3Spannung_V),
                'String3Strom_A'     : float(String3Strom_A),
                'L1Spannung_V'      : float(L1Spannung_V),
                'L1Leistung_W'      : float(L1Leistung_W),
                'L2Spannung_V'      : float(L2Spannung_V),
                'L2Leistung_W'      : float(L2Leistung_W),
                'L3Spannung_V'      : float(L3Spannung_V),
                'L3Leistung_W'      : float(L3Leistung_W),
            
                }
        
        
        }

        if printValue:
            print("\n\t\t*** Daten vom Wechselrichter ***\n\nBezeichnung\t\t\t Wert")
            print("Status \t\t\t\t Eingeschalten")
            print("Aktuelleleistung \t\t") + Aktuelleleistung_W
            print("Gesamtenergie \t\t\t 0") + Gesamtenergie_kWh
            print("Tagesenergie \t\t\t 0") +  Tagesenergie_kWh
            print("String1 Spannung\t\t\t 0") + String1Spannung_V
            print("String1 Strom\t\t\t 0") + String1Strom_A
            print("String2 Spannung\t\t\t 0") + String2Spannung_V
            print("String2 Strom\t\t\t 0") + String2Strom_A
            print("String3 Spannung\t\t\t 0") + String3Spannung_V
            print("String3 Strom\t\t\t 0") + String3Strom_A
            print("Spannung L1\t\t\t 0") + L1Spannung_V
            print("Leistung L1\t\t\t 0") + L1Leistung_W
            print("Spannung L2\t\t\t 0") + L2Spannung_V
            print("Leistung L2\t\t\t 0") + L2Leistung_W
            print("Spannung L3\t\t\t 0") + L3Spannung_V
            print("Leistung L3\t\t\t 0") + L3Leistung_W
            
        if useMQTT:
            clientmqtt.publish(mqttprefix + "Aktuelleleistung",Aktuelleleistung_W)
            clientmqtt.publish(mqttprefix + "Gesamtenergie",Gesamtenergie_kWh)
            clientmqtt.publish(mqttprefix + "Tagesenergie",Tagesenergie_kWh)
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

    except:
        json_payload = []
        daten = {
            "measurement":measurement,
            "tags": {
                "wr": "kostal"
                },
            "fields": {
                'Aktuelleleistung_W': 0.0,
                'Gesamtenergie_kWh' : 0.0,
                'Tagesenergie_kWh'  : 0.0,
                'String1Spannung_V'  : 0.0,
                'String1Strom_A'     : 0.0,
                'String2Spannung_V'  : 0.0,
                'String2Strom_A'     : 0.0,
                'String3Spannung_V' : 0.0,
                'String3Strom_A'     : 0.0,
                'L1Spannung_V'      : 0.0,
                'L1Leistung_W'      : 0.0,
                'L2Spannung_V'      : 0.0,
                'L2Leistung_W'      : 0.0,
                'L3Spannung_V'      : 0.0,
                'L3Leistung_W'      : 0.0,
            
                }
        
        
        }
        if printValue:
            print("\n\t\t*** Daten vom Wechselrichter ***\n\nBezeichnung\t\t\t Wert")
            print("Status \t\t\t\t Ausgeschalten")
        if useMQTT:
            clientmqtt.publish(mqttprefix + "Aktuelleleistung",0)
            clientmqtt.publish(mqttprefix + "Gesamtenergie",0)
            clientmqtt.publish(mqttprefix + "Tagesenergie",0)
            clientmqtt.publish(mqttprefix + "String1/Spannung",0)
            clientmqtt.publish(mqttprefix + "String1/Strom",0)
            clientmqtt.publish(mqttprefix + "String2/Spannung",0)
            clientmqtt.publish(mqttprefix + "String2/Strom",0)
            clientmqtt.publish(mqttprefix + "String3/Spannung",0)
            clientmqtt.publish(mqttprefix + "String3/Strom",0)
            clientmqtt.publish(mqttprefix + "L1/Spannung",0)
            clientmqtt.publish(mqttprefix + "L1/Leistung",0)
            clientmqtt.publish(mqttprefix + "L2/Spannung",0)
            clientmqtt.publish(mqttprefix + "L2/Leistung",0)
            clientmqtt.publish(mqttprefix + "L3/Spannung",0)
            clientmqtt.publish(mqttprefix + "L3/Leistung",0)
    time.sleep(5)
    json_payload.append(daten)
    
    
    
    #Senden der Daten
    if useDatabase:
        client.write_points(json_payload)
        #print("Daten gesendet")
    time.sleep(5)


