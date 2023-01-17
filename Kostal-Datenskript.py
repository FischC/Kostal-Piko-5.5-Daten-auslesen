from influxdb import InfluxDBClient
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import time


print("Datenbank verbindungsaufbau")
#Setup database
client = InfluxDBClient(host='192.168.1.99', port=8086, database='PvAnlage')


#Wechselrichter Anmeldedaten
url = 'http://192.168.1.21/'
nutzername = 'pvserver'
pw = 'Reit2196'


while 1:

    print("Datenabfrage")
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
    
    print(Aktuelleleistung_W)
    print("Datenabfrage Erfolgreich")

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
    except:
        json_payload = []
        daten = {
            "measurement":"wr",
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
        print("Wechselrichter ausgeschalten")
        print(datetime.now())
        time.sleep(5)
    json_payload.append(daten)
    
    
    
    #Senden der Daten
    client.write_points(json_payload,database="PvAnlage")
    time.sleep(5)


