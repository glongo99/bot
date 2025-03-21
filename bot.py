import pyodbc
import requests
import random
import time
import json
import threading
import paho.mqtt.client as mqtt
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from shapely.geometry import Point
from datetime import datetime
import asyncio
import os

# Configurazione database Azure SQL
DB_SERVER = "provapoliba.database.windows.net"
DB_NAME = "provapoliba"
DB_USERNAME = "prova"
DB_PASSWORD = "Test1234!"

conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={DB_SERVER};DATABASE={DB_NAME};UID={DB_USERNAME};PWD={DB_PASSWORD}'

# Configurazione MQTT
MQTT_BROKER = "eu1.cloud.thethings.network"
MQTT_PORT = 1883  
MQTT_TOPIC = "v3/longo-dipalma@ttn/devices/longo-dipalma-scheda/up"
MQTT_USERNAME = "longo-dipalma@ttn"
MQTT_PASSWORD = "NNSXS.SPXJTON7UDQ7ZQJIEKG355QKIAZ7Y6NE5XKFP3A.RQAQND2KNPBYIA7ZHRDS76JP26JUGVXJTZ6TGIRUHW6UPWF65YQQ"

# Token del bot Telegram
TOKEN = '7567669181:AAGBkTbUv1Nv4yoZr6-Xg9dsMFFj_SvXP90'
CHAT_ID = '210248450'

tracking_attivo = False
centro_area_sicura = Point(41.10926, 16.87784)
center_lat = centro_area_sicura.x
center_lon = centro_area_sicura.y
raggio_sicuro = 0.001

# Funzione per il comando /start
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Ciao! Questo bot ti avviserà se il dispositivo LoRa esce dall'area.")
    
# Funzione per il comando /help
async def help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Questo bot invia notifiche quando il dispositivo LoRa esce dall'area.")

# Funzione per il comando /stop
async def stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🔴 Il bot sta per essere arrestato.")
    
    # Ottenere il loop corrente e fermarlo
    loop = asyncio.get_event_loop()
    loop.stop()

    # Se il bot usa `Application`, possiamo chiuderlo in modo sicuro
    application = context.application
    if application:
        application.stop()

    sys.exit(0) 
    
# Funzione per inviare un alert
def send_alert(posizione, temperatura, batteria, first_alert=False):
    lat, lon = posizione.x, posizione.y
    messaggio = (f"🚨 ATTENZIONE! Il dispositivo è uscito dall'area!\n📍 Posizione: {lat}, {lon} "
                 f"Batteria: {batteria}" f"Temperatura: {temperatura}") if first_alert else f"📍 Aggiornamento posizione:\nLat: {lat}, Lon: {lon} \n Batteria: {batteria} Temperatura: {temperatura}"

    try:
        requests.get(f"https://api.telegram.org/bot{TOKEN}/sendMessage",
                     params={"chat_id": CHAT_ID, "text": messaggio}, timeout=10)

        requests.get(f"https://api.telegram.org/bot{TOKEN}/sendLocation",
                     params={"chat_id": CHAT_ID, "latitude": lat, "longitude": lon}, timeout=10)
    except requests.exceptions.Timeout:
        print("❌ Errore: Timeout nella richiesta a Telegram.")
    except requests.exceptions.RequestException as e:
        print(f"❌ Errore nella richiesta a Telegram: {e}")

# Funzione per notificare il rientro nell'area
def send_return_message(posizione):
    lat, lon = posizione.x, posizione.y
    messaggio = "✅ Il dispositivo è rientrato nell'area sicura."
    requests.get(f"https://api.telegram.org/bot{TOKEN}/sendMessage",
                 params={"chat_id": CHAT_ID, "text": messaggio})
    requests.get(f"https://api.telegram.org/bot{TOKEN}/sendLocation",
                 params={"chat_id": CHAT_ID, "latitude": lat, "longitude": lon})


# Funzione per la simulazione
async def test(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global tracking_attivo
    print("🔄 Avvio test di simulazione della posizione...")
    while True:
        lat = random.uniform(45.06, 45.08)
        lon = random.uniform(7.68, 7.70)
        posizione = Point(lat, lon)
        batteria = random.randint(10, 100)

        print(f"📍 Posizione simulata: {lat}, {lon}")

        if centro_area_sicura.distance(posizione) > raggio_sicuro:
            if not tracking_attivo:
                tracking_attivo = True
                send_alert(posizione, batteria, first_alert=True)
            else:
                send_alert(posizione, batteria, first_alert=False)
        else:
            if tracking_attivo:
                tracking_attivo = False
                send_return_message()

        time.sleep(10)

def write_to_db(lat, lon, temperatura, batteria):
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        query = """
        INSERT INTO Sensors (timestamp, lat, lon, center_lat, center_lon, temperatura, batteria)
        VALUES (?, ?, ?, ?, ?)
        """
        cursor.execute(query, (datetime.now(), lat, lon, cen_lat, cen_lon, temperatura, batteria))
        conn.commit()
        cursor.close()
        conn.close()
        print("✅ Dati salvati su database.")
    except Exception as e:
        print(f"❌ Errore nel salvataggio dati: {e}")


# Funzione per la gestione dei messaggi MQTT
def on_message(client, userdata, msg):
    global tracking_attivo
    try:
        payload = json.loads(msg.payload.decode())
        #print(f"📩 Payload ricevuto: {payload}")  # Debug

        uplink_msg = payload.get("uplink_message", {}).get("decoded_payload", {})
        lat = uplink_msg.get("latitude")
        lon = uplink_msg.get("longitude")
        temperatura = uplink_msg.get("temp")
        batteria = uplink_msg.get("battery")

        if lat is None or lon is None or temperatura is None or batteria is None:
           lat = 41.10926 
           lon =  16.87784 
           temperatura = 25.0 
           batteria = 80

        write_to_db(lat, lon, temperatura, batteria)
        posizione = Point(lat, lon)
        if centro_area_sicura.distance(posizione) > raggio_sicuro:
            if not tracking_attivo:
                tracking_attivo = True
                send_alert(posizione, temperatura, batteria, first_alert=True)
            else:
                send_alert(posizione, temperatura, batteria, first_alert=False)
        else:
            if tracking_attivo:
                tracking_attivo = False
                send_return_message(posizione)

    except json.JSONDecodeError as e:
        print(f"❌ Errore di parsing JSON: {e}")
    except KeyError as e:
        print(f"❌ Errore nel parsing del payload, chiave mancante: {e}")
    except Exception as e:
        print(f"❌ Errore generale in on_message: {e}")


# Funzione per avviare il client MQTT in un thread separato
def start_mqtt():
    while True:
        try:
            client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
            client.username_pw_set(username=MQTT_USERNAME, password=MQTT_PASSWORD)
            client.on_message = on_message
            client.connect(MQTT_BROKER, MQTT_PORT, 60)
            client.subscribe(MQTT_TOPIC)
            client.loop_forever()
        except Exception as e:
            print(f"❌ Errore MQTT: {e}, riavvio in 5 secondi...")
            time.sleep(5)  # Retry after 5 seconds
# Funzione principale per il bot Telegram
def main():
    app = Application.builder().token(TOKEN).build()

    # Aggiungere i comandi al bot
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help))
    app.add_handler(CommandHandler("test", test))
    app.add_handler(CommandHandler("stop", stop))
    

    # Avvia il client MQTT in un thread separato
    mqtt_thread = threading.Thread(target=start_mqtt, daemon=True)
    mqtt_thread.start()

    # Avvia il bot Telegram
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


### Esegui il bot
if __name__ == "__main__":
   main()




