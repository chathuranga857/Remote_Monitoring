import json
import sqlite3
import threading

from flask import Flask
from paho.mqtt import client as mqtt_client


# Robot ID list which are under monitoring
RobotIDs = ["rob1", "rob2", "rob3", "rob4", "rob5", "rob6", "rob7", "rob8", "rob9", "rob10"]


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d



conn = sqlite3.connect(':memory:', check_same_thread=False)
#conn = sqlite3.connect('fastoryDB.db', check_same_thread=False)
conn.row_factory = dict_factory
c = conn.cursor()


#create a robot table in the DB
c.execute("""CREATE TABLE IF NOT EXISTS robot (
             id text,
             manufacturer text,
             wsID integer
             );""")


#create an event table in the DB
c.execute("""CREATE TABLE IF NOT EXISTS event (
             deviceId text,
             state text ,
             time TIMESTAMP
             );""")


# function to insert robots to the DB
def insert_robot(id, manufacturer, wsID):
    with conn:
        c.execute("INSERT INTO robot VALUES (:id,:manufacturer, :wsID)", {'id': id, 'manufacturer': manufacturer, 'wsID': wsID})


# function to insert events to the DB
def insert_event(deviceId,state, time):
    with conn:
        c.execute("INSERT INTO event VALUES (:deviceId, :state, :time)", {'deviceId': deviceId, 'state': state, 'time': time})
        print("printing the database..")
        print(get_all_events())


#READ
def get_all_robots():
    sqlSt="SELECT * FROM robot WHERE 1"
    c.execute(sqlSt)
    return c.fetchall()


def get_current_state_by_device_id(id):
    sqlSt = "SELECT * FROM event WHERE deviceId='"+id+"' ORDER BY time DESC"
    print("checking database")
    c.execute(sqlSt)
    eventsOfRobot = c.fetchall()
    print(eventsOfRobot[0]["state"])
    return eventsOfRobot[0]["state"]


def get_events_within_time (id, startTime, endTime):
    sqlSt = "SELECT r.id, r.manufacturer, r.wsID, e.rowid, e.state, e.time FROM robot AS r INNER JOIN event AS e ON r.id = e.deviceId WHERE r.id ='"+id+"' AND e.time BETWEEN '"+startTime+"' AND '"+endTime+"'"
    c.execute(sqlSt)
    return c.fetchall()


def get_latest_events(id, noOfItems):
    sqlSt = "SELECT * FROM event WHERE deviceId='"+id+"' ORDER BY time DESC LIMIT '"+noOfItems+"'"
    print("checking database")
    c.execute(sqlSt)
    return c.fetchall()


def get_all_events():
    sqlSt="SELECT * FROM event WHERE 1"
    c.execute(sqlSt)
    return c.fetchall()


# insert robot data manually
rob1 = RobotIDs[0]
insert_robot(rob1, 'ABB', 6)

rob2 = RobotIDs[1]
insert_robot(rob2, 'KUKA', 2)

rob3 = RobotIDs[2]
insert_robot(rob3, 'ABB', 3)

print("robot insertion done")

app = Flask(__name__)

threadStarted=False


@app.route('/hello', methods=['GET'])
def helloWorld():
    print("Hello world endpoint")
    return "Hello World"


@app.route('/start', methods=['GET'])
def startThreads():
    print("Start threads attempt")
    global threadStarted
    if (threadStarted):
        return "Threads have started already"
    else:
        threadStarted=True
        #Mqtt
        x = threading.Thread(target=startSubscription)
        x.start()
        return "Starting threads for mqtt subscription"


@app.route('/AllRobotIDs', methods=['GET'])
def robotIDs():
    print("robot IDs end point")
    jsonStr = json.dumps(RobotIDs)
    return jsonStr


@app.route('/CurrentStatus/<id>', methods=['GET'])
def currentStatus(id):
    print("current status end point")
    if id in RobotIDs:
        try:
            currentStatus = get_current_state_by_device_id(id)
            print("printing current status :", currentStatus)
            return currentStatus
        except:
            return "No data for : " + id
    else:
        return "Robot ID :" + id + " is not in the monitoring list"


@app.route('/LatestEvents/<id>/<noOfItems>', methods=['GET'])
def latestEvents(id, noOfItems):
    print("latest events end point")
    if id in RobotIDs:
        try:
            latestEvents = get_latest_events(id, noOfItems)
            print("printing latest events :", latestEvents)
            print(type(latestEvents))
            jsonLatestEvents = json.dumps(latestEvents)
            return jsonLatestEvents
        except:
            return "No data for : " + id
    else:
        return "Robot ID :" + id + " is not in the monitoring list"


@app.route('/EventHistory/<robID>/<startTime>/<endTime>', methods=['GET'])
def eventHistory(robID,startTime,endTime):
    print("event history end point")
    if robID in RobotIDs:
        try:
            eventHistory = get_events_within_time(robID,startTime,endTime)
            print("printing current status :", eventHistory)
            jsonEventHistory = json.dumps(eventHistory)
            return jsonEventHistory
        except:
            return "No data for : " + robID
    else:
        return "Robot ID :" + str(id) + " is not in the monitoring list"


# inserting actual events from mqtt broker
def on_event(client, userdata, message):
    eventString = str(message.payload, 'utf-8')
    print("received an mqtt message:")
    print(eventString)
    try:
        eventDic = json.loads(eventString)
    except:
        print("unexpected message format")
        return None

    if "deviceId" in eventDic and "state" in eventDic and "time" in eventDic:
        if eventDic["deviceId"] in RobotIDs:
            insert_event(eventDic["deviceId"], eventDic["state"], eventDic["time"])
        else:
            print("Robot ID is not in the monitoring list")
    else:
        print("unexpected body format")


#Mqtt thread
def startSubscription():
    topic = "ii22/telemetry/#"
    print("mqtt subscription started for topic "+topic)
    client = mqtt_client.Client()
    client.on_message = on_event
    client.connect("broker.mqttdashboard.com")
    client.subscribe(topic)#subscribe to events from all robots
    rc = 0
    while rc == 0:
        rc = client.loop()


if __name__ == '__main__':
    print(get_all_robots())
    print("****************")
    print("API starting..")
    app.run()



