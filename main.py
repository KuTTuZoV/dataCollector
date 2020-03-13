import requests
import json
import sqlite3
import time
import psycopg2
from psycopg2 import sql
import os

resultUrl = "https://line11.bkfon-resource.ru/line/updatesFromVersion/%version%/ru"
totalScore = 0

def getEventsList(event,tournamentID):
    try:
        if(event['sportId'] == tournamentID) and (event['level'] == 1):
            return event
    except:
        pass

def getTournamentsList(tournament):
    try:
        if(tournament['parentId'] == 1):
            return tournament
    except:
        pass

def getTables(tableName):
    if tableName[0] != '':
        return tableName[0]

def addEvent(event, tables):
    if not 'events' in tables:
        cursor.execute(
            "CREATE TABLE events (id INTEGER, tournamentID INTEGER, timecode INTEGER,team1	TEXT,team2	TEXT)")
        conn.commit()

    if (not ("event" + str(event[1]['id']) + "_" + str(event[1]['startTime']) in tables)):
        cursor.execute("CREATE TABLE event" + str(event[1]['id']) + "_" + str(event[1][
                                                                                  'startTime']) + " (id	SERIAL PRIMARY KEY UNIQUE,e INTEGER,f	INTEGER,v	REAL,p	INTEGER,pt	TEXT)")

        insert = "INSERT INTO events (id, tournamentid, timecode, team1, team2) values(%id%, %tourId%, %startTime%, '%team1%', '%team2%')"
        insert = insert.replace("%id%", str(event[1]['id']))
        insert = insert.replace("%tourId%", str(event[1]['sportId']))
        insert = insert.replace("%startTime%", str(event[1]['startTime']))
        insert = insert.replace("%team1%", str(event[1]['team1']))
        insert = insert.replace("%team2%", str(event[1]['team2']))

        cursor.execute(insert)

        conn.commit()

def insertFactor(factor):
    dbString = "INSERT INTO event" + str(factor['e']) + "_" + str(event[1]['startTime']) + "(e,f,v,p,pt) VALUES(%e%,%f%,%v%,%p%,\'%pt%\')"

    try:
        dbString = dbString.replace('%e%', str(factor['e']))
    except:
        dbString = dbString.replace('%e%', "-1")

    try:
        dbString = dbString.replace('%f%', str(factor['f']))
    except:
        dbString = dbString.replace('%f%', "-1")

    try:
        dbString = dbString.replace('%v%', str(factor['v']))
    except:
        dbString = dbString.replace('%v%', "-1")

    try:
        dbString = dbString.replace('%p%', str(factor['p']))
    except:
        dbString = dbString.replace('%p%', "-1")

    try:
        dbString = dbString.replace('%pt%', str(factor['pt']))
    except:
        dbString = dbString.replace('%pt%', "-1")

    try:
        cursor.execute(dbString)
    except:
        conn.commit()
        pass

def getAllTables():
    tables = list()

    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
    res = cursor.fetchall()

    for tablename in res:
        tables.append(tablename[0])

    return tables

def doRequest(version):
    resultUrl = "https://line11.bkfon-resource.ru/line/updatesFromVersion/%version%/ru"

    try:
        resultUrl = resultUrl.replace('%version%', str(version))
        response = requests.get(resultUrl)
    except:
        print('Connection fault')
        return "NONE"

    return json.loads(response.text)
def DB_connect():
    conn = psycopg2.connect(dbname='eventData', user='postgres',
                            password='12345678', host='91.239.26.202', port=5432)
    conn.autocommit = False

    cursor = conn.cursor()


    return conn, cursor


version = 0
tables = list()

'''
Алгоритм выполнения
1. Подключиться к БД +
2. Сделать запрос к Fonbet
3. Получить список турниров(Дополнить список турниров)
4. Добавить в базу отсутствующие турниры
5. Выбрать только те события, которые относятся к футболу
6. Создать таблицы для отсутствующих событий
'''

#Подключение к БД
conn, cursor = DB_connect()
tables = getAllTables()

try:
    cursor.execute("SELECT id FROM tournaments")
    tournamentsId = list(map(lambda x: x[0], cursor.fetchall()))

except:
    conn.commit()
    cursor.execute("CREATE TABLE tournaments (id INTEGER,name	TEXT)")
    conn.commit()

while 1:

    try:
        eventList = list()

        # Запрос данных о результатах
        result_parsed_string = doRequest(version)
        version = result_parsed_string['packetVersion']

        print("Этап 2. Данные о событиях получены")

        #3. Получить список турниров для которых есть коэффициенты
        resultList = list(filter(lambda t: getTournamentsList(t), result_parsed_string['sports']))
        newTournaments = list(filter(lambda x : x['id'] not in tournamentsId, resultList))

        print("Этап 3. Количество турниров: " + str(len(resultList)))

        if(len(newTournaments) > 0):
            print("Новых турниров: " + str(len(newTournaments)))

        # Дополнить список турниров содержащихся в БД
        for item in newTournaments:
            tournamentsId.append(item['id'])

        print("Общее количество турниров в базе: " + str(len(tournamentsId)))

        # Добавить в базу отсутствующие турниры
        for tournament in newTournaments:
            insert = "INSERT INTO tournaments (id, name) values(%id%, '%name%')"
            insert = insert.replace("%id%", str(tournament['id']))
            insert = insert.replace("%name%", str(tournament['name']))

        #5. Выбрать только те события, которые относятся к футболу

        for tournament in resultList:
            try:
                tempEventList = list(filter(lambda t: getEventsList(t,tournament['id']), result_parsed_string['events']))
                for tempEvent in tempEventList:
                    try:
                        eventList.append((tournament['id'],tempEvent))
                    except:
                        pass
            except:
                pass
        print("Количество футбольных событий: " + str(len(eventList)))

       #6 Создать таблицы для отсутствующих событий
        newEventList = list(filter(lambda x : 'event_' + str(x[1]['id']) + "_" + str(x[1]['startTime']) not in tables, eventList))

        i = 0

        print('Добавляем события в базу:')

        for event in newEventList:
            try:
                addEvent(event, tables)

                print('\r' + str(i), end='')
                i = i + 1
            except:
                conn.commit()
                pass

        conn.commit()
        i = 0

        print('\nДобавляем факторы в базу:')

        for event in newEventList:
            try:
                customFactors = list(filter(lambda t: t['e'] == event[1]['id'], result_parsed_string['customFactors']))

                k = 0
                for factor in customFactors:
                    insertFactor(factor)
                    print("\rСобытие {}. {} фактор из {}".format(i, k, len(customFactors)), end='')
                    k += 1

                conn.commit()
                i = i + 1
            except:
                conn.commit()
                pass
    except:
        pass

    conn.commit()

    print("Data collecting in process. Event count in database - " + str(len(tables)))
    for x in range(60):
        time.sleep(1)
#        print("Pause: {}".format(60-x),end="")

#    conn.commit()

    clear = lambda: os.system('clear') #on Linux System
    clear()

a = 5
