import os
import sqlite3
import datetime
from datetime import timedelta
import pandas as pd
import mail
import settings

__debug__ is False

root = '/mnt/ftp/dados'

def createInverters(DBPath):
    conn = sqlite3.connect(DBPath)
    try:
        conn.execute("pragma foreign_keys") # enables foreign key
        #conn.execute("CREATE TABLE inverters(inv_name TEXT PRIMARY KEY UNIQUE, file INTEGER, status INTEGER, FOREIGN KEY(file) REFERENCES updates(file_id));")
        conn.execute("CREATE TABLE inverters(file INTEGER PRIMARY KEY UNIQUE, status INTEGER, FOREIGN KEY(file) REFERENCES updates(file_id));")
    except Exception as e:
        print(e)

def insertInverters(DBPath):
    errorTables = []
    conn = sqlite3.connect(DBPath)
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT local FROM files")
    campi = cur.fetchall()

    for campus in campi:
        #inversores
        cur.execute("SELECT * FROM files WHERE local = :local AND (tab_ou_tech LIKE 'mon%' OR tab_ou_tech LIKE 'pol%' OR tab_ou_tech = 'cdte' OR tab_ou_tech = 'cigs')",{'local':campus[0]})
        files = cur.fetchall()

        for file in files:
            #print(file[1]+'-'+file[2])
            #print(file)
            try:
                cur = conn.cursor()
                #cur.execute("INSERT INTO inverters (inv_name, file, status) VALUES (:inv_name, :file, :status);", {'inv_name':(file[1]+'-'+file[2]), 'file': file[0], 'status': 0})
                cur.execute("INSERT INTO inverters (file, status) VALUES (:file, :status);", {'file': file[0], 'status': 0})
            except Exception as e:
                print(e)
                
    conn.commit()
    conn.close()

def checkMode3(DBPath):
    errorTables = []
    conn = sqlite3.connect(DBPath)
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT local FROM files")
    campi = cur.fetchall()
    
    for campus in campi:
        #inversores
        cur.execute("SELECT * FROM files WHERE local = :local AND (tab_ou_tech LIKE 'mon%' OR tab_ou_tech LIKE 'pol%' OR tab_ou_tech = 'cdte' OR tab_ou_tech = 'cigs')",{'local':campus[0]})
        files = cur.fetchall()
        cur = conn.cursor()

        for file in files:
            cur.execute("SELECT measure_time, last_update_in_s FROM updates WHERE file_id = :file ORDER BY id DESC LIMIT 1",{'file':file[0]})
            result = cur.fetchone()
            time = datetime.datetime.utcnow()# - timedelta(hours = 2)
            measure_time = datetime.datetime.strptime(result[0],'%Y-%m-%dT%H:%M:%S')            
            no_update_time = int((time-measure_time).total_seconds() + result[1])   
            # we only need files that are still being updated
            if no_update_time < 3600:
                #cur.execute("UPDATE files SET status = 0 WHERE id = :file",{'file':file[0]})
                # rebuilds folder path. Extra steps needed thanks to the nth name change
                if file[2][0:3] == 'mon':
                    path = "{:04}/{:02}/inversores/mono/".format(time.year, time.month)
                elif file[2][0:3] == 'pol':
                    path = "{:04}/{:02}/inversores/poli/".format(time.year, time.month)
                else:
                    path = "{:04}/{:02}/inversores/{}/".format(time.year, time.month, file[2])
                try:
                    # read_csv may fail to read the file for various reasons, outputting an error.
                    # csv is accessed by grouping root, rebuilt path and rebuilt filename proper
                    # only the timestamp and the mode line are needed for this operation.
                    frame = pd.read_csv(root + path + file[1]+'-'+file[2]+'-'+time.strftime("%y")+"-"+time.strftime("%m")+"-"+time.strftime("%d")+'.csv', parse_dates = ['timestamp_iso'], error_bad_lines=False, skip_blank_lines=True, usecols = ['timestamp_iso', 'mod'])
                    # setting the timestamp as the index to avoid DatetimeIndex errors.
                    frame = frame.set_index(pd.DatetimeIndex(frame['timestamp_iso']))
                    #frame.dropna(inplace=True)
                    if '11:00' <= time.strftime('%H:%M') <= '24:00': # times in UTC  12 19
                        frame = frame.between_time((time - timedelta(hours = 1)).strftime('%H:%M'), time.strftime('%H:%M'))
                        num_lines = len(frame)
                        num_mod3 = len(frame.query('mod == 3'))
                        if __debug__: print('File: ' + root + path + file[1]+'-'+file[2]+'-'+time.strftime("%y")+"-"+time.strftime("%m")+"-"+time.strftime("%d")+'.csv' + ', Last update: ' + str(no_update_time) + 's')
                        # if more than 60% of the lines from the last hour have mod3
                        # we can assume there are issues with data acquisition
                        cur.execute('SELECT status FROM inverters WHERE file = :file;', {'file':file[0]})
                        inv_status = cur.fetchone()[0]
                        if ((num_mod3 >= int(num_lines*0.6)) and num_lines > 1) :
                            # inv_status == 0 means the inverter was previously sending data by RS485,
                            # so we change it to 1
                            if(inv_status == 0):
                                if __debug__: print('Acquisition error. Total: {}, Mode 3: {}'.format(num_lines, num_mod3))
                                cur.execute('UPDATE inverters SET status = 1 WHERE file = :file;', {'file':file[0]})
                                errorTables.append(file[1]+'-'+file[2])
                                print(errorTables)
                            elif(inv_status == 1):
                                if __debug__: print('Already reported. Total: {}, Mode 3: {}'.format(num_lines, num_mod3))
                            
                        elif(num_lines > 1):
                            # if file is not empty and the status was set to 1, inverter is working again. 
                            if(inv_status == 1):
                                if __debug__: print('Ok. Total: {}, Mode 3: {}'.format(num_lines, num_mod3))
                                cur.execute('UPDATE inverters SET status = 0 WHERE file = :file;', {'file':file[0]})
                            else: 
                                if __debug__: print('Already ok. Total: {}, Mode 3: {}'.format(num_lines, num_mod3))

                            
                        if __debug__: print('_' * 80)
                        
                except Exception as e:
                    print(e)
                    print('File: ' + root + path + file[1]+'-'+file[2]+'-'+time.strftime("%y")+"-"+time.strftime("%m")+"-"+time.strftime("%d")+'.csv')
                    print('_' * 80)
                    
    conn.commit()            
    conn.close()
    frame = ''
    
    return errorTables
    
def main():
    createInverters(settings.DBPath+'/database.db')
    insertInverters(settings.DBPath+'/database.db')
    
    files = checkMode3(settings.DBPath+'/database.db')

    if not files == []:
        mailAddresses = mail.getMailAddresses()

        if not mailAddresses == []:
            if len(files) > 1:
                msgHtml = mail.renderHTML('email-modo-3-multi.html',{'tabelas':files})
            else:
                msgHtml = mail.renderHTML('email-modo-3.html',{'tabelas':files})

            mail.sendMailHTML(settings.mailServer['serverAddress'],settings.mailServer['serverPort'],settings.mailServer['user'],settings.mailServer['passwd'],mailAddresses,'Monitoramento EPESOLs',msgHtml)

if __name__ == '__main__':
    main()