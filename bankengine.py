import pymysql
import time
import config
import functions

while (1):

    try:
        db_toko = functions.connect_db(config.MYSQL_SETTINGS_1)
        cursor_toko = db_toko.cursor()

        db_bank = functions.connect_db(config.MYSQL_SETTINGS_2)
        cursor_bank = db_bank.cursor()

        result = functions.select(config.tables_1[0],cursor_bank)
        history = functions.select(config.histories_1[0],cursor_bank)

        print("result len = %d | history len = %d" % ( len(result),len(history) ))

        #insert listener
        if(len(result) > len(history)):
            print("-- INSERT DETECTED --")
            for data in result:
                a = 0
                for dataHistory in history:
                    if (data[0] == dataHistory[0]):
                        a = 1
                if (a == 0):
                    print("-- RUN INSERT FOR ID = %d" % (data[0]))
                    functions.insert(config.histories_1[0],data,cursor_bank,db_bank)
                    functions.insert(config.histories_1[0],data,cursor_toko,db_toko)
                    functions.insert(config.tables_1[0],data,cursor_toko,db_toko)

        #delete listener
        if(len(result) < len(history)):
            print("-- DELETE DETECTED --")
            for dataHistory in history:
                a = 0
                for data in result:
                    if (dataHistory[0] == data[0]):
                        a = 1
                if (a == 0):
                    print("-- RUN DELETE FOR ID = %d" % (dataHistory[0]))
                    functions.delete(config.histories_1[0],dataHistory,cursor_bank,db_bank)
                    functions.delete(config.histories_1[0],dataHistory,cursor_toko,db_toko)
                    functions.delete(config.tables_1[0],dataHistory,cursor_toko,db_toko)

        #update listener
        if(result != history):
            print("-- UPDATE DETECTED --")
            for data in result:
                for dataHistory in history:
                    if (data[0] == dataHistory[0]):
                        if(data != dataHistory):
                            functions.update(config.histories_1[0],data,cursor_bank,db_bank)
                            functions.update(config.histories_1[0],data,cursor_toko,db_toko)
                            functions.update(config.tables_1[0],data,cursor_toko,db_toko)
                

    except (pymysql.Error, pymysql.Warning) as e:
        print(e)

    # Untuk delay
    time.sleep(1)