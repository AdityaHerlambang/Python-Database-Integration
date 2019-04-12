import pymysql
import time

def synchronize(cursor):
    sql_select = "SELECT * FROM tb_invoice"
    cursor.execute(sql_select)

    results = cursor.fetchall()

    return results

def insertData(data,cursor,db):
    sql = "INSERT INTO tb_invoice(invoice_id,total_harga,total_terbayar,flag_bank) VALUES (%s, %s, %s, %s)"
    val = (data[0], data[1], data[2], data[3])
    
    cursor.execute(sql, val)
    db.commit()

    return 1

def updateData(data,cursor,db):
    sql = "UPDATE tb_invoice SET total_harga = '%s', total_terbayar = '%s', flag_bank = '%s' WHERE invoice_id = '%s'"
    val = (data[1], data[2], data[3], data[0])
    
    cursor.execute(sql, val)
    db.commit()

    return 1

def checkDataToko(data_toko, data_bank, cursor_bank, db_at_bank):
    print("\n--------- TOKO : CHECK DATA ---------")

    #INSERT
    if (len(data_bank) < len(data_toko)):
        print("\n--------- TOKO : DATA INSERT EVENT DETECTED ----------")
        i = 0
        for dataToko in data_toko:
            for dataBank in data_bank:
                if (dataToko[0] == dataBank[0]):
                    i = 1
            if (i == 0):
                insertData(dataToko,cursor_bank,db_at_bank)
            else:
                i = 0


def checkDataBank(data_toko, data_bank, cursor_toko, db_at_toko):
    print("\n--------- BANK : CHECK DATA ---------")

    if (len(data_toko) < len(data_bank)):
        print("\n--------- BANK : DATA INSERT EVENT DETECTED ----------")
        i = 0
        for dataBank in data_bank:
            for dataToko in data_toko:
                if (dataBank[0] == dataToko[0]):
                    i = 1
            if (i == 0):
                insertData(dataBank,cursor_toko,db_at_toko)
            else:
                i = 0
    #UPDATE
    elif (len(data_bank) == len(data_toko)):
        for dataBank in data_bank:
            for dataToko in data_toko:
                a = 0
                if (dataToko[0] == dataBank[0]):
                    while a < len(dataBank):
                        if(dataToko[a] != dataBank[a]):
                            print("\n--------- TOKO : DATA UPDATE EVENT DETECTED ----------")
                            print("\n data toko = %s data bank = %s" % (dataToko[a], dataBank[a]))
                            updateData(dataBank,cursor_toko,db_at_toko)
                            i = 1
                        a += 1

while (1):

    try:
        db_at_bank = pymysql.connect("localhost", "root", "", "db_sync_tokobank_bank")
        cursor_bank = db_at_bank.cursor()

        db_at_toko = pymysql.connect("localhost", "root", "", "db_sync_tokobank_toko")
        cursor_toko = db_at_toko.cursor()

        data_bank = synchronize(cursor_bank)
        data_toko = synchronize(cursor_toko)

        # if (len(data_toko) < len(data_bank) or len(data_toko) < len(data_bank)):
        checkDataToko(data_toko, data_bank, cursor_bank, db_at_bank)
        checkDataBank(data_toko, data_bank, cursor_toko, db_at_toko)

    except (pymysql.Error, pymysql.Warning) as e:
        print(e)

    # Untuk delay
    time.sleep(1)