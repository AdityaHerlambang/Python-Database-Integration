import pymysql

def connect_db(config):
    conn = pymysql.connect(config["host"], config["username"], config["password"], config["db_name"])
    return conn

def update(table, data, cursor, db):
    try:
        sql = "UPDATE "+table+" SET total_harga = '%s', total_terbayar = '%s', flag_bank = '%s' WHERE invoice_id = '%s'"
        val = (data[1], data[2], data[3], data[0])
        
        cursor.execute(sql, val)
        db.commit()
    except (pymysql.Error, pymysql.Warning) as e:
        print(e)

    return 1

def insert(table, data, cursor, db):
    sql = "INSERT INTO "+table+"(invoice_id,total_harga,total_terbayar,flag_bank) VALUES (%s, %s, %s, %s)"
    val = (data[0], data[1], data[2], data[3])
    
    cursor.execute(sql, val)
    db.commit()

    return 1

def delete(table, data, cursor, db):
    sql = "DELETE FROM "+table+" WHERE invoice_id = '%s'"
    val = (data[0])
    
    cursor.execute(sql, val)
    db.commit()

    return 1

def select(table, cursor):
    sql_select = "SELECT * FROM "+table
    cursor.execute(sql_select)

    results = cursor.fetchall()

    return results