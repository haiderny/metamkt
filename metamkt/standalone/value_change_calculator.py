import common


def calculate_value_change(conn, user_id, mysql_interval):
    conn.execute("""
                    select 
                    (select value from ValueHistory where user_id=%s order by timestamp desc limit 1) - 
                    (select value from ValueHistory where user_id=%s and timestamp < (utc_timestamp() - %s) 
                    order by timestamp desc limit 1)""" % (user_id, user_id, mysql_interval))
    change = conn.fetchall()[0][0]
    return change


def insert_value_change(conn, user_id, interval, value):
    conn.execute("""INSERT INTO ValueChange (user_id, term, value) VALUES (%s, '%s', %s)""" % 
                 (user_id, interval, value))


def calculate_value_changes():
    log = common.get_logger()
    log.info('Calculating value changes..')
    conn = common.get_connection()
    trans = conn.begin()
    
    try:
        conn.execute("DELETE FROM ValueChange where 1=1")
        
        conn.execute("SELECT id, value FROM User")
        users = conn.fetchall()
        for user in users:
            uid = user[0]

            change = calculate_value_change(conn, uid, "INTERVAL 1 DAY")
            if change is None:
                change = 0
            insert_value_change(conn, uid, "1D", change)
        
            change = calculate_value_change(conn, uid, "INTERVAL 7 DAY")
            if change is None:
                change = 0
            insert_value_change(conn, uid, "7D", change)
        
            change = calculate_value_change(conn, uid, "INTERVAL 1 MONTH")
            if change is None:
                change = 0
            insert_value_change(conn, uid, "1M", change)
        
            change = calculate_value_change(conn, uid, "INTERVAL 3 MONTH")
            if change is None:
                change = 0
            insert_value_change(conn, uid, "3M", change)
            
            change = calculate_value_change(conn, uid, "INTERVAL 6 MONTH")
            if change is None:
                change = 0
            insert_value_change(conn, uid, "6M", change)
        
            change = calculate_value_change(conn, uid, "INTERVAL 1 YEAR")
            if change is None:
                change = 0
            insert_value_change(conn, uid, "1Y", change)
        
        trans.commit()
    except:
        trans.rollback()
        raise
    conn.close()
    log.info('..done.')
