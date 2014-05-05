import common


def calculate_price_change(conn, entity_id, mysql_interval):
    conn.execute("""
                    select 
                    (select price from PriceHistory where entity_id=%s order by timestamp desc limit 1) - 
                    (select price from PriceHistory where entity_id=%s and timestamp < (utc_timestamp() - %s) 
                    order by timestamp desc limit 1)""" % (entity_id, entity_id, mysql_interval))
    change = conn.fetchall()[0][0]
    return change


def insert_price_change(conn, entity_id, interval, value):
    conn.execute("""INSERT INTO PriceChange (entity_id, term, value) VALUES (%s, '%s', %s)""" %
                 (entity_id, interval, value))


def calculate_points_changes():
    log = common.get_logger()
    log.info('Calculating points changes..')
    conn = common.get_connection()
    trans = conn.begin()
    
    try:
        trans.execute("DELETE FROM PointsChange where 1=1")
        
        result = conn.execute("SELECT id FROM Entity")
        users = result.fetchall()
        for user in users:
            eid = user[0]
            
            change = calculate_price_change(conn, eid, "INTERVAL 1 DAY")
            if change is not None: 
                insert_price_change(conn, eid, "1D", change)
        
            change = calculate_price_change(conn, eid, "INTERVAL 7 DAY")
            if change is not None: 
                insert_price_change(conn, eid, "7D", change)
        
            change = calculate_price_change(conn, eid, "INTERVAL 3 MONTH")
            if change is not None: 
                insert_price_change(conn, eid, "3M", change)
            
            change = calculate_price_change(conn, eid, "INTERVAL 6 MONTH")
            if change is not None: 
                insert_price_change(conn, eid, "6M", change)
        
            change = calculate_price_change(conn, eid, "INTERVAL 1 YEAR")
            if change is not None: 
                insert_price_change(conn, eid, "1Y", change)
        
        trans.commit()
    except:
        trans.rollback()
        raise
    conn.close()
    log.info('..done.')


def main():
    calculate_points_changes()


if __name__ == "__main__":
    main()