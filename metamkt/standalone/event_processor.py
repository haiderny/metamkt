import common
import sys


def process_events(start_timestamp, end_timestamp, override):
    log = common.get_logger()
    log.info('Processing events..')
    conn = common.get_connection()
    trans = conn.begin()

    try:
        if override not in ['True', 'False']:
            print 'Invalid override argument.'
            sys.exit()
        #Find all events in the desired time range
        result = conn.execute("""
            SELECT Event.id, Event.entity_id, Event.action_id, Event.eventTime, (Action.points * Event.quantity)
            FROM Event
            INNER JOIN Action ON Event.action_id = Action.id
            WHERE Event.eventTime between %s and %s """ % (start_timestamp, end_timestamp))
        rows = result.fetchall()
        for row in rows:
            event_id = row[0]
            entity_id = row[1]
            event_time = row[3]
            points = row[4]

            #Find all users owning related shares
            result = conn.execute("""
                select User.id, Shares.quantity from User
                INNER JOIN Shares ON User.id = Shares.user_id
                WHERE Shares.entity_id = %s AND (('%s' BETWEEN Shares.startTime and Shares.endTime)
                OR ('%s' > Shares.startTime AND Shares.endTime = '00000000') )
                """ % (entity_id, event_time, event_time))
            user_rows = result.fetchall()
            for userRow in user_rows:
                user_id = userRow[0]
                num_shares = userRow[1]

                result = conn.execute("""
                    select COUNT(*) FROM Points
                    WHERE Points.event_id = %s AND Points.user_id = %s""" % (event_id, user_id))
                res = result.fetchone()
                existing_records = res[0]
                if override == 'False' and existing_records > 0:
                    continue

                amount = num_shares * points
                conn.execute("""
                    INSERT INTO Points (user_id, event_id, amount)
                    VALUES (%s, %s, %s)
                    """ % (user_id, event_id, amount))

                print user_id, event_id, amount
        trans.commit()
    except:
        trans.rollback()
        raise
    conn.close()
    log.info('..done.')


def main():
    start_timestamp = sys.argv[1]
    end_timestamp = sys.argv[2]
    override = sys.argv[3]
    process_events(start_timestamp, end_timestamp, override)


if __name__ == "__main__":
    main()