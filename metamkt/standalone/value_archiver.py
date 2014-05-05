import common


def archive_values():
    log = common.get_logger()
    log.info('Archiving values..')
    conn = common.get_connection()
    trans = conn.begin()
    try:
        #Insert new users
        conn.execute("""
                        INSERT INTO ValueHistory (user_id, value)
                        (select id, value from User where
                        (select count(value) from ValueHistory where user_id=User.id)=0
                        and value is not NULL)
                        """)

        #Update existing users
        conn.execute("""INSERT INTO ValueHistory (user_id, value)
                       SELECT User.id, User.value FROM User
                       WHERE User.value != (SELECT value FROM ValueHistory
                       WHERE user_id=User.id ORDER BY timestamp DESC LIMIT 1)""")
        trans.commit()
    except:
        trans.rollback()
        raise
    conn.close()
    log.info('..done.')


def main():
    archive_values()


if __name__ == "__main__":
    main()