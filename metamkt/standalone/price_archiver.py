import common


def archive_prices():
    log = common.get_logger()
    log.info('Archiving Prices..')
    conn = common.get_connection()
    trans = conn.begin()

    try:
        #Insert new users
        conn.execute("""
                        INSERT INTO priceHistory (entity_id, price)
                        SELECT entity.id, entity.price FROM entity
                        WHERE (SELECT count(price) FROM pricehistory WHERE entity_id=entity.id)=0
                        AND price IS NOT NULL
                        """)
        #Update existing users
        conn.execute("""INSERT INTO priceHistory (entity_id, price)
                       SELECT entity.id, entity.price FROM entity
                       WHERE entity.price != (SELECT price FROM priceHistory
                       WHERE entity_id=entity.id ORDER BY timestamp DESC LIMIT 1)""")
        trans.commit()
    except:
        trans.rollback()
        raise
    conn.close()
    log.info('..done.')


def main():
    archive_prices()


if __name__ == "__main__":
    main()
