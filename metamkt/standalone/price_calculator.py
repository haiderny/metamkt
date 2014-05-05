import argparse
import common


def calc_prices(include_untouched=False):
    log = common.get_logger()
    log.info('Calculating Prices..')
    conn = common.get_connection()
    trans = conn.begin()
    try:
        sql = """update Entity
                set price = (select (sum(quantity * cost)/sum(quantity)) from Shares where entity_id = Entity.id ),
                touched = 0 where Entity.touched=1"""
        if include_untouched:
            sql += ' or Entity.touched=0'
        conn.execute(sql)
        trans.commit()
    except:
        trans.rollback()
        raise
    conn.close()
    log.info('..done.')


def main():
    parser = argparse.ArgumentParser(description='Calculate entity prices.')
    parser.add_argument('-all', default=False, type=bool, choices=[True, False],
                        help='If true, also update prices of untouched entities.')
    args = parser.parse_args()
    calc_prices(args.all)


if __name__ == "__main__":
    main()