import common


def calculate_values():
    log = common.get_logger()
    log.info('Calculating Values..')
    conn = common.get_connection()
    trans = conn.begin()
    try:
        #Step 1: Calculate all prices
        conn.execute("""update User
        set value = ((  select sum(Shares.quantity * Entity.price)  from Shares
                        inner join Entity on Entity.id = entity_id where Shares.user_id=User.id) + User.cash),
        valueTouched = 0
        where User.valueTouched=1""")

        #Step 2: Set all null prices equal to cash, i.e. for users who don't own shares.
        conn.execute("""UPDATE User set User.value = User.cash where
                    (select count(id) from Shares where Shares.user_id=User.id)=0 """)
        trans.commit()
    except:
        trans.rollback()
        raise
    conn.close()
    log.info('..done.')


def main():
    calculate_values()


if __name__ == "__main__":
    main()