# Polls for buy orders from users and attempts to satisfy them with matching sell orders or free shares.

import common
import logging
import time
from decimal import *


# Given the minimum and maximum desired price of buyer and seller,
# establish fair price at which a transaction should be conducted.
def find_fair_price(min_buy_price, max_buy_price, min_sell_price, max_sell_price):
    points = [min_buy_price, max_buy_price, min_sell_price, max_sell_price]
    points.sort()
    return (points[1] + points[2]) / Decimal(2.0) 


# For a given user, get total cash balance.
def get_user_cash(cursor, user_id):
    sql = """select cash from user where id=%s""" % user_id
    cursor.execute(sql)
    res = cursor.fetchone()
    user_cash = res[0]
    return user_cash


# Deduct a user's shares of an entity by a given transaction quantity.
def deduct_user_shares(cursor, user_id, entity_id, trans_qty):
    # Get all active holdings of the entity by the user.
    sql = """select id, quantity, cost from shares
          where user_id=%s and entity_id=%s and active!=0
          order by quantity asc""" % (user_id, entity_id)
    cursor.execute(sql)
    shares = cursor.fetchall()
    for share in shares:
        # For each holding, deduct shares as appropriate.
        logging.debug("%s seller shares left to deduct." % trans_qty)
        if trans_qty == 0:
            continue
        share_id = share[0]
        share_qty = share[1]
        share_cost = share[2]
        if share_qty <= trans_qty:
            # Deduct current holding if less than transaction quantity.
            sql = """UPDATE shares set endTime=now(), active=0 where id=%s""" % share_id
            cursor.execute(sql)
            trans_qty = trans_qty - share_qty
            logging.debug("Deducting shares with ID %s" % share_id)
        else:
            # Break up holding if more than required transaction quantity.
            # First, create new holding with remaining shares..
            remainder = share_qty - trans_qty
            sql = """INSERT into shares (user_id, entity_id, quantity, cost, startTime, endTime, active) 
                    VALUES (%s, %s, %s, %s, now(), '0000-00-00 00:00:00', 1)"""\
                  % (user_id, entity_id, remainder, share_cost)
            logging.debug("Adding shares with quantity %s.." % remainder)
            cursor.execute(sql)
            # ..now remove the existing holding.
            sql = """UPDATE shares set active=0 where id=%s""" % share_id
            logging.debug("..and making shares with ID %s inactive." % share_id)
            cursor.execute(sql)
            trans_qty = 0
    return True


# Attempts to satisfy a buy order with sell orders.
def match_buy_with_sells(cursor, user_id, entity_id, buy_id, buy_min_price, buy_max_price, buy_quantity):
    cursor.execute("""SELECT id, entity_id, user_id, quantity, minPrice, maxPrice FROM orders
                                WHERE buyOrSell='sell'
                                and (((minPrice >= %s and minPrice <=%s) or (maxPrice >=%s and maxPrice <=%s))
                                or ((%s >= minPrice and %s <=maxPrice) or (%s >=minPrice and %s <=maxPrice)) )
                                and entity_id=%s and quantity>0
                                ORDER BY timestamp ASC, quantity DESC"""
                   % (buy_min_price, buy_max_price, buy_min_price, buy_max_price, buy_min_price, buy_min_price,
                      buy_max_price, buy_max_price, entity_id))
    sell_rows = cursor.fetchall()
    logging.debug("Found %s possible sellers" % len(sell_rows))
    for sellRow in sell_rows:
        if buy_quantity == 0:
            break
        trans_quantity = 0
        sell_id = sellRow[0]
        sell_user_id = sellRow[2]
        sell_quantity = sellRow[3]
        sell_min_price = sellRow[4]
        sell_max_price = sellRow[5]

        logging.debug("Trying order %s by seller %s" % (sell_id, sell_user_id))

        #Determine transaction quantity and price
        if sell_quantity >= buy_quantity:
            trans_quantity = buy_quantity
            sell_quantity = sell_quantity - buy_quantity
            buy_quantity = 0
        elif buy_quantity >= sell_quantity:
            trans_quantity = sell_quantity
            buy_quantity = buy_quantity - sell_quantity
            sell_quantity = 0
        else:
            trans_quantity = buy_quantity
            buy_quantity = 0
            sell_quantity = 0
        trans_price = find_fair_price(buy_min_price, buy_max_price, sell_min_price, sell_max_price)
        logging.debug("Established price of %s for %s shares." % (trans_price, trans_quantity))

        #Check whether seller has enough shares
        sql = """select sum(quantity) from shares where user_id=%s and entity_id=%s""" % (sell_user_id, entity_id)
        cursor.execute(sql)
        res = cursor.fetchone()
        shares_owned_by_seller = res[0]
        if shares_owned_by_seller < trans_quantity:
            logging.debug("Seller does not have enough shares.")
            continue

        #Check whether buyer has enough cash.
        transaction_cost = trans_quantity * trans_price
        buyers_cash = get_user_cash(cursor, user_id)
        if buyers_cash < transaction_cost:
            logging.debug("Buyer does not have enough cash.")
            continue

        # Debit transaction amount from buyer.
        buyers_balance = buyers_cash - transaction_cost
        sql = """UPDATE user set cash=%s where id=%s""" % (buyers_balance, user_id)
        cursor.execute(sql)
        logging.debug("Deducted %s from buyer." % trans_price)

        # Credit transaction amount to seller
        sql = "UPDATE user set cash=cash+%s where id=%s" % (transaction_cost, sell_user_id)
        cursor.execute(sql)
        logging.debug("Credit %s to seller." % trans_price)

        #Deduct seller's shares
        deduct_user_shares(cursor, sell_user_id, entity_id, trans_quantity)

        #Credit shares to buyer
        sql = """INSERT into shares (user_id, entity_id, quantity, cost, startTime, endTime)
                        VALUES (%s, %s, %s, %s, now(), '0000-00-00 00:00:00')""" % \
              (user_id, entity_id, trans_quantity, trans_price)
        cursor.execute(sql)

        #Add transaction record
        cursor.execute("""INSERT INTO transaction
                                    (entity_id, from_user_id, to_user_id, quantity, price, buy_order_id, sell_order_id)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s)""" %
                       (entity_id, sell_user_id, user_id, trans_quantity, trans_price, buy_id, sell_id))

        #Update buy and sell order quantities
        cursor.execute("""UPDATE orders set quantity=%s where id=%s""" % (sell_quantity, sell_id))
        cursor.execute("""UPDATE orders set quantity=%s where id=%s""" % (buy_quantity, buy_id))

        #If sell order has zero quantity, close it.
        if sell_quantity <= 0:
            sql = "UPDATE orders set active=0 where id=%s" % sell_id
            cursor.execute(sql)
            logging.debug("Closing order %s as no shares are left to be sold." % sell_id)

        logging.info('Executed transaction. User %s bought %s shares of %s from %s' %
                     (user_id, trans_quantity, entity_id, sell_user_id))

    return buy_quantity


# Attempts to satisfy a given buy order with unowned shares.
def match_buy_with_free_shares(cursor, buy_id, entity_id, user_id, buy_min_price, buy_max_price, buy_quantity):
    stmt = """select id, quantity, cost from shares where entity_id=%s
                                 and user_id is null
                                 and cost>=%s and cost<=%s order by quantity asc""" % \
           (entity_id, buy_min_price, buy_max_price)
    cursor.execute(stmt)
    free_shares = cursor.fetchall()
    logging.debug("Found %s possible free share rows." % len(free_shares))
    for freeShareRow in free_shares:
        if buy_quantity == 0:
            break
        trans_quantity = 0
        free_share_id = freeShareRow[0]
        free_share_qty = freeShareRow[1]
        free_share_cost = freeShareRow[2]
        logging.debug(
            "\t%s: Trying to match with %s free shares at cost %s" % (free_share_id, free_share_qty, free_share_cost))

        if free_share_qty >= buy_quantity:
            trans_quantity = buy_quantity
            free_share_qty = free_share_qty - buy_quantity
            buy_quantity = 0
        elif buy_quantity >= free_share_qty:
            trans_quantity = free_share_qty
            buy_quantity = buy_quantity - free_share_qty
            free_share_qty = 0
        else:
            trans_quantity = buy_quantity
            buy_quantity = 0
            free_share_qty = 0
        trans_price = free_share_cost
        logging.debug("Established price of %s for %s shares." % (trans_price, trans_quantity))

        #Check whether buyer has enough cash. If yes, deduct purchase amount from buyer.
        transaction_cost = trans_quantity * trans_price
        buyers_cash = get_user_cash(cursor, user_id)
        if buyers_cash < transaction_cost:
            logging.debug("Buyer does not have enough cash.")
            continue
        buyers_balance = buyers_cash - transaction_cost
        sql = """UPDATE user set cash=%s where id=%s""" % (buyers_balance, user_id)
        cursor.execute(sql)
        logging.debug("Deducted %s from buyer." % trans_price)

        #Add transaction record
        cursor.execute("""INSERT INTO transaction
                                (entity_id, from_user_id, to_user_id, quantity, price, buy_order_id, sell_order_id)
                                VALUES (%s, null, %s, %s, %s, %s, null)""" % (
            entity_id, user_id, trans_quantity, trans_price, buy_id))

        #Update purchase order
        cursor.execute("""UPDATE orders set quantity=%s where id=%s""" % (buy_quantity, buy_id))

        #Update free share quantity
        cursor.execute("""UPDATE shares set quantity=%s where id=%s""" % (free_share_qty, free_share_id))

        #Credit shares to buyer
        cursor.execute("""INSERT INTO shares
                                (user_id, entity_id, quantity, startTime, endTime, cost)
                                VALUES (%s, %s, %s, now(), '0000-00-00 00:00:00', %s)""" % (
            user_id, entity_id, trans_quantity, trans_price))

        logging.info(
            'Executed transaction. User %s bought %s free shares of %s' % (user_id, trans_quantity, entity_id))

    return buy_quantity


# Runs a single iteration of transaction processing.
def run_transaction_iteration(conn, cursor):
    # Find all outstanding buy orders.
    cursor.execute(
        "SELECT id, entity_id, user_id, quantity, minPrice, maxPrice FROM orders "
        "where buyOrSell='buy' and quantity>0")
    buy_rows = cursor.fetchall()
    for buyRow in buy_rows:
        #Handle an individual buy order.
        buy_id = buyRow[0]
        entity_id = buyRow[1]
        user_id = buyRow[2]
        buy_quantity = buyRow[3]
        buy_min_price = buyRow[4]
        buy_max_price = buyRow[5]

        logging.debug("%s: Trying to buy %s shares of %s for %s" % (buy_id, buy_quantity, entity_id, user_id))

        # Try matching sell orders.
        buy_quantity = match_buy_with_sells(cursor, user_id, entity_id, buy_id,
                                            buy_min_price, buy_max_price, buy_quantity)

        # After considering sell orders, try free shares.
        buy_quantity = match_buy_with_free_shares(cursor, buy_id, entity_id, user_id, buy_min_price,
                                                  buy_max_price, buy_quantity)

        #If the buy quantity left is now zero, close the order
        if buy_quantity <= 0:
            sql = "UPDATE orders set active=0 where id=%s" % buy_id
            cursor.execute(sql)
            logging.debug("Closing order %s as no shares are left to be bought." % buy_id)
    conn.commit()


# Primary method. Runs transaction processing.
def run_transactions():
    logging.basicConfig(level=logging.DEBUG)
    conn = common.get_connection()
    cursor = conn.cursor()
    while True:
        run_transaction_iteration(conn, cursor)
        time.sleep(5)
        logging.info('Sleeping for 5 seconds..')
    cursor.close()
    conn.close()


def main():
    #parser = argparse.ArgumentParser(description='Go through all active orders and process them as possible.')
    #args = parser.parse_args()
    run_transactions()


if __name__ == "__main__":
    main()