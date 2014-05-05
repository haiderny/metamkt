import common
import logging
import MySQLdb
import time
from decimal import *

def findFairPrice(minBuyPrice, maxBuyPrice, minSellPrice, maxSellPrice):
    points = [minBuyPrice, maxBuyPrice, minSellPrice, maxSellPrice]
    points.sort()
    return (points[1] + points[2]) / Decimal(2.0) 

def checkBuyerHasCash(cursor, user_id, cashNeeded):
    sql = """select cash from user where id=%s"""%user_id
    cursor.execute(sql)
    res = cursor.fetchone()
    buyersCash = res[0]
    if(buyersCash < cashNeeded): return -1
    return buyersCash

def deductSellerShares(cursor, seller_id, entity_id, transQty):
    sql = """select id, quantity, cost from shares where user_id=%s and entity_id=%s and active!=0 order by quantity asc"""%(seller_id, entity_id)
    cursor.execute(sql)
    shares = cursor.fetchall()
    for share in shares:
        logging.debug("%s seller shares left to deduct."%transQty)
        if transQty==0: continue
        shareID = share[0]
        shareQty = share[1]
        shareCost = share[2]
        if shareQty <= transQty:
            sql = """UPDATE shares set endTime=now(), active=0 where id=%s"""%shareID
            cursor.execute(sql)
            transQty = transQty - shareQty
            logging.debug("Deducting shares with ID %s"%shareID)
        else:
            remainder = shareQty - transQty
            sql = """INSERT into shares (user_id, entity_id, quantity, cost, startTime, endTime, active) 
                    VALUES (%s, %s, %s, %s, now(), '0000-00-00 00:00:00', 1)"""%(seller_id, entity_id, remainder, shareCost)
            logging.debug("Adding shares with quantity %s.."%remainder)
            cursor.execute(sql)
            sql = """UPDATE shares set active=0 where id=%s"""%shareID
            logging.debug("..and making shares with ID %s inactive."%shareID)
            cursor.execute(sql)
            transQty = 0
    return True

logging.basicConfig(level=logging.DEBUG)
conn = common.getConnection()
cursor = conn.cursor()

while True:
    cursor.execute ("SELECT id, entity_id, user_id, quantity, minPrice, maxPrice FROM soccermkt.orders where buyOrSell='buy' and quantity>0")
    buyRows = cursor.fetchall()
    for buyRow in buyRows:
        id =        buyRow[0]
        entity_id = buyRow[1]
        user_id =   buyRow[2]
        buyQuantity =  buyRow[3]
        minPrice =  buyRow[4]
        maxPrice =  buyRow[5]
        
        logging.debug("%s: Trying to buy %s shares of %s for %s"%(id, buyQuantity, entity_id, user_id))
        
        #first look for sellers
        cursor.execute ("""SELECT id, entity_id, user_id, quantity, minPrice, maxPrice FROM soccermkt.orders 
                            WHERE buyOrSell='sell'  
                            and (((minPrice >= %s and minPrice <=%s) or (maxPrice >=%s and maxPrice <=%s)) 
                            or ((%s >= minPrice and %s <=maxPrice) or (%s >=minPrice and %s <=maxPrice)) )
                            and entity_id=%s and quantity>0
                            ORDER BY timestamp ASC, quantity DESC"""%(minPrice, maxPrice, minPrice, maxPrice, minPrice, minPrice, maxPrice, maxPrice, entity_id))
        sellRows = cursor.fetchall()
        logging.debug("Found %s possible sellers"%len(sellRows))
        for sellRow in sellRows:
            if buyQuantity == 0: break
            transQuantity = 0
            sellId =        sellRow[0]
            sellEntity_id = sellRow[1]
            sellUser_id =   sellRow[2]
            sellQuantity =  sellRow[3]
            sellMinPrice =  sellRow[4]
            sellMaxPrice =  sellRow[5]
            
            logging.debug("Trying order %s by seller %s"%(sellId, sellUser_id))
                      
            #Determine transaction quantity and price
            if sellQuantity >= buyQuantity:
                transQuantity = buyQuantity 
                sellQuantity = sellQuantity - buyQuantity
                buyQuantity = 0
            elif buyQuantity >= sellQuantity:
                transQuantity = sellQuantity
                buyQuantity = buyQuantity - sellQuantity
                sellQuantity = 0
            else:
                transQuantity = buyQuantity 
                buyQuantity = 0
                sellQuantity = 0
            transPrice = findFairPrice(minPrice, maxPrice, sellMinPrice, sellMaxPrice)
            print transPrice
            logging.debug("Established price of %s for %s shares."%(transPrice, transQuantity))
            
            #Check whether seller has enough shares
            sql = """select sum(quantity) from shares where user_id=%s and entity_id=%s"""%(sellUser_id, entity_id)
            cursor.execute(sql)
            res = cursor.fetchone()
            sharesOwned = res[0]
            if(sharesOwned<transQuantity): 
                logging.debug("Seller does not have enough shares.")
                continue
            
            #Check whether buyer has enough cash. If yes, deduct purchase amount from buyer.
            transactionCost = transQuantity * transPrice
            buyersCash = checkBuyerHasCash(cursor, user_id, transactionCost)
            if buyersCash==-1: 
                logging.debug("Buyer does not have enough cash.")
                continue
            balance = buyersCash - transactionCost
            sql = """UPDATE user set cash=%s where id=%s"""%(balance, user_id)
            cursor.execute(sql)
            logging.debug("Deducted %s from buyer."%transPrice)
            
            #Credit transaction amount to seller
            sql = "UPDATE user set cash=cash+%s where id=%s"%(transactionCost, sellUser_id)
            cursor.execute(sql)
            logging.debug("Credit %s to seller."%transPrice)
            
            #Deduct seller's shares
            deductSellerShares(cursor, sellUser_id, entity_id, transQuantity)
            
            #Credit shares to buyer
            sql = """INSERT into shares (user_id, entity_id, quantity, cost, startTime, endTime) 
                    VALUES (%s, %s, %s, %s, now(), '0000-00-00 00:00:00')"""%(user_id, entity_id, transQuantity, transPrice)
            cursor.execute(sql)
            
            #Add transaction record
            cursor.execute ("""INSERT INTO soccermkt.transaction 
                                (entity_id, from_user_id, to_user_id, quantity, price, buy_order_id, sell_order_id) 
                                VALUES (%s, %s, %s, %s, %s, %s, %s)"""%(entity_id, sellUser_id, user_id, transQuantity, transPrice, id, sellId))
            
            #Update buy and sell order quantities
            cursor.execute ("""UPDATE soccermkt.orders set quantity=%s where id=%s"""%(sellQuantity, sellId))
            cursor.execute ("""UPDATE soccermkt.orders set quantity=%s where id=%s"""%(buyQuantity, id))
            
            #If sell order has zero quantity, close it.
            if sellQuantity<=0:
                sql = "UPDATE orders set active=0 where id=%s"%sellId
                cursor.execute(sql)
                logging.debug("Closing order %s as no shares are left to be sold."%sellId)
            
            logging.info('Executed transaction. User %s bought %s shares of %s from %s'%(user_id, transQuantity, entity_id, sellUser_id))
            
        #..then look for free rows    
        stmt = """select id, quantity, cost from shares where entity_id=%s
                             and user_id is null 
                             and cost>=%s and cost<=%s order by quantity asc"""%(entity_id, minPrice, maxPrice)
        #if verbose: print stmt
        cursor.execute (stmt)
        freeShareRows = cursor.fetchall()
        logging.debug("Found %s possible free share rows."%len(freeShareRows))
        for freeShareRow in freeShareRows:    
            if buyQuantity == 0: break
            transQuantity = 0
            freeShareID =   freeShareRow[0]
            freeShareQty =  freeShareRow[1]
            freeShareCost = freeShareRow[2]
            logging.debug("\t%s: Trying to match with %s free shares at cost %s"%(freeShareID, freeShareQty, freeShareCost))
            
            if freeShareQty >= buyQuantity:
                transQuantity = buyQuantity 
                freeShareQty = freeShareQty - buyQuantity
                buyQuantity = 0
            elif buyQuantity >= freeShareQty:
                transQuantity = freeShareQty
                buyQuantity = buyQuantity - freeShareQty
                freeShareQty = 0
            else:
                transQuantity = buyQuantity 
                buyQuantity = 0
                freeShareQty = 0
            transPrice = freeShareCost
            logging.debug("Established price of %s for %s shares."%(transPrice, transQuantity))
            
            #Check whether buyer has enough cash. If yes, deduct purchase amount from buyer.
            transactionCost = transQuantity * transPrice
            buyersCash = checkBuyerHasCash(cursor, user_id, transactionCost)
            if buyersCash==-1: 
                logging.debug("Buyer does not have enough cash.")
                continue
            balance = buyersCash - transactionCost
            sql = """UPDATE user set cash=%s where id=%s"""%(balance, user_id)
            cursor.execute(sql)
            logging.debug("Deducted %s from buyer."%transPrice)
            
            #Add transaction record
            cursor.execute ("""INSERT INTO soccermkt.transaction 
                            (entity_id, from_user_id, to_user_id, quantity, price, buy_order_id, sell_order_id) 
                            VALUES (%s, null, %s, %s, %s, %s, null)"""%(entity_id, user_id, transQuantity, transPrice, id))
            
            #Update purchase order
            cursor.execute ("""UPDATE soccermkt.orders set quantity=%s where id=%s"""%(buyQuantity, id))
            
            #Update free share quantity
            if freeShareQty>0:
                cursor.execute ("""UPDATE soccermkt.shares set quantity=%s where id=%s"""%(freeShareQty, freeShareID))
            else:
                sql = """DELETE from shares where id=%s"""%(freeShareID)
            
            #Credit shares to buyer
            cursor.execute ("""INSERT INTO soccermkt.shares
                            (user_id, entity_id, quantity, startTime, endTime, cost)
                            VALUES (%s, %s, %s, now(), '0000-00-00 00:00:00', %s)"""%(user_id, entity_id, transQuantity, transPrice))
            
            logging.info('Executed transaction. User %s bought %s free shares of %s'%(user_id, transQuantity, entity_id))
        
        #If the buy quantity left is now zero, close the order
        if buyQuantity<=0:
            sql = "UPDATE orders set active=0 where id=%s"%id
            cursor.execute(sql)
            logging.debug("Closing order %s as no shares are left to be bought."%id)
    conn.commit ()
    time.sleep(5)
    logging.info('Sleeping for 5 seconds..')
cursor.close ()
conn.close()