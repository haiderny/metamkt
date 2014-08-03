'''
Created on Aug 18, 2011

@author: Vishakh
'''
from sqlalchemy import or_, func

from pyramid.url import route_url

from models import Entity,EntityType,Group,Order,PriceChange,Shares,Transaction,User,ValueChange
import url as sm_url

#SQLAlchemy action list ==> List of dicts
def decodeActionList(request, dbsession, actions):
    actionList = []
    for action in actions:
        actionList.append(decodeAction(request, dbsession, action))
    return actionList

#SQLAlchemy action object ==> Dict
def decodeAction(request, dbsession, action):
    url = sm_url.formActionURI(request, action.name)
    return {'id': action.id, 'name': action.name, 'description': action.description, 'points': action.points, 'url': url}

#SQLAlchemy group object ==> Dict
def decodeGroup(request, dbsession, group):
    url = sm_url.formLeagueURI(request, group.name)
    return {'id': group.id, 'name': group.name, 'url': url}

#SQLAlchemy entity object ==> Dict
def decodeEntity(request, dbsession, entity, leagueObj):
    numShares = dbsession.query(func.sum(Shares.quantity)).filter(Shares.entity_id == entity.id).scalar()
    numFreeShares = dbsession.query(func.sum(Shares.quantity)).filter(Shares.entity_id == entity.id).filter(Shares.user_id == None).scalar()
    priceChanges = dbsession.query(PriceChange).filter(PriceChange.entity_id == entity.id).all()
    priceChangeDict = {}
    for priceChange in priceChanges:
        priceChangeDict[priceChange.term] = float(priceChange.value)
    leagueDict = {'name':leagueObj.name, 'uri':sm_url.formLeagueURI(request, leagueObj.name)}
    data = {
        'id':entity.id, 'name':entity.name, 'uri':sm_url.formPlayerURI(request, entity.name),
        'league':leagueDict, 'numShares':numShares, 'numFreeShares':numFreeShares,
        'price':entity.price, 'priceChanges':priceChangeDict}
    return data


#SQLAlchemy action object ==> Dict
def decodeEntityType(request, dbsession, entity_type):
    return {'id': entity_type.id, 'name': entity_type.name}


#SQLAlchemy event object ==> Dict
def decodeEvent(request, dbsession, event):
    return {'id': event.id, 'hash': event.hash, 'entity_id': event.entity_id, 'action_id': event.action_id,
            'quantity': event.quantity, 'description': event.description}

#SQLAlchemy order list ==> List of dicts
def decodeOrderList(request, dbsession, players):
    orderList = []
    for order in players:
        orderList.append(decodeOrder(request, dbsession, order))
    return orderList

#SQLAlchemy order object ==> Dict
def decodeOrder(request, dbsession, order):
    entityObj =dbsession.query(Entity).filter(Entity.id==order.entity_id).one()
    entityDict = {'name': entityObj.name, 'url': sm_url.formEntityURI(request, entityObj)}
    userObj = dbsession.query(User).filter(User.id == order.user_id).one()
    userDict = {'id': userObj.id, 'name': userObj.name, 'url': route_url('user', request, user=userObj.name)}
    transactions = dbsession.query(Transaction).filter(or_(Transaction.buy_order_id == order.id, Transaction.sell_order_id == order.id)).all()
    transactionList = decodeTransactionList(request, dbsession, transactions)
    return {'id': order.id, 'quantity': order.quantity, 'minPrice': float(order.minPrice), 'maxPrice': float(order.maxPrice), 'buyorsell': order.buyOrSell, 'timestamp': str(order.timestamp), 'entity':entityDict, 'url': route_url('order', request, order=order.id), 'user': userDict, 'transactions': transactionList}

#SQLAlchemy player list ==> List of dicts
def decodePlayerList(request, dbsession, players):
    playerList = []
    for player in players:
        playerList.append(decodePlayer(request, dbsession, player))
    return playerList

#SQLAlchemy player object ==> Dict
def decodePlayer(request, dbsession, player):
    teamObj = dbsession.query(Entity).filter(Entity.id == player.parent_id).one()
    teamDict = {'name':teamObj.name, 'uri': sm_url.formTeamURI(request, teamObj.name)}
    leagueObj = dbsession.query(Group).filter(Group.id == teamObj.group_id).one()
    data = decodeEntity(request, dbsession, player, leagueObj)
    data['team'] = teamDict
    return data

#SQLAlchemy player list ==> List of dicts
def decodeTeamList(request, dbsession, teams):
    teamList = []
    for team in teams:
        teamList.append(decodeTeam(request, dbsession, team))
    return teamList

#SQLAlchemy team object ==> Dict
def decodeTeam(request, dbsession, team):
    leagueObj = dbsession.query(Group).filter(Group.id == team.group_id).one()
    data = decodeEntity(request, dbsession, team, leagueObj)
    return data


#SQLAlchemy share list ==> List of dicts
def decodeShareList(request, dbsession, shares):
    shareList = []
    for share in shares:
        shareList.append(decodeShare(request, dbsession, share))
    return shareList

#SQLAlchemy shares object ==> Dict
def decodeShare(request, dbsession, share):
    entityObj =dbsession.query(Entity).filter(Entity.id==share.entity_id).one()
    entityDict = {'name': entityObj.name, 'url': sm_url.formEntityURI(request, entityObj)}
    return {'quantity': int(share.quantity), 'cost': float(share.cost), 'entity': entityDict}

#SQLAlchemy transaction list ==> List of dicts
def decodeTransactionList(request, dbsession, transactions):
    transactionList = []
    for transaction in transactions:
        transactionList.append(decodeTransaction(request, dbsession, transaction))
    return transactionList

#SQLAlchemy transaction object ==> Dict
def decodeTransaction(request, dbsession, transaction):
    entityObj =dbsession.query(Entity).filter(Entity.id==transaction.entity_id).one()
    entityDict = {'name': entityObj.name, 'url': sm_url.formEntityURI(request, entityObj)}
    if transaction.from_user_id==None: buyerDict = {'id': '', 'name': 'Free shares', 'url': ''}
    else:
        buyerObj = dbsession.query(User).filter(User.id == transaction.from_user_id).one()
        buyerDict = {'id': buyerObj.id, 'name': buyerObj.name, 'url': route_url('user', request, user=buyerObj.name)}
    sellerObj = dbsession.query(User).filter(User.id == transaction.to_user_id).one()
    sellerDict = {'id': sellerObj.id, 'name': sellerObj.name, 'url': route_url('user', request, user=sellerObj.name)}
    buyOrderDict = {'id': transaction.buy_order_id, 'url': route_url('order', request, order=transaction.buy_order_id)}
    sellOrderDict = {'id': transaction.sell_order_id, 'url': route_url('order', request, order=transaction.sell_order_id)}
    return {'id': transaction.id, 'timestamp': str(transaction.timestamp), 'entity': entityDict, 'quantity': transaction.quantity, 'price': str(transaction.price), 'buyer': buyerDict, 'seller': sellerDict, 'buyOrder': buyOrderDict, 'sellOrder': sellOrderDict, 'url': route_url('transaction', request, transaction=transaction.id)}

#SQLAlchemy user list ==> List of dicts
def decodeUserList(request, dbsession, users):
    userList = []
    for user in users:
        userList.append(decodeUser(request, dbsession, user))
    return userList

#SQLAlchemy user object ==> Dict
def decodeUser(request, dbsession, user, user_data):
    shares = dbsession.query(Shares.entity_id, Shares.cost, func.sum(Shares.quantity).label('quantity')).filter(Shares.user_id == user.id).filter(Shares.active == 1).group_by(Shares.entity_id, Shares.cost).all()
    shareList = decodeShareList(request, dbsession, shares)
    ordersURL = sm_url.formUserOrdersURI(request, user.name)
    transactionsURL = sm_url.formUserTransactionsURI(request, user.name)
    valueChanges = dbsession.query(ValueChange).filter(ValueChange.user_id == user.id).all()
    userValue = user_data.value
    if user_data.value is None:
        userValue = 0.0
    valueChangeDict = {}
    for valueChange in valueChanges:
        valueChangeDict[valueChange.term] = float(valueChange.value)
    return {'name': user.name, 'uri':sm_url.formUserURI(request, user.name),'value': float(userValue), 'cash': float(user_data.cash), 'id': user.id, 'ordersURL': ordersURL, 'transactionsURL': transactionsURL, 'shares': shareList, 'valueChanges': valueChangeDict}