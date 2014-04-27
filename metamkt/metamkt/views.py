from cornice import Service
import datetime
import transaction

import convert
from models import Action, DBSession, Entity, EntityType, Event, Group, Order, PriceChange, Shares, Transaction, User, \
    ValueChange

# ********** Cornice Services ********** #

root = Service(name='index', path='/', description="Metamkt")

action = Service(name='action', path='/actions/{action}', description="Action")
entity_type = Service(name="entity_type", path="/entity_types/{entity_type}", description="Entity Type")
event = Service(name='event', path='/events/{event}', description="Action")
league = Service(name='league', path='/leagues/{group}', description="League")
player = Service(name="player", path="/players/{player}", description="Player")
team = Service(name="team", path="/teams/{team}", description="Team")
user = Service(name='user', path='/users/{user}', description="User")

# ********** View Functions ********** #


@root.get()
def get_info(request):
    return {'Hello': 'World'}

@action.delete()
def action_delete(request):
    dbsession = DBSession()
    name = clean_matchdict_value(request, 'action')
    action = dbsession.query(Action).filter(Action.name == name).one()
    dbsession.delete(action)
    transaction.commit()
    return {'status': 'success'}


@action.get()
def action_get(request):
    dbsession = DBSession()
    name = clean_matchdict_value(request, 'action')
    action = dbsession.query(Action).filter(Action.name == name).one()
    action_json = convert.decodeAction(request, dbsession, action)
    return {'status': 'success', 'action': action_json}


@action.put()
def action_put(request):
    dbsession = DBSession()
    action = Action()
    action.name = clean_matchdict_value(request, 'action')
    action.description = clean_param_value(request, 'description')
    action.points = clean_param_value(request, 'points')
    action.timestamp = get_timestamp()
    dbsession.add(action)
    transaction.commit()
    return {'status': 'success'}


@entity_type.delete()
def entity_type_delete(request):
    dbsession = DBSession()
    name = clean_matchdict_value(request, 'entity_type')
    entity_type = dbsession.query(EntityType).filter(EntityType.name == name).one()
    dbsession.delete(entity_type)
    transaction.commit()
    return {'status': 'success'}


@entity_type.get()
def entity_type_get(request):
    dbsession = DBSession()
    name = clean_matchdict_value(request, 'entity_type')
    entity_type = dbsession.query(EntityType).filter(EntityType.name == name).one()
    entity_type_json = convert.decodeEntityType(request, dbsession, entity_type)
    return {'status': 'success', 'entity_type': entity_type_json}


@entity_type.put()
def entity_type_put(request):
    dbsession = DBSession()
    entity_type = EntityType()
    entity_type.name = clean_matchdict_value(request, 'entity_type')
    entity_type.timestamp = get_timestamp()
    dbsession.add(entity_type)
    transaction.commit()
    return {'status': 'success'}


@event.delete()
def event_delete(request):
    dbsession = DBSession()
    name = clean_matchdict_value(request, 'event')
    event = dbsession.query(Event).filter(Event.name == name).one()
    dbsession.delete(event)
    transaction.commit()
    return {'status': 'success'}


@event.get()
def event_get(request):
    dbsession = DBSession()
    name = clean_matchdict_value(request, 'event')
    event = dbsession.query(Event).filter(Event.name == name).one()
    event_json = convert.decodeEvent(request, dbsession, event)
    return {'status': 'success', 'event': event_json}


@event.put()
def event(request):
    dbsession = DBSession()
    name = clean_matchdict_value(request, 'event')
    event = Event()
    event.entity_id = name
    event.action_id = clean_param_value(request, 'action_id')
    event.quantity = clean_param_value(request, 'quantity')
    event.description = clean_param_value(request, 'description')
    event.timestamp = get_timestamp()
    dbsession.add(event)
    transaction.commit()
    return {'status': 'success'}

@league.delete()
def group_delete(request):
    dbsession = DBSession()
    name = clean_matchdict_value(request, 'group')
    group = dbsession.query(Group).filter(Group.name == name).one()
    dbsession.delete(group)
    transaction.commit()
    return {'status': 'success'}


@league.get()
def group_get(request):
    dbsession = DBSession()
    name = clean_matchdict_value(request, 'group')
    group = dbsession.query(Group).filter(Group.name == name).one()
    group_json = convert.decodeGroup(request, dbsession, group)
    return {'status': 'success', 'group': group_json}


@league.put()
def group(request):
    dbsession = DBSession()
    name = clean_matchdict_value(request, 'group')
    group = Group()
    group.name = name
    group.timestamp = get_timestamp()
    dbsession.add(group)
    transaction.commit()
    return {'status': 'success'}


@player.delete()
def player_delete(request):
    dbsession = DBSession()
    name = clean_matchdict_value(request, 'player')
    entity = dbsession.query(Entity).filter(Entity.name == name).filter(Entity.parent_id != None).one()
    dbsession.delete(entity)
    transaction.commit()
    return {'status': 'success'}


@player.get()
def player_get(request):
    dbsession = DBSession()
    name = clean_matchdict_value(request, 'player')
    entity = dbsession.query(Entity).filter(Entity.name == name).filter(Entity.parent_id != None).one()
    entity_json = convert.decodePlayer(request, dbsession, entity)
    return {'status': 'success', 'player': entity_json}


@player.put()
def player_put(request):
    dbsession = DBSession()
    entity = Entity()
    entity.name = clean_matchdict_value(request, 'player')
    entity.entityType_id = getPlayerTypeID()
    entity.group_id = clean_param_value(request, 'group_id')
    entity.parent_id = clean_param_value(request, 'parent_id')
    entity.price = 0
    entity.timestamp = get_timestamp()
    dbsession.add(entity)
    transaction.commit()
    return {'status': 'success'}


@team.delete()
def team_delete(request):
    dbsession = DBSession()
    name = clean_matchdict_value(request, 'team')
    entity = dbsession.query(Entity).filter(Entity.name == name).filter(Entity.parent_id == None).one()
    dbsession.delete(entity)
    transaction.commit()
    return {'status': 'success'}


@team.get()
def team_get(request):
    dbsession = DBSession()
    name = clean_matchdict_value(request, 'team')
    entity = dbsession.query(Entity).filter(Entity.name == name).filter(Entity.parent_id == None).one()
    entity_json = convert.decodeTeam(request, dbsession, entity)
    return {'status': 'success', 'team': entity_json}


@team.put()
def team_put(request):
    dbsession = DBSession()
    entity = Entity()
    entity.name = clean_matchdict_value(request, 'team')
    entity.entityType_id = getTeamTypeID()
    entity.group_id = clean_param_value(request, 'group_id')
    entity.parent_id = None
    entity.price = 0
    entity.timestamp = get_timestamp()
    dbsession.add(entity)
    transaction.commit()
    return {'status': 'success'}


@user.delete()
def user_delete(request):
    dbsession = DBSession()
    name = clean_matchdict_value(request, 'user')
    user = dbsession.query(User).filter(User.name == name).one()
    dbsession.delete(user)
    transaction.commit()
    return {'status': 'success'}


@user.get()
def user_get(request):
    dbsession = DBSession()
    name = clean_matchdict_value(request, 'user')
    user = dbsession.query(User).filter(User.name == name).one()
    user_json = convert.decodeUser(request, dbsession, user)
    return {'status': 'success', 'user': user_json}


@user.put()
def user_put(request):
    dbsession = DBSession()
    username = clean_matchdict_value(request, 'user')
    user = User()
    user.name = username
    user.email = request.params['email']
    user.salt = 'salt'
    user.password = 'password'
    user.cash = 10000
    dbsession.add(user)
    transaction.commit()
    return {'status': 'success'}


# ********** Cornice Validators ********** #



# ********** Utility Functions ********** #

def getTeamTypeID():
    return DBSession().query(EntityType).filter(EntityType.name == 'team').first().id


def getPlayerTypeID():
    return DBSession().query(EntityType).filter(EntityType.name == 'player').first().id


def get_timestamp():
    return datetime.datetime.utcnow()


def clean_matchdict_value(request, key):
    return request.matchdict[key].replace('_', ' ')


def clean_param_value(request, key):
    return request.params[key].replace('_', ' ')