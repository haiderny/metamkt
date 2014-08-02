import json
import requests
import sys
from price_calculator import calc_prices

def query(http_method, url, payload=None):
    print '\n<<=====>>\n'
    print url, http_method
    if http_method is 'GET':
        r = requests.get(url)
    elif http_method is 'PUT':
        r = requests.put(url, data=payload)
    elif http_method is 'DELETE':
        r = requests.delete(url)
    else:
        raise Exception('Unsupported HTTP method.')
    print 'Status code: ', r.status_code
    print 'Content: ', r.content
    if r.status_code is not 200:
        print 'Query unsuccessful.'
        sys.exit()
    print 'Query successful.'
    return r.content

baseUrl = 'http://localhost:6543/'

entityTypeUrl1 = baseUrl+"entity_types/team"
entityTypeUrl2 = baseUrl+"entity_types/player"
actionUrl = baseUrl+"actions/ScoredGoal"
groupUrl = baseUrl+"groups/EPL"
userUrl = baseUrl+"users/vishakh"
teamUrl = baseUrl+"teams/Liverpool"
playerUrl = baseUrl+"players/MichaelOwen"
eventsUrl = baseUrl+"events"
eventUrl = eventsUrl
ordersUrl = baseUrl+"orders"
orderUrl = ordersUrl

get_and_put = True
run_calculators = True
delete = False

if get_and_put:
    thePayload = None
    theUrl = entityTypeUrl1
    query('PUT', theUrl, thePayload)
    response = json.loads(query('GET', theUrl))
    teamTypeID = response['entity_type']['id']

    thePayload = None
    theUrl = entityTypeUrl2
    query('PUT', theUrl, thePayload)
    response = json.loads(query('GET', theUrl))
    playerTypeID = response['entity_type']['id']

    thePayload = {'description': 'Scored goal', 'points': 10}
    theUrl = actionUrl
    query('PUT', theUrl, thePayload)
    response = json.loads(query('GET', theUrl))
    actionID = response['action']['id']

    thePayload = None
    theUrl = groupUrl
    query('PUT', theUrl, thePayload)
    response = json.loads(query('GET', theUrl))
    groupID = response['group']['id']

    thePayload = {'email': 'vishakh@yahoo.com'}
    theUrl = userUrl
    query('PUT', theUrl, thePayload)
    response = json.loads(query('GET', theUrl))
    userID = response['user']['id']

    thePayload = {'group_id': groupID}
    theUrl = teamUrl
    query('PUT', theUrl, thePayload)
    response = json.loads(query('GET', theUrl))
    teamID = response['team']['id']

    thePayload = {'group_id': groupID, 'parent_id': teamID}
    theUrl = playerUrl
    query('PUT', theUrl, thePayload)
    response = json.loads(query('GET', theUrl))
    playerID = response['player']['id']

    thePayload = {'entity_id': playerID, 'action_id': actionID, 'quantity': 1, 'description': 'Something happened!'}
    theUrl = eventsUrl
    response = json.loads(query('PUT', theUrl, thePayload))
    eventID = response['event']['id']
    theUrl = eventUrl + "/" + str(eventID)
    response = json.loads(query('GET', theUrl))
    eventUrl = theUrl

    thePayload = {'user_id': userID, 'entity_id': playerID, 'action_id': actionID, 'quantity': 1,
                  'minPrice': 0, 'maxPrice': 1000, 'buyOrSell': 'buy'}
    theUrl = ordersUrl
    response = json.loads(query('PUT', theUrl, thePayload))
    orderID = response['order']['id']
    theUrl = ordersUrl + "/" + str(orderID)
    response = json.loads(query('GET', theUrl))
    orderUrl = theUrl

if run_calculators:
    print 'Calculating prices..'
    calc_prices(False)

if delete:
    query('DELETE', orderUrl)
    query('DELETE', eventUrl)
    query('DELETE', playerUrl)
    query('DELETE', teamUrl)
    query('DELETE', actionUrl)
    query('DELETE', groupUrl)
    query('DELETE', userUrl)
    query('DELETE', entityTypeUrl2)
    query('DELETE', entityTypeUrl1)