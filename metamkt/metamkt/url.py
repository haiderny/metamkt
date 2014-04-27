'''
Created on Aug 18, 2011

@author: Vishakh
'''

from pyramid.url import route_url

fake_url = 'http://foo.bar/'

def formActionURI(request, actionName):
    return route_url('action', request, action=actionName)

def formLeagueURI(request, leagueName):
    return route_url('league', request, group=leagueName.replace(' ', '_'))

def formTeamURI(request, teamName):
    return route_url('team', request, team=teamName.replace(' ', '_'))

def formPlayerURI(request, playerName):
    return route_url('player', request, player=playerName.replace(' ', '_'))

def formUserURI(request, userName):
    return route_url('user', request, user=userName)

def formUserOrdersURI(request, userName):
    #return route_url('user_orders', request, user=userName)
    return fake_url

def formUserTransactionsURI(request, userName):
    #return route_url('user_transactions', request, user=userName)
    return fake_url

def formEntityURI(request, entityObj):
    if entityObj.parent_id == None:
        return formTeamURI(request, entityObj.name)
    return formPlayerURI(request, entityObj.name)
