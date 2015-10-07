__author__ = 'vovacooper'

from py2neo import authenticate, Graph, Node, Relationship, Path, Rev
import requests

import xml.etree.ElementTree as ET
import xmltodict

import sys
import json

from classes.logger import logger
from classes.config import *

reload(sys)
sys.setdefaultencoding('utf8')


authenticate("localhost:7474", "neo4j", "1234")
# authenticate("52.27.227.159:7474", "neo4j", "1234")
graph = Graph(GRAPH_CONNECTION_STRNIG)

def find_multiProp(graph, *labels, **properties):
    results = None
    for l in labels:
        for k,v in properties.iteritems():
            if results == None:
                genNodes = lambda l,k,v: graph.find(l, property_key=k, property_value=v)
                results = [r for r in genNodes(l,k,v)]
                continue
            prevResults = results
            results = [n for n in genNodes(l,k,v) if n in prevResults]
    return results

def merge_one_multiProp(graph, *labels, **properties):
    r = find_multiProp(graph, *labels, **properties)
    if not r:
        # remove tuple association
        node,= graph.create(Node(*labels, **properties))
    else:
        node = r[0]
    return node





def create_shows():
    """
    fills DB with all the shows
    :return:
    """

    for page_num in range(0, 10, 1):
        r = requests.get('http://api.tvmaze.com/shows?page={0}'.format(page_num))
        shows = json.loads(r.text)

        for show in shows:
            show_node = graph.merge_one("Show", 'id', show['id'])

            show_node['url'] = show['url']
            show_node['name'] = show['name']
            show_node['type'] = show['type']
            show_node['status'] = show['status']
            show_node['runtime'] = show['runtime']
            show_node['premiered'] = show['premiered']
            show_node['weight'] = show['weight']

            if show['rating'] is not None:
                show_node['rating'] = show['rating']['average']

            show_node['summary'] = show['summary']

            show_node['img_medium'] = show['image'].get('medium', None)
            show_node['img_original'] = show['image'].get('original', None)
            show_node.push()

            for genre in show.get('genres', []):
                genre_node = graph.merge_one("Genre", 'genre', genre)
                show_of_genre = Relationship(show_node, "of genre", genre_node)
                graph.create_unique(show_of_genre)

            if show['webChannel'] is not None:
                webchannel_node = graph.merge_one("WebChannel", 'id', show['webChannel']['id'])
                webchannel_node['name'] = show['webChannel']['name']
                webchannel_node.push()

                show_of_webchannel = Relationship(show_node, "from", webchannel_node)
                graph.create_unique(show_of_webchannel)

                if show['webChannel']['country'] is not None:
                    country_node = graph.merge_one("Country", 'code', show['webChannel']['country']['code'])
                    country_node['name'] = show['webChannel']['country']['name']
                    country_node['timezone'] = show['webChannel']['country']['timezone']
                    country_node.push()

                    webchannel_from_country = Relationship(webchannel_node, "from", country_node)
                    graph.create_unique(webchannel_from_country)

            if show['network'] is not None:
                network_node = graph.merge_one("Network", 'id', show['network']['id'])
                network_node['name'] = show['network']['name']
                network_node.push()

                show_of_network = Relationship(show_node, "from", network_node)
                graph.create_unique(show_of_network)

                country_node = graph.merge_one("Country", 'code', show['network']['country']['code'])
                country_node['name'] = show['network']['country']['name']
                country_node['timezone'] = show['network']['country']['timezone']
                country_node.push()

                network_from_country = Relationship(network_node, "from", country_node)
                graph.create_unique(network_from_country)

            r1 = requests.get('http://api.tvmaze.com/shows/{0}/episodes'.format(show['id']))
            episodes = json.loads(r1.text)

            for episode in episodes:
                episode_node = graph.merge_one("Episode", 'id', episode['id'])
                episode_node['name'] = episode['name']
                episode_node['season'] = episode['season']
                episode_node['number'] = episode['number']
                episode_node['airdate'] = episode['airdate']
                episode_node['airtime'] = episode['airtime']
                episode_node['airstamp'] = episode['airstamp']
                episode_node['runtime'] = episode['runtime']
                episode_node['summary'] = episode['summary']

                if episode['image'] is not None:
                    episode_node['img_medium'] = episode['image'].get('medium', None)
                    episode_node['img_original'] = episode['image'].get('original', None)

                episode_node.push()

                show_has_episode = Relationship(show_node, "has", episode_node)
                graph.create_unique(show_has_episode)

                search = show['name'] + ' s' + str(episode['season']).zfill(2) + 'e' + str(episode['number']).zfill(2)

                # links
                search_numbers = [3552639851]#, 8556419051]; #, 2649486255, 7079685853, 8416818254, 1870757059, 1731156253,4545021852, 6021755051, 8975221455]

                for n in search_numbers:
                    try:
                        links_from_google = requests.get(
                            'https://www.googleapis.com/customsearch/v1element?key=AIzaSyCVAXiUzRYsML1Pv6RwSG1gunmMikTzQqY&rsz=small&num=10&hl=en&prettyPrint=false&source=gcsc&gss=.com&sig=cb6ef4de1f03dde8c26c6d526f8a1f35&cx=partner-pub-2526982841387487:{1}'
                            '&q={0}&googlehost=www.google.com&oq={0}'.format(search, n))

                        dict_from_google = json.loads(links_from_google.text)
                        for result in dict_from_google['results']:

                            link_node = graph.merge_one("Link", 'url', result['url'])
                            link_node['host'] = result.get('visibleUrl', 'unknown')
                            link_node.push()

                            link_has_episode = Relationship(episode_node, "has", link_node)
                            graph.create(link_has_episode)

                    except Exception, err:
                        logger.exception("error grom google part")






def create_popular_shows():
    """
    create popular shows node and connect all the popular show to it
    :return:
    """
    return

def show_to_country(show_node, country):
    from_country = country
    node_country = graph.merge_one("Country", 'country', from_country)
    #node_country.push()

    show_from_country = Relationship(show_node, "from", node_country)
    graph.create(show_from_country)

def show_to_genre(show_node, genre):
    node_genre = graph.merge_one("Genre", 'name', genre)
    node_genre.push()

    show_of_genre = Relationship(show_node, "of genre", node_genre)
    graph.create(show_of_genre)

def get_season_for_show(show_node, season_no):
    season_node = merge_one_multiProp(graph, *('Season',), **{'no': season_no, 'show_id': show_node[id]})

    show_season = Relationship(show_node, "has", season_node)
    graph.create(show_season)

    return season_node

def get_episode_for_season_show(show_node, season_node, episode):
    episode_node = merge_one_multiProp(graph, *('Episode',), **episode)

    return episode_node


def update_show_info(show_node):
    """
    collect the basic information about the show
    :param show_id:
    :return:
    """
    result_dict = {}
    try:
        # data source (tvrage)
        show_info_e_list = requests.get(
            'http://services.tvrage.com/feeds/full_show_info.php?sid={0}'.format(show_node['id']))
        result_dict = xmltodict.parse(show_info_e_list.text)

        show_node['started'] = result_dict['Show'].get('started', None)
        show_node['ended'] = result_dict['Show'].get('ended', None)
        show_node['image'] = result_dict['Show'].get('image', None)
        show_node['status'] = result_dict['Show'].get('status', None)

        # data source (omdbapi)
        omdb_show_info = requests.get(
            'http://www.omdbapi.com/?t={0}&y=&plot=full&r=json'.format(show_node['name']))
        dict_omdb_show_info = json.loads(omdb_show_info.text)
        if dict_omdb_show_info['Response'] == 'True':
            for key, value in dict_omdb_show_info.iteritems():
                show_node[key] = value
        show_node.push()

    except ValueError as e:
        logger.exception("Value Error")
        return
    except Exception as e:
        logger.exception("Some network issue, will try again")

    # Country
    show_to_country(show_node, result_dict['Show'].get('origin_country', 'unknown'))

    #Genres
    if result_dict['Show'].get('genres', None) is not None:
        genre_list = []
        if type(result_dict['Show']['genres']['genre']) is list:
            genre_list = result_dict['Show']['genres']['genre']
        else:
            genre_list.append(result_dict['Show']['genres']['genre'])

        for genre in genre_list:
            show_to_genre(show_node, genre)

    #Seasons
    season_list = []
    if result_dict['Show'].get('Episodelist', None) is None:
        # there are no seasons for this show
        return

    if type(result_dict['Show']['Episodelist']['Season']) is list:
        season_list = result_dict['Show']['Episodelist']['Season']
    else:
        season_list.append(result_dict['Show']['Episodelist']['Season'])

    for season in season_list:
        season_node = get_season_for_show(show_node, season['@no'])

        #Episodes
        episode_list = []
        if type(season['episode']) is list:
            episode_list = season['episode']
        else:
            episode_list.append(season['episode'])

        count = 1
        for episode in episode_list:
            episode_basic_info = {
                'airdate': episode.get('airdate', None),
                'epnum': count,
                'screencap': episode.get('screencap', None),
                'title': episode.get('title', None)
            }
            episode_node = get_episode_for_season_show(show_node, season_node ,episode_basic_info)

            # Add episode info
            try:
                omdb_episode_info = requests.get('http://www.omdbapi.com/?t={0}&Season={1}&Episode={2}'
                                                 .format(show_node['name'],
                                                         season_node['no'],
                                                         episode_node['epnum']))

                dict_omdb_episode_info = json.loads(omdb_episode_info.text)

                if dict_omdb_episode_info['Response'] == 'True':
                    for key, value in dict_omdb_episode_info.iteritems():
                        episode_node[key] = value
                episode_node.push()
            except ValueError as e:
                logger.exception("Value error")
            except Exception as e:
                logger.exception("network issue: wil try again")
                success = True

            show_episode = Relationship(season_node, "has", episode_node)
            graph.create(show_episode)
            count = count + 1


def update_links():
    results = graph.cypher.stream("match (s:Show)-->(se:Season)-->(e:Episode) return s.name,se.no,e.epnum,id(e) as eid")

    for record in results:
        search = record['s.name'] + ' s' + str(record['e.epnum']).zfill(2) + 'e' + str(record['se.no']).zfill(2)

        # links
        search_numbers = [3552639851, 8556419051, 2649486255, 7079685853, 8416818254, 1870757059, 1731156253,
                          4545021852, 6021755051, 8975221455]

        for n in search_numbers:
            try:
                links_from_google = requests.get(
                    'https://www.googleapis.com/customsearch/v1element?key=AIzaSyCVAXiUzRYsML1Pv6RwSG1gunmMikTzQqY&rsz=small&num=10&hl=en&prettyPrint=false&source=gcsc&gss=.com&sig=cb6ef4de1f03dde8c26c6d526f8a1f35&cx=partner-pub-2526982841387487:{1}'
                    '&q={0}&googlehost=www.google.com&oq={0}'.format(search, n))

                dict_from_google = json.loads(links_from_google.text)
                for result in dict_from_google['results']:

                    link_node = graph.merge_one("Link", 'url', result['url'])
                    link_node['host'] = result.get('visibleUrl', 'unknown')
                    link_node.push()

                    episode_node = graph.node(record['eid'])
                    link_episode = Relationship(episode_node, "has", link_node)
                    graph.create(link_episode)

            except Exception, err:
                logger.exception("error grom google part")


def main():
    graph.delete_all()
    logger.info(" + Creating shows")
    create_shows()
    logger.info(" - Finished Creating shows")

    #logger.info("Creating popular shows")
    #create_popular_shows()
    #logger.info(" - Finished Creating popular shows")
    """
    logger.info(" + Start updating Show info")
    results = graph.cypher.stream("match (s:Show) return id(s) as eid")
    #results = graph.cypher.stream("match (p:Popular)-->(s:Show) return id(s) as eid")
    for record in results:
        node_show = graph.node(record['eid'])
        logger.info(" + Updating show info for: {0}".format(node_show['name']))
        update_show_info(node_show)
        logger.info(" - Finished updating show info for: {0}".format(node_show['name']))
    logger.info(" - Finished updating Show info")

    logger.info(" + Start updating links")
    update_links()
    logger.info(" - Finished updating links")
    """

def update_show_info_old():
    print 'updating show info'
    authenticate("localhost:7474", "neo4j", "1234")
    graph = Graph(GRAPH_CONNECTION_STRNIG)

    results = graph.cypher.stream("match (s:Show) return id(s) as eid,s.id")
    start_id = 764
    for record in results:
        if int(record['s.id']) < start_id:
            continue

        node_show = graph.node(record['eid'])

        result_dict = {}

        success = True
        while success:
            try:
                show_info_e_list = requests.get(
                    'http://services.tvrage.com/feeds/full_show_info.php?sid={0}'.format(node_show['id']))
                result_dict = xmltodict.parse(show_info_e_list.text)

                omdb_show_info = requests.get(
                    'http://www.omdbapi.com/?t={0}&y=&plot=full&r=json'.format(node_show['name']))
                dict_omdb_show_info = json.loads(omdb_show_info.text)
                if dict_omdb_show_info['Response'] == 'True':
                    for key, value in dict_omdb_show_info.iteritems():
                        node_show[key] = value
                success = False
            except ValueError as e:
                logger.exception("Value Error")
                continue
            except Exception as e:
                logger.exception("Some network issue, will try again")
                success = True

        print str(node_show['name'])
        # info

        node_show['started'] = result_dict['Show'].get('started', None)
        node_show['ended'] = result_dict['Show'].get('ended', None)
        node_show['image'] = result_dict['Show'].get('image', None)
        node_show['status'] = result_dict['Show'].get('status', None)
        node_show.push()

        #Country
        from_country = result_dict['Show'].get('origin_country', 'unknown')

        node_country = graph.merge_one("Country", 'country', from_country)
        node_country.push()

        show_from_country = Relationship(node_show, "from", node_country)
        graph.create(show_from_country)


        #Genres
        if result_dict['Show'].get('genres', None) is not None:
            genre_list = []
            if type(result_dict['Show']['genres']['genre']) is list:
                genre_list = result_dict['Show']['genres']['genre']
            else:
                genre_list.append(result_dict['Show']['genres']['genre'])

            for genre in genre_list:
                node_genre = graph.merge_one("Genre", 'name', genre)
                node_genre.push()

                show_of_genre = Relationship(node_show, "of genre", node_genre)
                graph.create(show_of_genre)

        """try:
            print node_show['started']
            a = node_show['started'].split("/")
            if int(a[len(a)-1]) < 2000:
                continue
        except Exception:
            continue
        """

        #Seasons
        season_list = []
        if result_dict['Show'].get('Episodelist', None) is None:
            continue
        if type(result_dict['Show']['Episodelist']['Season']) is list:
            season_list = result_dict['Show']['Episodelist']['Season']
        else:
            season_list.append(result_dict['Show']['Episodelist']['Season'])

        for season in season_list:
            node_season = Node.cast('Season', {'no': season['@no']})
            graph.create(node_season)

            show_season = Relationship(node_show, "has", node_season)
            graph.create(show_season)

            #Episodes
            episode_list = []
            if type(season['episode']) is list:
                episode_list = season['episode']
            else:
                episode_list.append(season['episode'])
            count = 1
            for episode in episode_list:
                node_episode = Node.cast('Episode', {
                    'airdate': episode.get('airdate', None),
                    'epnum': count,
                    'screencap': episode.get('screencap', None),
                    'title': episode.get('title', None)
                })
                graph.create(node_episode)

                success = True
                while success:
                    try:
                        omdb_episode_info = requests.get('http://www.omdbapi.com/?t={0}&Season={1}&Episode={2}'
                                                         .format(node_show['name'],
                                                                 node_season['no'],
                                                                 node_episode['epnum']))
                        dict_omdb_episode_info = json.loads(omdb_episode_info.text)
                        if dict_omdb_episode_info['Response'] == 'True':
                            for key, value in dict_omdb_episode_info.iteritems():
                                node_episode[key] = value
                        node_episode.push()
                        success = False
                    except ValueError as e:
                        logger.exception("Value error")
                        continue
                    except Exception as e:
                        logger.exception("network issue: wil try again")
                        success = True

                show_episode = Relationship(show_season, "has", node_episode)
                graph.create(show_episode)
                count = count + 1

    print 'end updating show info'


def update_info_and_links():
    print 'updating show info'
    authenticate("localhost:7474", "neo4j", "1234")
    graph = Graph(GRAPH_CONNECTION_STRNIG)

    results = graph.cypher.stream("match (s:Show) return id(s) as eid,s.id")
    start_id = 0
    for record in results:
        if int(record['s.id']) < start_id:
            continue

        node_show = graph.node(record['eid'])

        result_dict = {}

        success = True
        while success:
            try:
                show_info_e_list = requests.get(
                    'http://services.tvrage.com/feeds/full_show_info.php?sid={0}'.format(node_show['id']))
                result_dict = xmltodict.parse(show_info_e_list.text)

                omdb_show_info = requests.get(
                    'http://www.omdbapi.com/?t={0}&y=&plot=full&r=json'.format(node_show['name']))
                dict_omdb_show_info = json.loads(omdb_show_info.text)
                if dict_omdb_show_info['Response'] == 'True':
                    for key, value in dict_omdb_show_info.iteritems():
                        node_show[key] = value
                success = False
            except ValueError as e:
                logger.exception("Value error")
                continue
            except Exception as e:
                logger.exception("Some network issue: will try again")
                success = True

        print str(node_show['name'])
        # info

        node_show['started'] = result_dict['Show'].get('started', None)
        node_show['ended'] = result_dict['Show'].get('ended', None)
        node_show['image'] = result_dict['Show'].get('image', None)
        node_show['status'] = result_dict['Show'].get('status', None)
        node_show.push()

        #Country
        from_country = result_dict['Show'].get('origin_country', 'unknown')

        node_country = graph.merge_one("Country", 'country', from_country)
        node_country.push()

        show_from_country = Relationship(node_show, "from", node_country)
        graph.create(show_from_country)


        #Genres
        if result_dict['Show'].get('genres', None) is not None:
            genre_list = []
            if type(result_dict['Show']['genres']['genre']) is list:
                genre_list = result_dict['Show']['genres']['genre']
            else:
                genre_list.append(result_dict['Show']['genres']['genre'])

            for genre in genre_list:
                node_genre = graph.merge_one("Genre", 'name', genre)
                node_genre.push()

                show_of_genre = Relationship(node_show, "of genre", node_genre)
                graph.create(show_of_genre)
        """
        try:
            print node_show['started']
            a = node_show['started'].split("/")
            if int(a[len(a)-1]) < 2000:
                continue
        except Exception:
            continue
        """


        #Seasons
        season_list = []
        if result_dict['Show'].get('Episodelist', None) is None:
            continue
        if type(result_dict['Show']['Episodelist']['Season']) is list:
            season_list = result_dict['Show']['Episodelist']['Season']
        else:
            season_list.append(result_dict['Show']['Episodelist']['Season'])

        for season in season_list:
            node_season = Node.cast('Season', {'no': season['@no']})
            graph.create(node_season)

            show_season = Relationship(node_show, "has", node_season)
            graph.create(show_season)

            #Episodes
            episode_list = []
            if type(season['episode']) is list:
                episode_list = season['episode']
            else:
                episode_list.append(season['episode'])
            count = 1
            for episode in episode_list:
                node_episode = Node.cast('Episode', {
                    'airdate': episode.get('airdate', None),
                    'epnum': count,
                    'screencap': episode.get('screencap', None),
                    'title': episode.get('title', None)
                })
                graph.create(node_episode)

                success = True
                while success:
                    try:
                        omdb_episode_info = requests.get('http://www.omdbapi.com/?t={0}&Season={1}&Episode={2}'
                                                         .format(node_show['name'],
                                                                 node_season['no'],
                                                                 node_episode['epnum']))
                        dict_omdb_episode_info = json.loads(omdb_episode_info.text)
                        if dict_omdb_episode_info['Response'] == 'True':
                            for key, value in dict_omdb_episode_info.iteritems():
                                node_episode[key] = value
                        node_episode.push()

                        success = False
                    except ValueError as e:
                        logger.exception("Value error")
                        continue
                    except Exception as e:
                        logger.exception("Some network issue: will try again")
                        success = True

                try:

                    search = node_show['name'] + ' s' + str(node_season['no']).zfill(2) + 'e' + str(node_episode['epnum']).zfill(2)

                    print search
                    #links
                    search_numbers = [3552639851, 8556419051, 2649486255, 7079685853, 8416818254, 1870757059,
                                      1731156253, 4545021852, 6021755051, 8975221455]

                    for n in search_numbers:
                        links_from_google = requests.get(
                            'https://www.googleapis.com/customsearch/v1element?key=AIzaSyCVAXiUzRYsML1Pv6RwSG1gunmMikTzQqY&rsz=small&num=10&hl=en&prettyPrint=false&source=gcsc&gss=.com&sig=cb6ef4de1f03dde8c26c6d526f8a1f35&cx=partner-pub-2526982841387487:{1}'
                            '&q={0}&googlehost=www.google.com&oq={0}'.format(search, n))

                        dict_from_google = json.loads(links_from_google.text)
                        for result in dict_from_google['results']:
                            node_link = Node.cast('Link', {
                                'host': result.get('visibleUrl', None),
                                'url': result['url']
                            })
                            graph.create(node_link)
                            link_episode = Relationship(node_episode, "has", node_link)
                            graph.create(link_episode)
                except Exception, err:
                    logger.exception("error grom google part")

                show_episode = Relationship(show_season, "has", node_episode)
                graph.create(show_episode)
                count = count + 1

    print 'end updating show info'


def main1():
    authenticate("localhost:7474", "neo4j", "1234")
    graph = Graph(GRAPH_CONNECTION_STRNIG)

    graph.delete_all()

    banana = Node("Fruit", name="banana", colour="yellow", tasty=True)
    graph.create(banana)

    t = graph.merge_one("Fruit", 'name', 'apple')
    t['colour'] = 'green'
    t['tasty'] = True
    t.push()


def main2():
    authenticate("localhost:7474", "neo4j", "1234")
    graph = Graph(GRAPH_CONNECTION_STRNIG)

    graph.delete_all()

    banana = Node("Fruit", name="banana", colour="yellow", tasty=True)
    graph.create(banana)

    t = graph.merge_one("Fruit", 'name', 'apple')
    t['colour'] = 'green'
    t['tasty'] = True
    t.push()

    alice = Node("Person", name="Alice")
    bob = Node("Person", name="Bob")
    alice_knows_bob = Relationship(alice, "KNOWS", bob, since=1999)
    graph.create(alice)
    graph.create(bob)
    graph.create(alice_knows_bob)


if __name__ == "__main__":
    main()



