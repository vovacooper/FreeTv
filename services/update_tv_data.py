__author__ = 'vovacooper'

from py2neo import authenticate, Graph, Node, Relationship, Path, Rev
import requests

import xml.etree.ElementTree as ET
import xmltodict

import sys
import json

reload(sys)
sys.setdefaultencoding('utf8')


GRAPH_CONNECTION_STRNIG = 'http://localhost:7474/db/data/'



def CreateShows():
    print 'creating shows'
    authenticate("localhost:7474", "neo4j", "1234")
    graph = Graph(GRAPH_CONNECTION_STRNIG)

    graph.delete_all()

    r = requests.get('http://services.tvrage.com/feeds/show_list.php')
    root = ET.fromstring(r.text)

    shows = []
    for child in root:
        show = {}
        for c in child:
            show[c.tag] = c.text
        shows.append(show)
        nShow = Node.cast('Show', show)
        graph.create(nShow)
        print show['name']
    print 'end creating shows'

def update_show_info():
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
                show_info_e_list = requests.get('http://services.tvrage.com/feeds/full_show_info.php?sid={0}'.format(node_show['id']))
                result_dict = xmltodict.parse(show_info_e_list.text)

                omdb_show_info = requests.get('http://www.omdbapi.com/?t={0}&y=&plot=full&r=json'.format(node_show['name']))
                dict_omdb_show_info = json.loads(omdb_show_info.text)
                if dict_omdb_show_info['Response'] == 'True':
                    for key, value in dict_omdb_show_info.iteritems():
                        node_show[key] = value
                success = False
            except ValueError as e:
                print e
                continue
            except Exception as e:
                print e
                print 'Tring again'
                success = True

        print node_show['name']
        #info

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

        try:
            print node_show['started']
            a = node_show['started'].split("/")
            if int(a[len(a)-1]) < 2000:
                continue
        except Exception:
            continue


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
                        print e
                        continue
                    except Exception as e:
                        print e
                        print 'Tring again'
                        success = True


                show_episode = Relationship(show_season, "has", node_episode)
                graph.create(show_episode)
                count = count + 1

    print 'end updating show info'

def update_links():
    authenticate("localhost:7474", "neo4j", "1234")
    graph = Graph(GRAPH_CONNECTION_STRNIG)

    results = graph.cypher.stream("match (s:Show)-->(se:Season)-->(e:Episode) return s.name,se.no,e.epnum,id(e) as eid")

    for record in results:
        search = record['s.name'] + ' s' + str(record['e.epnum']).zfill(2) + 'e' + str(record['se.no']).zfill(2)

        print search
        #links
        search_numbers = [3552639851, 8556419051, 2649486255, 7079685853, 8416818254, 1870757059, 1731156253, 4545021852, 6021755051, 8975221455]

        for n in search_numbers:
            links_from_google = requests.get('https://www.googleapis.com/customsearch/v1element?key=AIzaSyCVAXiUzRYsML1Pv6RwSG1gunmMikTzQqY&rsz=small&num=10&hl=en&prettyPrint=false&source=gcsc&gss=.com&sig=cb6ef4de1f03dde8c26c6d526f8a1f35&cx=partner-pub-2526982841387487:{1}'
                         '&q={0}&googlehost=www.google.com&oq={0}'.format(search, n))

            dict_from_google = json.loads(links_from_google.text)
            for result in dict_from_google['results']:
                node_link = Node.cast('Link', {
                    'host': result.get('visibleUrl', None),
                    'url': result['url']
                })
                graph.create(node_link)
                node_episode = graph.node(record['eid'])
                link_episode = Relationship(node_episode, "has", node_link)
                graph.create(link_episode)


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
    #CreateShows()
    update_show_info()
    #update_links()

