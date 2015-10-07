__author__ = 'vovacooper'
import datetime
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
        for k, v in properties.iteritems():
            if results == None:
                genNodes = lambda l, k, v: graph.find(l, property_key=k, property_value=v)
                results = [r for r in genNodes(l, k, v)]
                continue
            prevResults = results
            results = [n for n in genNodes(l, k, v) if n in prevResults]
    return results


def merge_one_multiProp(graph, *labels, **properties):
    r = find_multiProp(graph, *labels, **properties)
    if not r:
        # remove tuple association
        node, = graph.create(Node(*labels, **properties))
    else:
        node = r[0]
    return node


def show_to_country(show_node, country):
    from_country = country
    node_country = graph.merge_one("Country", 'country', from_country)
    # node_country.push()

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







class TVMaze():
    """
    source: http://www.tvmaze.com/api
    """
    def get_all_shows(self):
        i = 0
        shows = []
        while True:
            r = requests.get('http://api.tvmaze.com/shows?page={0}'.format(i))
            if r.status_code == 404:
                break
            new_shows = json.loads(r.text)
            i += 1
            shows += new_shows
        return shows

    def get_show(self, show_id):
        r = requests.get('http://api.tvmaze.com/shows/{0}'.format(show_id))
        show = json.loads(r.text)
        return show

    def get_show_episodes(self, show_id):
        r = requests.get('http://api.tvmaze.com/shows/{0}/episodes'.format(show_id))
        episodes = json.loads(r.text)
        return episodes

    def get_show_episode(self, show_id, season, episode):
        r = requests.get(
            'http://api.tvmaze.com/shows/{0}/episodebynumber?season={1}&number={2}'.format(show_id, season, episode))
        episodes = json.loads(r.text)
        return episodes


class LinksProvider():

    def get_links_for_episode(self, show, season, episode):
        search = show + ' s' + str(season).zfill(2) + 'e' + str(episode).zfill(2)

        search_numbers = [
            3552639851]  # , 8556419051]; #, 2649486255, 7079685853, 8416818254, 1870757059, 1731156253,4545021852, 6021755051, 8975221455]

        links = []
        return links
        for n in search_numbers:
            try:
                links_from_google = requests.get(
                    'https://www.googleapis.com/customsearch/v1element?key=AIzaSyCVAXiUzRYsML1Pv6RwSG1gunmMikTzQqY&rsz=small&num=10&hl=en&prettyPrint=false&source=gcsc&gss=.com&sig=cb6ef4de1f03dde8c26c6d526f8a1f35&cx=partner-pub-2526982841387487:{1}'
                    '&q={0}&googlehost=www.google.com&oq={0}'.format(search, n))

                dict_from_google = json.loads(links_from_google.text)
                for result in dict_from_google['results']:
                    links.append({
                        "url": result['url'],
                        "host": result.get('visibleUrl', 'unknown')
                    })
            except Exception, err:
                logger.exception("error grom google part")

        return links


class CreateDB():
    def __init__(self):
        authenticate("localhost:7474", "neo4j", "1234")
        # authenticate("52.27.227.159:7474", "neo4j", "1234")
        self.graph = Graph(GRAPH_CONNECTION_STRNIG)

        self.link_provider = LinksProvider()
        self.tvmaze = TVMaze()
        self.shows = []

        self.node_count = 0
        self.relationship_count = 0

    def create_shows(self):
        self.graph.delete_all()
        self.shows = self.tvmaze.get_all_shows()
        print " ++++  Creating {0} shows".format(len(self.shows))
        show_count = 0
        for show in self.shows:
            starttime = datetime.datetime.now()
            print "{0}:---------------------------------------------".format(show_count)
            show_count += 1
            print " + Creating show: {0}".format(show["name"])
            show_node = self.create_show(show)

            for genre in show.get('genres', []):
                genre_node = self.create_genre(genre)
                self.set_show_genre_relationship(show_node, genre_node)

            if show['webChannel'] is not None:
                webchannel_node = self.create_web_channel(show['webChannel'])
                self.create_web_channel_show_relationship(genre_node, webchannel_node)

            if show['network'] is not None:
                network_node = self.create_network(show['network'])
                self.create_network_show_relationship(network_node, show_node)

            episode_nodes = self.create_episodes(show)
            print " + creating {0} episodes".format(len(episode_nodes))
            for episode_node in episode_nodes:
                self.create_episode_show_relationship(episode_node, show_node)
            endtime = datetime.datetime.now()
            deltatime = endtime-starttime
            print " - Operation took " + str(deltatime)
            print " - total Nodes: {0}, Relationships: {1}".format(self.node_count, self.relationship_count)
            print " - finished creating show"

        print " --- finished creating shows"
        return

    def create_show(self, show):
        show_node = graph.merge_one("Show", 'id', show['id'])

        show_node['url'] = show['url']
        show_node['name'] = show['name']
        show_node['type'] = show['type']
        show_node['status'] = show['status']
        show_node['runtime'] = show['runtime']
        show_node['premiered'] = show['premiered']
        show_node['weight'] = show['weight']
        show_node['summary'] = show['summary']
        show_node['img_medium'] = show['image'].get('medium', None)
        show_node['img_original'] = show['image'].get('original', None)

        if show['rating'] is not None:
            show_node['rating'] = show['rating']['average']

        show_node.push()
        self.node_count += 1
        return show_node

    """
    Genre
    """
    def create_genre(self, genre):
        genre_node = graph.merge_one("Genre", 'genre', genre)
        self.node_count += 1
        return genre_node

    def set_show_genre_relationship(self, show_node, genre_node):
        show_of_genre = Relationship(show_node, "of genre", genre_node)
        graph.create_unique(show_of_genre)
        self.relationship_count += 1
        return genre_node

    """
    Network
    """
    def create_network(self, network):
        network_node = graph.merge_one("Network", 'id', network['id'])
        network_node['name'] = network['name']
        network_node.push()
        self.node_count += 1

        if network['country'] is not None:
            country_node = self.create_country(network['country'])
            self.create_network_show_relationship(country_node, network_node)

        return network_node

    def create_network_show_relationship(self, network_node, show_node):
        show_of_network = Relationship(show_node, "from", network_node)
        graph.create_unique(show_of_network)
        self.relationship_count += 1
        return show_of_network
    """
    WebChannel
    """
    def create_web_channel(self, webChannel):
        webchannel_node = graph.merge_one("WebChannel", 'id', webChannel['id'])
        webchannel_node['name'] = webChannel['name']
        webchannel_node.push()
        self.node_count += 1

        if webChannel['country'] is not None:
            country_node = self.create_country(webChannel['country'])
            self.create_country_web_channel_relationship(country_node, webchannel_node)

        return webchannel_node

    def create_web_channel_show_relationship(self, show_node, webchannel_node ):
        show_of_webchannel = Relationship(show_node, "from", webchannel_node)
        graph.create_unique(show_of_webchannel)
        self.relationship_count += 1
        return show_of_webchannel

    """
    Country
    """
    def create_country(self, country):
        country_node = graph.merge_one("Country", 'code', country['code'])
        country_node['name'] = country['name']
        country_node['timezone'] = country['timezone']
        country_node.push()
        self.node_count += 1
        return country_node

    def create_country_web_channel_relationship(self, country_node, webchannel_node):
        webchannel_from_country = Relationship(webchannel_node, "from", country_node)
        graph.create_unique(webchannel_from_country)
        self.relationship_count += 1
        return webchannel_from_country

    def create_network_show_relationship(self, country_node, network_node ):
        network_from_country = Relationship(network_node, "from", country_node)
        graph.create_unique(network_from_country)
        self.relationship_count += 1
        return network_from_country

    """
    Episodes
    """
    def create_episodes(self, show):
        episodes = self.tvmaze.get_show_episodes(show["id"])

        episode_nodes = []
        for episode in episodes:
            episode_node = self.create_episode(episode)
            episode_nodes.append(episode_node)

            episode_links = self.link_provider.get_links_for_episode(show['name'], episode['season'], episode['number'])

            for link in episode_links:
                link_node = self.create_link(link)
                self.create_link_episode_relationship(link_node, episode_node)

        return episode_nodes

    def create_episode(self, episode):
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
        self.node_count += 1
        return episode_node

    def create_episode_show_relationship(self, episode_node, show_node):
        show_has_episode = Relationship(show_node, "has", episode_node)
        graph.create_unique(show_has_episode)
        self.relationship_count += 1
        return show_has_episode

    """
    Link
    """
    def create_link(self, link):
        """
        :param link:
        {
            url: str,
            host: str
        }
        :return:
        """
        link_node = graph.merge_one("Link", 'url', link['url'])
        link_node['host'] = link["host"]
        link_node.push()
        self.node_count += 1
        return link_node

    def create_link_episode_relationship(self, link_node, episode_node):
        link_has_episode = Relationship(episode_node, "has", link_node)
        graph.create(link_has_episode)
        self.relationship_count += 1
        return link_has_episode


if __name__ == "__main__":
    cdb = CreateDB()
    cdb.create_shows()

