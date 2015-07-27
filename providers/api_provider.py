from py2neo import authenticate, Graph, Node, Relationship, Path, Rev
import requests

import xml.etree.ElementTree as ET
import xmltodict

import sys
import json

from classes.logger import logger

from uuid import uuid4
import random
from datetime import datetime, timedelta
import datetime
import json
from classes.config import *


class ApiProvider():
    def __init__(self, request_data):
        self._request_data = request_data
        # authenticate("localhost:7474", "neo4j", "1234")
        authenticate("52.27.227.159:7474", "neo4j", "1234")
        self.graph = Graph(GRAPH_CONNECTION_STRNIG)


    def _update_show(self, show_id):
        # get the node from the graph
        node_show = self.graph.node(show_id)
        if node_show['updated'] == True:
            return

        result_dict = {}
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
            return
        except Exception as e:
            logger.exception("Some network issue, will try again")
            return

        # add the new extracted data to the show
        node_show['started'] = result_dict['Show'].get('started', None)
        node_show['ended'] = result_dict['Show'].get('ended', None)
        node_show['image'] = result_dict['Show'].get('image', None)
        node_show['status'] = result_dict['Show'].get('status', None)
        node_show.push()

        # Country
        from_country = result_dict['Show'].get('origin_country', 'unknown')
        node_country = self.graph.merge_one("Country", 'country', from_country)
        node_country.push()

        # add the relation to the graph
        show_from_country = Relationship(node_show, "from", node_country)
        self.graph.create(show_from_country)

        # Genres
        if result_dict['Show'].get('genres', None) is not None:
            genre_list = []
            if type(result_dict['Show']['genres']['genre']) is list:
                genre_list = result_dict['Show']['genres']['genre']
            else:
                genre_list.append(result_dict['Show']['genres']['genre'])

            for genre in genre_list:
                # create the genre node
                node_genre = self.graph.merge_one("Genre", 'name', genre)
                node_genre.push()

                # add the Genre relation to the graph
                show_of_genre = Relationship(node_show, "of genre", node_genre)
                self.graph.create(show_of_genre)

        # Seasons
        season_list = []
        if result_dict['Show'].get('Episodelist', None) is None:
            return
        if type(result_dict['Show']['Episodelist']['Season']) is list:
            season_list = result_dict['Show']['Episodelist']['Season']
        else:
            season_list.append(result_dict['Show']['Episodelist']['Season'])

        for season in season_list:
            # create node for season
            node_season = Node.cast('Season', {'no': season['@no']})
            self.graph.create(node_season)

            # create the relation n the graph
            show_season = Relationship(node_show, "has", node_season)
            self.graph.create(show_season)

            # Episodes
            episode_list = []
            if type(season['episode']) is list:
                episode_list = season['episode']
            else:
                episode_list.append(season['episode'])

            count = 1
            for episode in episode_list:
                # create a node for episode
                node_episode = Node.cast('Episode', {
                    'airdate': episode.get('airdate', None),
                    'epnum': count,
                    'screencap': episode.get('screencap', None),
                    'title': episode.get('title', None)
                })
                self.graph.create(node_episode)

                # add relation to the graph
                show_episode = Relationship(show_season, "has", node_episode)
                self.graph.create(show_episode)


                # Episode info
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
                except ValueError as e:
                    logger.exception("Value error")

                except Exception as e:
                    logger.exception("network issue: wil try again")

                # links
                try:
                    search = node_show['name'] + ' s' + str(node_season['no']).zfill(2) + 'e' + str(
                        node_episode['epnum']).zfill(2)

                    # links
                    search_numbers = [3552639851, 8556419051, 2649486255, 7079685853, 8416818254, 1870757059,
                                      1731156253, 4545021852, 6021755051, 8975221455]

                    for n in search_numbers:
                        links_from_google = requests.get(
                            'https://www.googleapis.com/customsearch/v1element?key=AIzaSyCVAXiUzRYsML1Pv6RwSG1gunmMikTzQqY&rsz=small&num=10&hl=en&prettyPrint=false&source=gcsc&gss=.com&sig=cb6ef4de1f03dde8c26c6d526f8a1f35&cx=partner-pub-2526982841387487:{1}'
                            '&q={0}&googlehost=www.google.com&oq={0}'.format(search, n))

                        dict_from_google = json.loads(links_from_google.text)
                        for result in dict_from_google['results']:
                            # create node for link
                            node_link = Node.cast('Link', {
                                'host': result.get('visibleUrl', None),
                                'url': result['url']
                            })
                            self.graph.create(node_link)

                            # create the relation in the graph
                            link_episode = Relationship(node_episode, "has", node_link)
                            self.graph.create(link_episode)
                except Exception, err:
                    logger.exception("error grom google part")
                count = count + 1

        # notify that all went OK and finish
        node_show['updated'] = True
        node_show.push()


    def get_shows(self):
        """
        get all shows

        :return:
        {
            id: int,
            name: str,
            poster: url,
            episodes, { length: int }
        }
        """
        # TODO add 0-9 search
        if self._request_data.get('alphabet', None) is not None:
            if self._request_data.get('genre', None) is not None:
                query = "match (g:Genre {name:" + self._request_data[
                    'genre'] + "})<--(s:Show) WHERE s.name =~ '" + self._request_data.get(
                    'alphabet') + ".*' return s limit 8"
            else:
                query = "match (s:Show) WHERE s.name =~ '" + self._request_data.get(
                    'alphabet') + ".*' return s limit 8"

        else:
            if self._request_data.get('genre', None) is not None:
                query = "match (g:Genre {name:'" + self._request_data['genre'] + "'})<--(s:Show) return s limit 8"
            else:
                query = "match (s:Show) return s limit 8"

        results = self.graph.cypher.stream(query)

        resp = []
        for record in results:
            resp.append({'id': record.s['id'], 'name': record.s['name'], 'poster': record.s['img_medium'],
                         'episodes': {'length': 20}})

        return resp

    def get_shows_names(self):
        """
        get all shows

        :return:
        {
            id: int,
            name: str,
            poster: url,
            episodes, { length: int }
        }
        """

        query = "MATCH (n:Show) RETURN n.name,n.id limit 1000"

        results = self.graph.cypher.execute(query)

        resp = []
        for record in results:
            resp.append({'id': record['n.id'], 'name': record['n.name']})
        return resp


    def get_show(self, show_id):
        """
        :param id:
        :return:
        {
            name: str,
            rating: float,
            airsDayOfWeek: [day1,...],
            airsTime: time,
            overview: str,
            poster: url,
            seasons:{
                1:{
                    episodes: {
                        episodeName: str,
                        season: int,
                        episodeNumber: int,
                        firstAired: date,
                        overview: str,
                    }
                },...
            }
        }
        """
        #self.graph.delete_all()
        # fill the show on the first time!

        #results = self.graph.cypher.execute("MATCH (n:Show {id:'" + str(show_id) +  "'}) return id(n) as id")

        #node_id = None
        #for record in results:
        #    node_id = record['id']
        #if node_id is None:
        #    return
        #self._update_show(node_id)

        results = self.graph.cypher.stream(
            "match (s:Show {id:" + str(show_id) + "})-->(e:Episode) return s,e")

        resp = {'episodes': []}
        for record in results:
            resp['episodes'].append({
                'episodeName': record.e['name'],
                'season': record.e['season'],
                'episodeNumber': record.e['number'],
                'firstAired': record.e['airdate'],
                'overview': record.e['airdate']
            })
            resp['name'] =record.s['name']
            resp['rating'] =1
            resp['airsDayOfWeek'] = 'a'
            resp['airsTime'] =record.s['runtime']
            resp['overview'] =record.s['summary']
            resp['poster'] =record.s['img_medium']
        return resp






if __name__ == "__main__":
    rp = ApiProvider({})

