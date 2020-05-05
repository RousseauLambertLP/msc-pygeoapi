# ========================295=========================================
#
# Author: Louis-Philippe Rousseau-Lambert
#         <Louis-Philippe.RousseauLambert2@canada.ca>
#
# Copyright (c) 2020 Louis-Philippe Rousseau-Lambert
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# =================================================================

import click
from datetime import datetime, timedelta
import json
import logging
from lxml import etree
import os
import re
import sys

from msc_pygeoapi.env import (MSC_PYGEOAPI_ES_TIMEOUT, MSC_PYGEOAPI_ES_URL,
                              MSC_PYGEOAPI_ES_AUTH, MSC_PYGEOAPI_BASEPATH,
                              GEOMET_WEATHER_BASEPATH)
from msc_pygeoapi.loader.base import BaseLoader
from msc_pygeoapi.util import click_abort_if_false, get_es

LOGGER = logging.getLogger(__name__)

# cleanup settings
DAYS_TO_KEEP = 30

# Alerts by increasing severity
ALERTS_LEVELS = ['advisory', 'statement', 'watch', 'warning']

# Index settings
INDEX_NAME = 'cap_alerts'

SETTINGS = {
    'settings': {
        'number_of_shards': 1,
        'number_of_replicas': 0
    },
    'mappings': {
        'properties': {
            'geometry': {
                'type': 'geo_shape'
            },
            'properties': {
                'properties': {
                    'identifier': {
                        'type': 'text',
                        'fields': {
                            'raw': {
                                'type': 'keyword'
                            }
                        }
                    },
                    'area': {
                        'type': 'text',
                        'fields': {
                            'raw': {
                                'type': 'keyword'
                            }
                        }
                    },
                    'reference': {
                        'type': 'text',
                        'fields': {
                            'raw': {
                                'type': 'keyword'
                            }
                       }
                    },
                    'zone': {
                        'type': 'text',
                        'fields': {
                            'raw': {
                                'type': 'keyword'
                            }
                        }
                    },
                    'headline': {
                        'type': 'text',
                        'fields': {
                            'raw': {
                                'type': 'keyword'
                            }
                        }
                    },
                    'titre': {
                        'type': 'text',
                        'fields': {
                            'raw': {
                                'type': 'keyword'
                            }
                        }
                    },
                    'descrip_en': {
                        'type': 'text',
                        'fields': {
                            'raw': {
                                'type': 'keyword'
                            }
                        }
                    },
                    'descrip_fr': {
                        'type': 'text',
                        'fields': {
                            'raw': {
                                'type': 'keyword'
                            }
                        }
                    },
                    'effective': {
                        'type': 'date',
                        'format': "YYYY-MM-DD'T'HH:mm:ss'Z'"
                    },
                    'expires': {
                        'type': 'date',
                        'format': "YYYY-MM-DD'T'HH:mm:ss'Z'"
                    },
                    'alert_type': {
                        'type': 'text',
                        'fields': {
                            'raw': {
                                'type': 'keyword'
                            }
                        }
                    },
                    'status': {
                        'type': 'text',
                        'fields': {
                            'raw': {
                                'type': 'keyword'
                            }
                        }
                    },
                    'url': {
                        'type': 'text',
                        'fields': {
                            'raw': {
                                'type': 'keyword'
                            }
                        }
                    }
                }
            }
        }
    }
}


class CapAlertsRealtimeLoader(BaseLoader):
    """Cap Alerts real-time loader"""

    def __init__(self, plugin_def):
        """initializer"""

        BaseLoader.__init__(self)

        self.ES = get_es(MSC_PYGEOAPI_ES_URL, MSC_PYGEOAPI_ES_AUTH)

        if not self.ES.indices.exists(INDEX_NAME):
            self.ES.indices.create(index=INDEX_NAME, body=SETTINGS,
                                   request_timeout=MSC_PYGEOAPI_ES_TIMEOUT)

    def load_data(self, filepath):
        """
        fonction from base to load the data in ES

        :param filepath: filepath for parsing the current condition file

        :returns: True/False
        """

        data = self.weather_warning2geojson(filepath)

        try:
            bulk_data = []
            for doc in data:
                op_dict = {
                    'index': {
                        '_index': INDEX_NAME,
                        '_type': '_doc'
                    }
                }
                op_dict['index']['_id'] = doc['properties']['identifier']
                bulk_data.append(op_dict)
                bulk_data.append(doc)
            r = self.ES.bulk(index=INDEX_NAME, body=bulk_data)
            print(r)

            LOGGER.debug('Result: {}'.format(r))
            return True

        except Exception as err:
            LOGGER.warning('Error bulk indexing: {}'.format(err))
            return False

        #print(data)

        #try:
        #    r = self.ES.index(index=INDEX_NAME,
        #                      id=data['properties']['identifier'],
        #                      body=data)
        #    LOGGER.debug('Result: {}'.format(r))
        #    return True
        #except Exception as err:
        #    LOGGER.warning('Error indexing: {}'.format(err))
        #    return False

    def _get_date_format(self, date):
        """
        Convenience function to parse CAP dates

        :param date: date form the cap XML

        returns: date as datetime object
        """
        for char in ["T", "-", ":"]:
            if char in date:
                date = date.replace(char, '')
        date = date[:14]
        date = datetime.strptime(date, "%Y%m%d%H%M%S")

        return date

    def _get_element(self, node, path, attrib=None):
        """
        Convenience function to resolve lxml.etree.Element handling

        :param node: xml node
        :param path: path in the xml node
        :param attrib: attribute to get in the node

        returns: attribute as text or None
        """

        val = node.find(path)
        if attrib is not None and val is not None:
            return val.attrib.get(attrib)
        if hasattr(val, 'text') and val.text not in [None, '']:
            return val.text
        return None

    def weather_warning2geojson(self, filepath):
        """
        Create GeoJSON that will be use to display weather alerts

        :param filepath: filepath to the cap-xml file

        :returns: xml as json object
        """

        # we must define the variable that we'll need
        now = datetime.utcnow()

        french_alert = {}
        english_alert = {}
        english_alert_remove = []
        list_id = []

        timeformat = '%Y-%m-%dT%H:%M:%SZ'
        # we want to run a loop on every cap-xml in filepath and add them
        # in the geojson
        # we want to strat by the newest file in the directory
        LOGGER.info('Processing {} CAP documents'.format(len(filepath)))
    
        LOGGER.debug('Processing {}'.format(filepath))
        # with the lxml library we parse the xml file
        try:
            tree = etree.parse(filepath)
        except Exception as err:
            LOGGER.warning('Cannot parse {}: {}.  Skipping'.format(filepath, err))
    
        url = 'https://dd.weather.gc.ca/{}'.format(filepath)
        url = url.replace('{}{}'.format(GEOMET_WEATHER_BASEPATH, '/amqp/'), '')

        root = tree.getroot()

        base_xml = '{urn:oasis:names:tc:emergency:cap:1.2}'

        identifier = self._get_element(root,
                                  '{}identifier'.format(base_xml))
        references = self._get_element(root,
                                  '{}references'.format(base_xml))

        lastref = references.split(',')[-1]

        for grandchild in root.iter('{}info'.format(base_xml)):
            expires = self._get_date_format(self._get_element(grandchild,
                                                    '{}expires'.format(base_xml)))\
                      .strftime(timeformat)
    
            status_alert = self._get_element(grandchild,
                                        '{}parameter[last()-4]/'
                                        '{}value'.format(base_xml, base_xml))
    
            if self._get_date_format(expires) > now or identifier not in list_id:
                list_id.append(lastref)
                language = self._get_element(grandchild,
                                        '{}language'.format(base_xml))
                if language == 'fr-CA':
                    headline = self._get_element(grandchild,
                                            '{}headline'.format(base_xml))
                    descript = self._get_element(grandchild,
                                            '{}description'.format(base_xml))\
                        .replace("\n", " ").strip()
    
                    for i in grandchild.iter('{}area'.format(base_xml)):
                        tag = self._get_element(i, '{}polygon'.format(base_xml))
                        name = self._get_element(i, '{}areaDesc'.format(base_xml))
                        id_warning = 'a-{}'.format(re.sub('[-, .]', '', tag)[:25])
    
                        if id_warning not in french_alert:
                            french_alert[id_warning] = (id_warning,
                                                        name,
                                                        headline,
                                                        descript)
                else:
                    headline = self._get_element(grandchild,
                                            '{}headline'.format(base_xml))
                    descript = self._get_element(grandchild,
                                            '{}description'.format(base_xml))\
                        .replace("\n", " ").strip()
    
                    effective = self._get_date_format(self._get_element
                                                 (grandchild,
                                                  '{}effective'.format(base_xml)))\
                        .strftime(timeformat)
    
                    warning = self._get_element(grandchild,
                                           '{}parameter[1]/'
                                           '{}value'.format(base_xml,
                                                            base_xml))
    
                    # There can be many <area> cobvered by one
                    #  <info> so we have to loop through the info
                    for i in grandchild.iter('{}area'.format(base_xml)):
                        tag = self._get_element(i, '{}polygon'.format(base_xml))
                        name = self._get_element(i, '{}areaDesc'.format(base_xml))
    
                        split_tag = re.split(' |,', tag)
                        # We want to create a unique id for an area and one warning
                        # We use the warning ID + a unique code for the area
                        id_warning = 'a-{}'.format(re.sub('[-, .]', '', tag)[:25])
                        # We start by parsing the most recent file
                        # so we can say that if an unique ID is already in
                        # the dictionary don't add the warning from the oldest file
                        # The newest file is to announce the end of a warning or
                        # to update the warning
                        # So we never want to have twice the same id in the dict
                        if id_warning not in english_alert:
                            english_alert[id_warning] = (split_tag,
                                                         name,
                                                         headline,
                                                         effective,
                                                         expires,
                                                         warning,
                                                         status_alert,
                                                         id_warning,
                                                         descript,
                                                         url)

        LOGGER.info('Done processing')
        for j in english_alert:
            if self._get_date_format(english_alert[j][4]) < now:
                english_alert_remove.append(j)
                # We can't remove a element of a dictionary while looping in it
                # So we remove the warning in another step
        for key in english_alert_remove:
            del english_alert[key]
            del french_alert[key]
    
        # To keep going we want to have the same number of warning
        # in english and in french
        if len(french_alert) == len(english_alert):
            LOGGER.info('Creating %d features', len(english_alert))
    
            data = []
            for num_poly in english_alert:
                poly = []
                for l in list(reversed(range(0,
                                             len(english_alert[num_poly][0]),
                                             2))):
                    if len(english_alert[num_poly][0]) > 1:
                        poly.append([float(english_alert[num_poly][0][l + 1]),
                                     float(english_alert[num_poly][0][l]), 0.0])
    
                # for temporary care of the duplicate neighbors coordinate
                # poly = [k for k, g in groupby(poly)]
                no_dup_poly = []
                for k in poly:
                    if k not in no_dup_poly:
                        no_dup_poly.append(k)
                no_dup_poly.append(poly[-1])

                id_ = english_alert[num_poly][7]

                AlertLocation = {
                    'type': "Feature",
                    'properties': {
                        'identifier': id_,
                        'area': english_alert[num_poly][1],
                        'reference': identifier,
                        'zone': french_alert[num_poly][1],
                        'headline': english_alert[num_poly][2],
                        'titre': french_alert[num_poly][2],
                        'descrip_en': english_alert[num_poly][8],
                        'descrip_fr': french_alert[num_poly][3],
                        'effective': english_alert[num_poly][3],
                        'expires': english_alert[num_poly][4],
                        'alert_type': english_alert[num_poly][5],
                        'status': english_alert[num_poly][6],
                        'url': english_alert[num_poly][9]
                    },
                   'geometry': {
                       'type': "Polygon",
                       'coordinates': [no_dup_poly]
                    }
                }

                data.append(AlertLocation)
    
        return data


@click.group()
def cap_alerts():
    """Manages cap alerts index"""
    pass


@click.command()
@click.pass_context
@click.option('--days', '-d', default=DAYS_TO_KEEP, type=int,
              help='delete documents older than n days (default={})'.format(
                  DAYS_TO_KEEP))
@click.option('--yes', is_flag=True, callback=click_abort_if_false,
              expose_value=False,
              prompt='Are you sure you want to delete old documents?')
def clean_records(ctx, days):
    """Delete old documents"""

    es = get_es(MSC_PYGEOAPI_ES_URL, MSC_PYGEOAPI_ES_AUTH)

    older_than = (datetime.now() - timedelta(days=days)).strftime(
        '%Y-%m-%d %H:%M')
    click.echo('Deleting documents older than {} ({} days)'.format(
        older_than, days))

    query = {
        'query': {
            'range': {
                'properties.expires': {
                    'lte': older_than
                }
            }
        }
    }

    es.delete_by_query(index=INDEX_NAME, body=query)


@click.command()
@click.pass_context
@click.option('--yes', is_flag=True, callback=click_abort_if_false,
              expose_value=False,
              prompt='Are you sure you want to delete this index?')
def delete_index(ctx):
    """Delete current conditions index"""

    es = get_es(MSC_PYGEOAPI_ES_URL, MSC_PYGEOAPI_ES_AUTH)

    if es.indices.exists(INDEX_NAME):
        es.indices.delete(INDEX_NAME)


cap_alerts.add_command(clean_records)
cap_alerts.add_command(delete_index)
