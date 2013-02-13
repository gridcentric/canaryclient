# Copyright 2013 Gridcentric Inc.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

from novaclient import utils
from novaclient import base
from novaclient.v1_1 import hosts
from novaclient import utils

import collections

def __pre_parse_args__():
    pass

def __post_parse_args__(args):
    pass

CanaryDataPoint = collections.namedtuple("CanaryDataPoint", "host_name metric timestamp cf value")
CanaryHostInfo = collections.namedtuple("CanaryHostInfo", "host_name")
CanaryMetricInfo = collections.namedtuple("CanaryMetricInfo", "host_name metric from_time to_time cfs resolutions")

@utils.arg('host', metavar='<host>', help='ID or name of the host')
@utils.arg('metric', metavar='<metric>', default=None, help='The metric to query')
@utils.arg('--from_time', metavar='<from_time>', default=None, help='The start time')
@utils.arg('--to_time', metavar='<to_time>', default=None, help='The end time')
@utils.arg('--cf', metavar='<consolidation_function>', default='AVERAGE',
        help='Consolidation function, generally one of: AVERAGE,MIN,MAX')
@utils.arg('--resolution', metavar='<resolution>', default=None, help='The measurement resolution')
def do_canary_query(cs, args):
    """ Query a Canary host for monitoring stats. """
    kwargs = \
    {
        "from_time" : args.from_time,
        "to_time" : args.to_time,
        "cf" : args.cf,
        "resolution" : args.resolution
    }
    utils.print_list(cs.canary.query(args.host, args.metric, **kwargs),
        ["host_name", "metric", "timestamp", "cf", "value"])

@utils.arg('host', metavar='<host>', help='ID or name of the host')
def do_canary_info(cs, args):
    """ Show available stats for a Canary host. """
    utils.print_list(cs.canary.info(args.host), 
        ["host_name", "metric", "from_time", "to_time", "cfs", "resolutions"])

def do_canary_list(cs, args):
    """ Show available canary hosts. """
    utils.print_list(cs.canary.list(), ["host_name"])

class CanaryHost(hosts.Host):

    def canary_query(self, **kwargs):
        return self.manager.query(self.host, **kwargs)

    def canary_query(self, **kwargs):
        return self.manager.query(self.host, **kwargs)

class CanaryHostManager(hosts.HostManager):
    resource_class = CanaryHost

    def __init__(self, client, *args, **kwargs):
        hosts.HostManager.__init__(self, client, *args, **kwargs)

        # Make sure this instance is available as canary.
        if not(hasattr(client, 'canary')):
            setattr(client, 'canary', self)

    def query(self, host, metric, cf='AVERAGE', from_time=None, to_time=None, resolution=None):
        args = \
        {
            "metric" : metric,
            "from_time" : from_time,
            "to_time" : to_time,
            "cf" : cf,
            "resolution" : resolution
        }
        body = { "args" : args }
        url = '/canary/%s/query' % base.getid(host)
        res = self.api.client.post(url, body=body)[1]
        return map(lambda v: CanaryDataPoint(host, metric, v[0], cf, v[1]), res)

    def list(self):
        url = '/canary'
        res = self.api.client.get(url)[1]
        return map(CanaryHostInfo, res)

    def info(self, host):
        url = '/canary/%s/info' % base.getid(host)
        res = self.api.client.get(url)[1]
        return map(lambda (k,v): CanaryMetricInfo(host,
                    k,
                    v.get("from_time"),
                    v.get("to_time"),
                    v.get("cfs"),
                    v.get("resolutions")), res.items())
