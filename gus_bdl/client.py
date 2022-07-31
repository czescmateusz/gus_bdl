"""
Client for getting data from GUS BDL
"""
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, Any, Dict
from gevent import idle

import requests
from requests import Response
import urllib
import json

from pandas import json_normalize
from pandas import DataFrame

AnyDict = Optional[Dict[str, Any]]

LOG = logging.getLogger(__name__)

@dataclass()
class Endpoints:
    """
    BDL endpoints
    """
    _api = 'https://bdl.stat.gov.pl/api/v1'
    subjects = '%s/subjects?' % _api
    search_subjects = '%s/search?' % _api
    byvariable = '%s/data/by-variable/' % _api 
    localities = '%s/data/localities/by-variable/' % _api
    units = '%s/units?' % _api 
    aggregates = '%s/aggregates?' % _api
    measures = '%s/measures?' % _api
    levels = '%s/levels?' % _api
    variables = '%s/variables?' % _api
    years = '%s/years?' % _api


class AbstractClient(ABC):
    """
    Abstract GET client for dumping data
    """

    def __init__(self):
        self._params = {}

    @property
    def params(self):
        return self._params

    @params.setter
    def params(self, params: AnyDict):
        if params is not None:
            self._params.update(params)

    @abstractmethod
    def get(self, params: Optional[Dict[str, Any]] = None) -> Response:
        """
        :param params: optional, parameters to be used in API call.
        if not provided, query is using defaults, e.g. first page of results.
        :return: Response object
 
        """
        pass

    def __repr__(self):
        return str(self._params)

class SubjectListClient(AbstractClient):
    """
    Client for fetching data from BDL on subjects.
    """
    def get(self, params: Optional[Dict[str, Any]] = None) -> Response:
        resp = requests.get(Endpoints.subjects, params).json()
        json_normalize(resp['results']).drop('hasVariables', axis=1)
        return json_normalize(resp['results']).drop('hasVariables', axis=1)

class VariablesListClient(AbstractClient):
    """
    Client for fetching data from BDL on subjects.
    """
    def get(self, params: Optional[Dict[str, Any]] = None) -> Response:
        resp = requests.get(Endpoints.subjects).json()
        json_normalize(resp['results']).drop('hasVariables', axis=1)
        return json_normalize(resp['results']).drop('hasVariables', axis=1)

class DataClient(AbstractClient):
    """
    Client for fetching data from BDL. 
    """
    def get(self, varid, params: Optional[Dict[str, Any]] = None) -> Response:
        resp = requests.get(Endpoints.byvariable + varid + '?', params).json()
        results = json_normalize(resp['results'])
        valseries = results.set_index('name')['values'].explode()
        return DataFrame(valseries.tolist(), index=valseries.index).reset_index()
        