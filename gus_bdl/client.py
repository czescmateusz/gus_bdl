"""
Client for getting data from GUS BDL
"""
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, Any, Dict

import requests
from requests import Response
import urllib
import json

AnyDict = Optional[Dict[str, Any]]

LOG = logging.getLogger(__name__)

@dataclass()
class Endpoints:
    """
    BDL endpoints
    """
    subjects = 'https://bdl.stat.gov.pl/api/v1/subjects?'
    _data = 'https://bdl.stat.gov.pl/api/v1/data/'

class AbstractClient(ABC):
    """
    Abstract GET client for dumping data from:

    For each endpoint, there is a child class with `get` method, that is
    updating or passing additional arguments to query.
    Refer to api documentation for details.

    - dump endpoint: https://www.saos.org.pl/api/dump
    returns full documents.
    - search endpoint: https://www.saos.org.pl/api/search
    returns truncated documents, but can be queried
    - single judgement endpoint: https://www.saos.org.pl/api/judgments/JUDGMENT_ID

    API documentation:
       https://www.saos.org.pl/help/index.php/dokumentacja-api/
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
        resp = requests.get(Endpoints.subjects, params, verify=False)
        json_normalize(resp['results']).drop('hasVariables', axis=1)
        return json_normalize(resp['results']).drop('hasVariables', axis=1)