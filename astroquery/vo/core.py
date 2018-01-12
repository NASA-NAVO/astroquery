# Licensed under a 3-clause BSD style license - see LICENSE.rst
"""
MAST Portal
===========

Module to query the Barbara A. Mikulski Archive for Space Telescopes (MAST).

"""

from __future__ import print_function, division

import warnings
import json
import time
import os
import re
import keyring
import io

import numpy as np

from requests import HTTPError
from getpass import getpass
from base64 import b64encode

import astropy.units as u
import astropy.coordinates as coord

from astropy.table import Table, Row, vstack, MaskedColumn
from astropy.extern.six.moves.urllib.parse import quote as urlencode
from astropy.extern.six.moves.http_cookiejar import Cookie
from astropy.utils.exceptions import AstropyWarning
from astropy.logger import log

from ..query import BaseQuery
from ..utils import commons, async_to_sync
from ..utils.class_or_instance import class_or_instance
from ..exceptions import (TimeoutError, InvalidQueryError, RemoteServiceError,
                          LoginError, ResolverError, MaxResultsWarning,
                          NoResultsWarning, InputWarning, AuthenticationWarning)
from . import conf


__all__ = ['Registry', 'RegistryClass']


def _prepare_service_request_string(json_obj):
    """
    Takes a mashup JSON request object and turns it into a url-safe string.

    Parameters
    ----------
    json_obj : dict
        A Mashup request JSON object (python dictionary).

    Returns
    -------
    response : str
        URL encoded Mashup Request string.
    """
    requestString = json.dumps(json_obj)
    requestString = urlencode(requestString)
    return "request="+requestString



class RegistryClass(BaseQuery):
    """
    Registry query class.
"""


    def __init__(self):

        super(RegistryClass, self).__init__()

        self._REGISTRY_TAP_SYNC_URL = conf.registry_tap_url + "/sync"


    def query(self, **kwargs):
        
        
        adql = self._build_adql(**kwargs)
        x = """
            select b.waveband,b.short_name,a.ivoid,b.res_description,c.access_url,b.reference_url from rr.capability a 
            natural join rr.resource b 
            natural join rr.interface c
            where a.cap_type='SimpleImageAccess' and a.ivoid like 'ivo://%stsci%' 
            order by short_name
        """
        print ('ADQL = ', adql)
        
        method = 'POST'
        url = self._REGISTRY_TAP_SYNC_URL
        
        tap_params = {
            "request": "doQuery",
            "lang": "ADQL",
            "query": adql
        }
        
        response = self._request(method, url, data=tap_params)
        
        #print('Queried: ' + response.url)
        
        aptable = self._astropy_table_from_votable_response(response)
        
        return aptable
    
    def _build_adql(self, **kwargs):
        
        # Default values
        service_type=""
        keyword=""
        waveband=""
        source=""
        order_by=""
        logic_string=" and "
        
        # Find the keywords we recognize
        for key,val in kwargs.items():
            if (key == 'service_type'):
                service_type = val
            elif (key == 'keyword'):
                keyword = val
            elif (key == 'waveband'):
                waveband = val
            elif (key == 'source'):
                source = val
            elif (key == 'order_by'):
                order_by = val
            elif (key == 'logic_string'):
                logic_string = val
        
        ##
        if "image" in service_type.lower():
           service_type="simpleimageaccess"
        elif "spectr" in service_type.lower():
           service_type="simplespectralaccess"
        elif "cone" in service_type.lower():
           service_type="simpleconesearch"
        
    
        query_retcols="""
          select res.short_name,cap.ivoid
           from rr.capability cap
           natural join rr.resource res
           """
        
        query_where="where "
        
        wheres=[]
        if service_type is not "":
            wheres.append("cap.cap_type='{}'".format(service_type))
        if source is not "":
            wheres.append("cap.ivoid like '%{}%'".format(source))
        
        query_where=query_where+logic_string.join(wheres)
        
        if order_by is not "":
            query_order="order by {}".format(order_by)
        else: query_order=""
        
        query=query_retcols+query_where+query_order
        
        return query

    
    def _astropy_table_from_votable_response(self, response):
        """
        Takes a VOTABLE response from a web service and returns an astropy table.
        
        Parameters
        ----------
        response : requests.Response
            Response whose contents are assumed to be a VOTABLE.
            
        Returns
        -------
        astropy.table.Table
            Astropy Table containing the data from the first TABLE in the VOTABLE.
        """
        
        # The astropy table reader would like a file-like object, so convert
        # the response content a byte stream.  This assumes Python 3.x.
        # 
        # (The reader also accepts just a string, but that seems to have two 
        # problems:  It looks for newlines to see if the string is itself a table,
        # and we need to support unicode content.)
        file_like_content = io.BytesIO(response.content)
        
        # The astropy table reader will auto-detect that the content is a VOTABLE
        # and parse it appropriately.
        aptable = Table.read(file_like_content)
        
        # String values in the VOTABLE are stored in the astropy Table as bytes instead 
        # of strings.  To makes accessing them more convenient, we will convert all those
        # bytes values to strings.
        ### TSD stringify_table(aptable)
        
        return aptable



Registry = RegistryClass()
