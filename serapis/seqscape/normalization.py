'''
#################################################################################
#
# Copyright (c) 2013 Genome Research Ltd.
# 
# Author: Irina Colgiu <ic4@sanger.ac.uk>
# 
# This program is free software: you can redistribute it and/or modify it under
# the terms of the GNU General Public License as published by the Free Software
# Foundation; either version 3 of the License, or (at your option) any later
# version.
# 
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
# details.
# 
# You should have received a copy of the GNU General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
# 
#################################################################################

Created on Nov 7, 2014

@author: ic4
'''

import pycountry
from fuzzywuzzy import process, fuzz

HIGH_CUTOFF = 90
LOW_CUTOFF  = 70

HUMAN_CANONICAL_FORM = 'Homo Sapiens'
HUMAN_ORG_SYNONYMS = ['human', 'Human', 'Human (breast cancer)', ]


def normalize_human_organism(organism):
    ''' This function checks if the organism given is human, and if it is
        it maps it on the canonical form of the Homo Sapiens organism.
    '''
    if fuzz.ratio(organism, HUMAN_CANONICAL_FORM) >= LOW_CUTOFF:
        return HUMAN_CANONICAL_FORM
    best_match = process.extractOne(organism, HUMAN_ORG_SYNONYMS, score_cutoff=LOW_CUTOFF)
    if best_match:
        return HUMAN_CANONICAL_FORM
    return organism


def normalize_country(country):
    if process.extractBests(country, choices=['N/A', 'n/a'], score_cutoff=HIGH_CUTOFF):
        return None
    if country == 'null':
        return None
    if country in ['USA', 'UK']:
        return country
    if process.extractOne(country, choices=['United Kingdom', 'UK'], score_cutoff=HIGH_CUTOFF):
        return 'UK'
    if process.extractOne(country, choices=['England', 'Wales', 'Scotland'], score_cutoff=HIGH_CUTOFF):
        return 'UK'
    if process.extractOne(country, choices=['Russia', 'Russian Federation'], score_cutoff=LOW_CUTOFF):
        return 'Russia'
    try:
        country = pycountry.historic_countries.get(name=country.capitalize())
        return country.name
    except KeyError: pass
    
    try:
        country = pycountry.historic_countries.get(alpha2=country.upper())
        return country.name
    except KeyError: pass

    countries = [country.name for country in pycountry.countries]
    best_match = process.extractOne(country, choices=countries, score_cutoff=90)
    return best_match if best_match else country 






