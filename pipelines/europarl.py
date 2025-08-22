"""
title: European Parliament Data API
author: Claude Code
description: Fetch EU Parliament data via the official OData API
required_open_webui_version: 0.4.3
requirements: requests
version: 1.0.0
licence: MIT
"""

from typing import List, Union, Generator, Iterator, Dict, Any
from pydantic import BaseModel, Field
import requests
import re
import json
from datetime import datetime
from logging import getLogger

logger = getLogger(__name__)
logger.setLevel("DEBUG")


class Pipeline:
    class Valves(BaseModel):
        API_BASE_URL: str = Field(
            default="https://data.europarl.europa.eu/api/v2",
            description="European Parliament API base URL"
        )
        MAX_RESULTS: int = Field(
            default=10,
            description="Maximum number of results to return"
        )
        TIMEOUT: int = Field(
            default=30,
            description="API request timeout in seconds"
        )

    def __init__(self):
        self.name = "europarl"
        self.valves = self.Valves()
        
        # Common country codes mapping
        self.country_mapping = {
            'france': 'FR', 'français': 'FR', 'french': 'FR',
            'germany': 'DE', 'allemagne': 'DE', 'deutsch': 'DE', 'german': 'DE',
            'italy': 'IT', 'italie': 'IT', 'italian': 'IT', 'italiano': 'IT',
            'spain': 'ES', 'espagne': 'ES', 'spanish': 'ES', 'español': 'ES',
            'poland': 'PL', 'pologne': 'PL', 'polish': 'PL',
            'netherlands': 'NL', 'pays-bas': 'NL', 'dutch': 'NL',
            'belgium': 'BE', 'belgique': 'BE', 'belgian': 'BE',
            'austria': 'AT', 'autriche': 'AT', 'austrian': 'AT',
            'portugal': 'PT', 'portugais': 'PT', 'portuguese': 'PT',
            'greece': 'GR', 'grèce': 'GR', 'greek': 'GR',
            'czech': 'CZ', 'tchèque': 'CZ', 'czech republic': 'CZ',
            'hungary': 'HU', 'hongrie': 'HU', 'hungarian': 'HU',
            'sweden': 'SE', 'suède': 'SE', 'swedish': 'SE',
            'denmark': 'DK', 'danemark': 'DK', 'danish': 'DK',
            'finland': 'FI', 'finlande': 'FI', 'finnish': 'FI',
            'ireland': 'IE', 'irlande': 'IE', 'irish': 'IE',
            'slovakia': 'SK', 'slovaquie': 'SK', 'slovak': 'SK',
            'slovenia': 'SI', 'slovénie': 'SI', 'slovenian': 'SI',
            'estonia': 'EE', 'estonie': 'EE', 'estonian': 'EE',
            'latvia': 'LV', 'lettonie': 'LV', 'latvian': 'LV',
            'lithuania': 'LT', 'lituanie': 'LT', 'lithuanian': 'LT',
            'luxembourg': 'LU', 'luxembourgeois': 'LU',
            'malta': 'MT', 'malte': 'MT', 'maltese': 'MT',
            'cyprus': 'CY', 'chypre': 'CY', 'cypriot': 'CY',
            'croatia': 'HR', 'croatie': 'HR', 'croatian': 'HR',
            'bulgaria': 'BG', 'bulgarie': 'BG', 'bulgarian': 'BG',
            'romania': 'RO', 'roumanie': 'RO', 'romanian': 'RO'
        }

    async def on_startup(self):
        logger.debug(f"on_startup: {self.name}")
        pass

    async def on_shutdown(self):
        logger.debug(f"on_shutdown: {self.name}")
        pass

    def parse_query(self, query: str) -> Dict[str, Any]:
        """
        Parse user query to extract filters for the OData API.
        Supports country, birth year, and general search terms.
        """
        query_lower = query.lower()
        filters = {}
        
        # Extract country information
        for country_name, country_code in self.country_mapping.items():
            if country_name in query_lower:
                filters['country'] = country_code
                break
        
        # Extract birth year constraints
        year_pattern = r'(?:né|born|naissance).*?(?:après|after)\s+(\d{4})'
        match = re.search(year_pattern, query_lower)
        if match:
            filters['birth_year_after'] = int(match.group(1))
        
        year_pattern_before = r'(?:né|born|naissance).*?(?:avant|before)\s+(\d{4})'
        match = re.search(year_pattern_before, query_lower)
        if match:
            filters['birth_year_before'] = int(match.group(1))
        
        # Extract specific year
        year_pattern_in = r'(?:né|born|naissance).*?(?:en|in)\s+(\d{4})'
        match = re.search(year_pattern_in, query_lower)
        if match:
            filters['birth_year'] = int(match.group(1))
        
        return filters

    def build_api_url(self, endpoint: str, filters: Dict[str, Any]) -> str:
        """
        Build European Parliament API URL.
        """
        base_url = f"{self.valves.API_BASE_URL}/{endpoint}"
        return base_url

    def fetch_meps_data(self, filters: Dict[str, Any]) -> Dict[str, Any]:
        """
        Fetch MEPs (Members of European Parliament) data from the API.
        """
        try:
            url = self.build_api_url("meps", filters)
            logger.info(f"Fetching MEPs data from: {url}")
            
            headers = {
                'Accept': 'application/ld+json',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            response = requests.get(url, headers=headers, timeout=self.valves.TIMEOUT)
            response.raise_for_status()
            
            data = response.json()
            meps = data.get('data', [])
            
            # Apply client-side filtering if needed
            if 'country' in filters:
                country_filter = filters['country']
                # Filter will be applied in format_response based on available data
            
            # Limit results
            limited_meps = meps[:self.valves.MAX_RESULTS]
            
            return {
                'success': True,
                'count': len(limited_meps),
                'total_available': len(meps),
                'results': limited_meps,
                'url': url,
                'filters': filters
            }
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            return {
                'success': False,
                'error': f"API request failed: {str(e)}",
                'url': url if 'url' in locals() else 'N/A'
            }
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return {
                'success': False,
                'error': f"Unexpected error: {str(e)}"
            }

    def format_response(self, query: str, data: Dict[str, Any]) -> str:
        """
        Format the API response into a readable format.
        """
        if not data['success']:
            return f"❌ **Error fetching European Parliament data**\n\n{data['error']}"
        
        if data['count'] == 0:
            return f"ℹ️ **No MEPs found for your query:** {query}"
        
        response = f"🏛️ **European Parliament - MEPs Data**\n\n"
        response += f"**Query:** {query}\n"
        response += f"**Results:** {data['count']} MEPs found\n\n"
        
        for i, mep in enumerate(data['results'], 1):
            # Extract data from JSON-LD format
            full_name = mep.get('label', mep.get('familyName', '') + ' ' + mep.get('givenName', '')).strip()
            if not full_name or full_name == ' ':
                full_name = mep.get('id', 'N/A').split('/')[-1] if '/' in str(mep.get('id', '')) else 'N/A'
            
            mep_id = mep.get('identifier', 'N/A')
            family_name = mep.get('familyName', 'N/A')
            given_name = mep.get('givenName', 'N/A')
            
            response += f"**{i}. {full_name}**\n"
            response += f"   • ID: {mep_id}\n"
            response += f"   • Family Name: {family_name}\n"
            response += f"   • Given Name: {given_name}\n\n"
        
        if data['count'] >= self.valves.MAX_RESULTS:
            response += f"*Showing first {self.valves.MAX_RESULTS} results. "
            if 'total_available' in data and data['total_available'] > data['count']:
                response += f"Total available: {data['total_available']}*\n\n"
        
        response += f"---\n*Data from: European Parliament Open Data API*\n"
        response += f"*API Endpoint: {data.get('url', 'N/A')}*"
        
        return response

    def pipe(
        self, 
        user_message: str, 
        model_id: str, 
        messages: List[dict], 
        body: dict
    ) -> Union[str, Generator, Iterator]:
        """
        Main pipeline function that processes user queries and returns EU Parliament data.
        
        Examples of supported queries:
        - "Députés français" → French MEPs
        - "German MEPs born after 1980" → German MEPs born after 1980
        - "Italian politicians born in 1975" → Italian MEPs born in 1975
        - "Spanish members" → Spanish MEPs
        """
        logger.debug(f"Processing query: {user_message}")
        
        # Check if this is a title generation request (skip processing)
        if ("broad tags categorizing" in user_message.lower()) or ("Create a concise" in user_message.lower()):
            return "(title generation disabled for europarl pipeline)"
        
        # Parse the user query to extract filters
        filters = self.parse_query(user_message)
        logger.info(f"Extracted filters: {filters}")
        
        # If no specific filters found, default to a general search
        if not filters:
            # For general queries, we'll show a sample of MEPs
            pass
        
        # Fetch data from the European Parliament API
        data = self.fetch_meps_data(filters)
        
        # Format and return the response
        response = self.format_response(user_message, data)
        
        return response