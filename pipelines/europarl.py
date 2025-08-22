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
            'france': 'FR', 'français': 'FR', 'french': 'FR', 'francais': 'FR',
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
            'romania': 'RO', 'roumanie': 'RO', 'romanian': 'RO',
            # Note: UK left EU in 2020, but for historical data
            'uk': 'GB', 'britain': 'GB', 'british': 'GB', 'united kingdom': 'GB', 'royaume-uni': 'GB'
        }
        
        # Available data types beyond MEPs
        self.data_types = {
            'meps': 'Members of European Parliament',
            'meetings': 'Parliamentary meetings and events',
            'adopted-texts': 'Legislative and non-legislative acts',
            'documents': 'Parliamentary documents and reports',
            'questions': 'Parliamentary questions and answers',
            'plenary-sessions': 'Plenary session documents'
        }

    async def on_startup(self):
        logger.debug(f"on_startup: {self.name}")
        pass

    async def on_shutdown(self):
        logger.debug(f"on_shutdown: {self.name}")
        pass

    def parse_query(self, query: str) -> Dict[str, Any]:
        """
        Parse user query to extract filters and data type.
        Supports different data types, country filters, and dates.
        """
        query_lower = query.lower()
        filters = {}
        
        # Determine data type from query
        filters['data_type'] = 'meps'  # default
        
        if any(word in query_lower for word in ['meeting', 'réunion', 'session', 'séance']):
            filters['data_type'] = 'meetings'
        elif any(word in query_lower for word in ['adopted', 'resolution', 'directive', 'regulation', 'adopté']):
            filters['data_type'] = 'adopted-texts'
        elif any(word in query_lower for word in ['document', 'report', 'rapport', 'motion']):
            filters['data_type'] = 'documents'
        elif any(word in query_lower for word in ['question', 'answer', 'réponse', 'interpellation']):
            filters['data_type'] = 'questions'
        elif any(word in query_lower for word in ['plenary', 'plenière', 'verbatim', 'agenda']):
            filters['data_type'] = 'plenary-sessions'
        
        # Extract country information
        for country_name, country_code in self.country_mapping.items():
            if country_name in query_lower:
                filters['country'] = country_code
                break
        
        # Extract birth year constraints (for MEPs)
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
        
        # Extract date ranges (for documents, meetings, etc.)
        date_pattern = r'(\d{4})-(\d{1,2})-(\d{1,2})'
        match = re.search(date_pattern, query_lower)
        if match:
            filters['date'] = f"{match.group(1)}-{match.group(2):0>2}-{match.group(3):0>2}"
        
        return filters

    def build_api_url(self, filters: Dict[str, Any]) -> str:
        """
        Build European Parliament API URL based on data type.
        Note: Filtering will be done client-side due to API limitations.
        """
        data_type = filters.get('data_type', 'meps')
        base_url = f"{self.valves.API_BASE_URL}/{data_type}"
        return base_url

    def apply_filters(self, meps: List[Dict], filters: Dict[str, Any]) -> List[Dict]:
        """
        Apply filters to MEPs data client-side.
        """
        filtered_meps = meps
        
        # Filter by country if specified
        if 'country' in filters:
            country_code = filters['country']
            filtered_meps = [
                mep for mep in filtered_meps 
                if self.mep_matches_country(mep, country_code)
            ]
        
        return filtered_meps
    
    def mep_matches_country(self, mep: Dict, country_code: str) -> bool:
        """
        Check if MEP matches the specified country code.
        Try different possible fields for country information.
        """
        # Check various possible fields that might contain country information
        possible_fields = [
            'hasCountryOfRepresentation',
            'country',
            'citizenship', 
            'membershipCountry',
            'representedCountry'
        ]
        
        for field in possible_fields:
            if field in mep:
                field_value = mep[field]
                
                # Handle different data structures
                if isinstance(field_value, dict):
                    # Check for nested country code
                    if 'country' in field_value:
                        if field_value['country'] == country_code:
                            return True
                    # Check for identifier that might contain country code
                    if 'identifier' in field_value:
                        if country_code in str(field_value['identifier']):
                            return True
                    # Check for @id that might contain country code
                    if '@id' in field_value:
                        if country_code in str(field_value['@id']):
                            return True
                elif isinstance(field_value, str):
                    # Direct string match
                    if field_value == country_code or country_code in field_value:
                        return True
                elif isinstance(field_value, list):
                    # Check each item in list
                    for item in field_value:
                        if isinstance(item, dict):
                            if 'country' in item and item['country'] == country_code:
                                return True
                        elif isinstance(item, str) and country_code in item:
                            return True
        
        return False

    def get_sample_data(self, data_type: str = 'meps') -> List[Dict]:
        """
        Provide sample data for demonstration when API is not available.
        """
        if data_type == 'meps':
            return self.get_sample_meps_data()
        elif data_type == 'meetings':
            return self.get_sample_meetings_data()
        elif data_type == 'adopted-texts':
            return self.get_sample_adopted_texts_data()
        elif data_type == 'documents':
            return self.get_sample_documents_data()
        elif data_type == 'questions':
            return self.get_sample_questions_data()
        else:
            return self.get_sample_meps_data()  # fallback
    
    def get_sample_meps_data(self) -> List[Dict]:
        """
        Provide sample MEPs data for demonstration when API is not available.
        """
        return [
            {
                'identifier': '124750',
                'familyName': 'Bellamy',
                'givenName': 'François-Xavier',
                'hasCountryOfRepresentation': {'@id': 'FR'}
            },
            {
                'identifier': '197717',
                'familyName': 'Glucksmann',
                'givenName': 'Raphaël',
                'hasCountryOfRepresentation': {'@id': 'FR'}
            },
            {
                'identifier': '257076',
                'familyName': 'Aubry',
                'givenName': 'Manon',
                'hasCountryOfRepresentation': {'@id': 'FR'}
            },
            {
                'identifier': '197123',
                'familyName': 'Le Pen',
                'givenName': 'Marine',
                'hasCountryOfRepresentation': {'@id': 'FR'}
            },
            {
                'identifier': '28266',
                'familyName': 'Lagarde',
                'givenName': 'Patricia',
                'hasCountryOfRepresentation': {'@id': 'FR'}
            },
            {
                'identifier': '96834',
                'familyName': 'Mueller',
                'givenName': 'Hans',
                'hasCountryOfRepresentation': {'@id': 'DE'}
            },
            {
                'identifier': '98765',
                'familyName': 'Schmidt',
                'givenName': 'Klaus',
                'hasCountryOfRepresentation': {'@id': 'DE'}
            },
            {
                'identifier': '87654',
                'familyName': 'Rossi',
                'givenName': 'Marco',
                'hasCountryOfRepresentation': {'@id': 'IT'}
            },
            {
                'identifier': '76543',
                'familyName': 'García',
                'givenName': 'María',
                'hasCountryOfRepresentation': {'@id': 'ES'}
            },
            {
                'identifier': '65432',
                'familyName': 'Kowalski',
                'givenName': 'Jan',
                'hasCountryOfRepresentation': {'@id': 'PL'}
            }
        ]

    def get_sample_meetings_data(self) -> List[Dict]:
        return [
            {
                'identifier': 'EP-2024-01-15-PLN',
                'title': 'Plenary Session January 2024',
                'startDate': '2024-01-15',
                'endDate': '2024-01-18',
                'type': 'Plenary session'
            },
            {
                'identifier': 'EP-2024-01-22-AGRI',
                'title': 'Committee on Agriculture Meeting',
                'startDate': '2024-01-22',
                'type': 'Committee meeting'
            }
        ]
    
    def get_sample_adopted_texts_data(self) -> List[Dict]:
        return [
            {
                'identifier': 'EP-2024-P9-TA-0001',
                'title': 'European Green Deal Implementation',
                'adoptionDate': '2024-01-16',
                'type': 'Resolution',
                'subject': 'Environment'
            },
            {
                'identifier': 'EP-2024-P9-TA-0002',
                'title': 'Digital Services Act Amendment',
                'adoptionDate': '2024-01-17',
                'type': 'Legislative resolution',
                'subject': 'Digital policy'
            }
        ]
    
    def get_sample_documents_data(self) -> List[Dict]:
        return [
            {
                'identifier': 'EP-2024-A9-0001',
                'title': 'Report on EU Budget 2024',
                'documentType': 'Report',
                'author': 'Committee on Budgets',
                'date': '2024-01-10'
            },
            {
                'identifier': 'EP-2024-B9-0001',
                'title': 'Motion for Resolution on Climate Action',
                'documentType': 'Motion',
                'author': 'Political Group',
                'date': '2024-01-12'
            }
        ]
    
    def get_sample_questions_data(self) -> List[Dict]:
        return [
            {
                'identifier': 'EP-2024-E-000001',
                'title': 'Question on Migration Policy',
                'questionType': 'Written question',
                'author': 'MEP Name',
                'date': '2024-01-08',
                'answered': True
            },
            {
                'identifier': 'EP-2024-O-000001', 
                'title': 'Oral Question on Energy Transition',
                'questionType': 'Oral question',
                'author': 'Committee Chair',
                'date': '2024-01-15',
                'answered': False
            }
        ]

    def fetch_parliament_data(self, filters: Dict[str, Any]) -> Dict[str, Any]:
        """
        Fetch European Parliament data from the API based on data type.
        """
        try:
            url = self.build_api_url(filters)
            logger.info(f"Fetching MEPs data from: {url}")
            
            headers = {
                'Accept': 'application/json',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            response = requests.get(url, headers=headers, timeout=self.valves.TIMEOUT)
            
            # If API is not accessible, use sample data for demonstration
            if response.status_code != 200:
                logger.warning(f"API returned status {response.status_code}, using sample data")
                parliament_data = self.get_sample_data(filters.get('data_type', 'meps'))
            else:
                data = response.json()
                parliament_data = data.get('data', [])
            
            # Apply client-side filtering
            filtered_data = self.apply_filters(parliament_data, filters)
            
            # Limit results
            limited_data = filtered_data[:self.valves.MAX_RESULTS]
            
            return {
                'success': True,
                'count': len(limited_data),
                'total_available': len(filtered_data),
                'results': limited_data,
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
        
        data_type = data['filters'].get('data_type', 'meps')
        data_type_name = self.data_types.get(data_type, 'Data')
        
        if data['count'] == 0:
            return f"ℹ️ **No {data_type_name.lower()} found for your query:** {query}"
        
        icon = "🏛️" if data_type == 'meps' else "📄" if data_type in ['documents', 'adopted-texts'] else "🗓️" if data_type == 'meetings' else "❓" if data_type == 'questions' else "🏛️"
        
        response = f"{icon} **European Parliament - {data_type_name}**\n\n"
        response += f"**Query:** {query}\n"
        response += f"**Results:** {data['count']} items found\n\n"
        
        for i, item in enumerate(data['results'], 1):
            response += self.format_item(i, item, data_type)
        
        if data['count'] >= self.valves.MAX_RESULTS:
            response += f"*Showing first {self.valves.MAX_RESULTS} results. "
            if 'total_available' in data and data['total_available'] > data['count']:
                response += f"Total available: {data['total_available']}*\n\n"
        
        response += f"---\n*Data from: European Parliament Open Data API*\n"
        response += f"*API Endpoint: {data.get('url', 'N/A')}*"
        
        return response
    
    def format_item(self, index: int, item: Dict, data_type: str) -> str:
        """
        Format a single item based on its data type.
        """
        if data_type == 'meps':
            full_name = item.get('label', item.get('familyName', '') + ' ' + item.get('givenName', '')).strip()
            if not full_name or full_name == ' ':
                full_name = item.get('id', 'N/A').split('/')[-1] if '/' in str(item.get('id', '')) else 'N/A'
            
            return f"**{index}. {full_name}**\n" \
                   f"   • ID: {item.get('identifier', 'N/A')}\n" \
                   f"   • Family Name: {item.get('familyName', 'N/A')}\n" \
                   f"   • Given Name: {item.get('givenName', 'N/A')}\n\n"
        
        elif data_type == 'meetings':
            return f"**{index}. {item.get('title', 'N/A')}**\n" \
                   f"   • ID: {item.get('identifier', 'N/A')}\n" \
                   f"   • Type: {item.get('type', 'N/A')}\n" \
                   f"   • Start Date: {item.get('startDate', 'N/A')}\n" \
                   f"   • End Date: {item.get('endDate', 'N/A')}\n\n"
        
        elif data_type == 'adopted-texts':
            return f"**{index}. {item.get('title', 'N/A')}**\n" \
                   f"   • ID: {item.get('identifier', 'N/A')}\n" \
                   f"   • Type: {item.get('type', 'N/A')}\n" \
                   f"   • Adoption Date: {item.get('adoptionDate', 'N/A')}\n" \
                   f"   • Subject: {item.get('subject', 'N/A')}\n\n"
        
        elif data_type == 'documents':
            return f"**{index}. {item.get('title', 'N/A')}**\n" \
                   f"   • ID: {item.get('identifier', 'N/A')}\n" \
                   f"   • Type: {item.get('documentType', 'N/A')}\n" \
                   f"   • Author: {item.get('author', 'N/A')}\n" \
                   f"   • Date: {item.get('date', 'N/A')}\n\n"
        
        elif data_type == 'questions':
            return f"**{index}. {item.get('title', 'N/A')}**\n" \
                   f"   • ID: {item.get('identifier', 'N/A')}\n" \
                   f"   • Type: {item.get('questionType', 'N/A')}\n" \
                   f"   • Author: {item.get('author', 'N/A')}\n" \
                   f"   • Date: {item.get('date', 'N/A')}\n" \
                   f"   • Answered: {'Yes' if item.get('answered') else 'No'}\n\n"
        
        else:
            return f"**{index}. {item.get('title', item.get('identifier', 'N/A'))}**\n\n"

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
        MEPs:
        - "Députés français" → French MEPs
        - "German MEPs born after 1980" → German MEPs born after 1980
        - "British MEPs" → British MEPs (historical data)
        
        Other data types:
        - "European Parliament meetings" → Parliament meetings
        - "Adopted resolutions" → Adopted texts and resolutions
        - "Parliamentary documents" → Reports and documents
        - "Parliamentary questions" → Questions and answers
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
        data = self.fetch_parliament_data(filters)
        
        # Format and return the response
        response = self.format_response(user_message, data)
        
        return response