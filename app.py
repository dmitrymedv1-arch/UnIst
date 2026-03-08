import streamlit as st
import pandas as pd
import numpy as np
import requests
import time
import hashlib
import pickle
import os
import re
import sys
import logging
from datetime import datetime
from dateutil.parser import parse as date_parse
from unidecode import unidecode
from rapidfuzz import fuzz, process
from collections import defaultdict, Counter
import concurrent.futures
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import networkx as nx
import matplotlib.pyplot as plt
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import io
import base64
from typing import Optional, Dict, List, Tuple, Any
import random

# =============================================================================
# НАУЧНЫЙ СТИЛЬ ГРАФИКОВ
# =============================================================================

plt.rcParams.update({
    # Font sizes and weights
    'font.size': 10,
    'font.family': 'serif',
    'axes.labelsize': 11,
    'axes.labelweight': 'bold',
    'axes.titlesize': 12,
    'axes.titleweight': 'bold',
    
    # Axes appearance
    'axes.facecolor': 'white',
    'axes.edgecolor': 'black',
    'axes.linewidth': 1.0,
    'axes.grid': False,
    
    # Tick parameters
    'xtick.color': 'black',
    'ytick.color': 'black',
    'xtick.labelsize': 10,
    'ytick.labelsize': 10,
    'xtick.direction': 'out',
    'ytick.direction': 'out',
    'xtick.major.size': 4,
    'xtick.minor.size': 2,
    'ytick.major.size': 4,
    'ytick.minor.size': 2,
    'xtick.major.width': 0.8,
    'ytick.major.width': 0.8,
    
    # Legend
    'legend.fontsize': 10,
    'legend.frameon': True,
    'legend.framealpha': 0.9,
    'legend.edgecolor': 'black',
    'legend.fancybox': False,
    
    # Figure
    'figure.dpi': 600,
    'savefig.dpi': 600,
    'savefig.bbox': 'tight',
    'savefig.pad_inches': 0.1,
    'figure.facecolor': 'white',
    
    # Lines
    'lines.linewidth': 1.5,
    'lines.markersize': 6,
    'errorbar.capsize': 3,
})

# =============================================================================
# ЦВЕТОВЫЕ ПАЛИТРЫ (10 вариантов)
# =============================================================================

COLOR_PALETTES = {
    "Научная классика": ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd", "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf"],
    "Современная минималистичная": ["#4C72B0", "#DD8452", "#55A868", "#C44E52", "#8172B2", "#937860", "#DA8BC3", "#8C8C8C", "#CCB974", "#64B5CD"],
    "Теплая земля": ["#BF5B17", "#F46D43", "#FDAE61", "#FEE08B", "#FFFFBF", "#D9EF8B", "#A6D96A", "#66BD63", "#1A9850", "#006837"],
    "Океаническая": ["#053061", "#2166AC", "#4393C3", "#92C5DE", "#D1E5F0", "#F7F7F7", "#FDDBC7", "#F4A582", "#D6604D", "#B2182B"],
    "Винтажная": ["#A50F15", "#DE2D26", "#FB6A4A", "#FC9272", "#FCBBA1", "#FEE5D9", "#EFF3FF", "#BDD7E7", "#6BAED6", "#2171B5"],
    "Неоновая": ["#FF00FF", "#00FF00", "#FF6600", "#00CCFF", "#FF3366", "#CC33FF", "#FFFF00", "#FF9933", "#33FF99", "#9966FF"],
    "Пастельная": ["#FBB4AE", "#B3CDE3", "#CCEBC5", "#DECBE4", "#FED9A6", "#FFFFCC", "#E5D8BD", "#FDDAEC", "#F2F2F2", "#B3E2CD"],
    "Монохромная синяя": ["#08306B", "#08519C", "#2171B5", "#4292C6", "#6BAED6", "#9ECAE1", "#C6DBEF", "#DEEBF7", "#F7FBFF", "#E3F2FD"],
    "Закат": ["#67001F", "#B2182B", "#D6604D", "#F4A582", "#FDDBC7", "#F7F7F7", "#D1E5F0", "#92C5DE", "#4393C3", "#2166AC"],
    "Лесная": ["#00441B", "#006D2C", "#238B45", "#41AB5D", "#74C476", "#A1D99B", "#C7E9C0", "#E5F5E0", "#F7FCF5", "#EDF8E9"]
}

# Функция для получения случайной палитры
def get_random_palette():
    return random.choice(list(COLOR_PALETTES.values()))

# Функция для получения палитры по имени
def get_palette_by_name(name):
    return COLOR_PALETTES.get(name, COLOR_PALETTES["Научная классика"])

# =============================================================================
# КОНФИГУРАЦИЯ
# =============================================================================

CACHE_DIR = "cache"
LOG_DIR = "logs"
CROSSREF_EMAIL = "analysis@unist.org"  # Replace with your email
MAX_WORKERS = 7
RATE_LIMIT_DELAY = 0.7
MAX_RETRIES = 5

# Create directories
os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# Logging setup
log_filename = f"{LOG_DIR}/analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# =============================================================================
# КЭШИРОВАНИЕ ISSN
# =============================================================================

class ISSNCache:
    """Cache for ISSN normalization and matching results"""
    
    def __init__(self, cache_dir=CACHE_DIR):
        self.cache_dir = cache_dir
        self.normalization_cache = {}
        self.if_matches_cache = {}
        self.cs_matches_cache = {}
        
    def normalize_issn(self, issn_str: str) -> str:
        """
        Normalize ISSN to format XXXX-XXXX
        Examples:
        24510769 -> 2451-0769
        5912385 -> 0591-2385 (add leading zero)
        445401 -> 0044-5401 (add two leading zeros)
        """
        if not issn_str or pd.isna(issn_str):
            return ""
        
        # Check cache
        cache_key = str(issn_str)
        if cache_key in self.normalization_cache:
            return self.normalization_cache[cache_key]
        
        # Convert to string and remove any non-digit characters
        issn_clean = re.sub(r'[^0-9]', '', str(issn_str))
        
        # Handle different lengths
        if len(issn_clean) == 8:
            # Already 8 digits, just add hyphen
            normalized = f"{issn_clean[:4]}-{issn_clean[4:]}"
        elif len(issn_clean) == 7:
            # 7 digits - add leading zero
            normalized = f"0{issn_clean[:3]}-{issn_clean[3:]}"
        elif len(issn_clean) == 6:
            # 6 digits - add two leading zeros
            normalized = f"00{issn_clean[:2]}-{issn_clean[2:]}"
        else:
            # If can't normalize, return original
            normalized = issn_str
        
        self.normalization_cache[cache_key] = normalized
        return normalized
    
    def load_if_data(self) -> pd.DataFrame:
        """Load and preprocess IF.xlsx"""
        if not os.path.exists("IF.xlsx"):
            return pd.DataFrame()
        
        df = pd.read_excel("IF.xlsx")
        
        # Normalize ISSN columns
        if 'ISSN' in df.columns:
            df['ISSN_norm'] = df['ISSN'].apply(self.normalize_issn)
        if 'eISSN' in df.columns:
            df['eISSN_norm'] = df['eISSN'].apply(self.normalize_issn)
        
        return df
    
    def load_cs_data(self) -> pd.DataFrame:
        """Load and preprocess CS.xlsx"""
        if not os.path.exists("CS.xlsx"):
            return pd.DataFrame()
        
        df = pd.read_excel("CS.xlsx")
        
        # Normalize ISSN columns
        if 'Print ISSN' in df.columns:
            df['Print_ISSN_norm'] = df['Print ISSN'].apply(self.normalize_issn)
        if 'E-ISSN' in df.columns:
            df['E-ISSN_norm'] = df['E-ISSN'].apply(self.normalize_issn)
        
        # Group by ISSN and take highest quartile for each journal
        result_rows = []
        
        # Process by Title to handle multiple entries
        if 'Title' in df.columns:
            for title, group in df.groupby('Title'):
                # Find the highest quartile (1 is highest, 4 is lowest)
                if 'Quartile' in group.columns:
                    quartiles = group['Quartile'].dropna()
                    if not quartiles.empty:
                        # Convert to numeric, handle string format like 'Q1', 'Q2', etc.
                        quartile_nums = []
                        for q in quartiles:
                            if pd.isna(q):
                                continue
                            if isinstance(q, str) and q.startswith('Q'):
                                try:
                                    quartile_nums.append(int(q[1:]))
                                except:
                                    pass
                            elif isinstance(q, (int, float)):
                                quartile_nums.append(int(q))
                        
                        if quartile_nums:
                            best_quartile_num = min(quartile_nums)  # 1 is highest
                            best_quartile = f"Q{best_quartile_num}"
                            
                            # Get the corresponding row
                            best_row = group[group['Quartile'] == best_quartile].iloc[0] if best_quartile in group['Quartile'].values else group.iloc[0]
                            result_rows.append(best_row)
                        else:
                            result_rows.append(group.iloc[0])
                    else:
                        result_rows.append(group.iloc[0])
                else:
                    result_rows.append(group.iloc[0])
        else:
            # If no Title column, just take unique ISSN combinations with highest quartile
            seen = set()
            for _, row in df.iterrows():
                issn_key = f"{row.get('Print ISSN', '')}_{row.get('E-ISSN', '')}"
                if issn_key not in seen:
                    seen.add(issn_key)
                    result_rows.append(row)
        
        result_df = pd.DataFrame(result_rows)
        return result_df
    
    def find_if_match(self, issn: str) -> Tuple[Optional[float], Optional[str]]:
        """
        Find IF and Quartile for given ISSN
        Returns (IF, Quartile)
        """
        if not issn:
            return None, None
        
        # Check cache
        if issn in self.if_matches_cache:
            return self.if_matches_cache[issn]
        
        # Load IF data
        if_df = self.load_if_data()
        if if_df.empty:
            return None, None
        
        # Normalize input ISSN
        issn_norm = self.normalize_issn(issn)
        
        # Search in ISSN_norm and eISSN_norm
        match = None
        if 'ISSN_norm' in if_df.columns:
            mask = (if_df['ISSN_norm'] == issn_norm)
            if mask.any():
                match = if_df[mask].iloc[0]
        
        if match is None and 'eISSN_norm' in if_df.columns:
            mask = (if_df['eISSN_norm'] == issn_norm)
            if mask.any():
                match = if_df[mask].iloc[0]
        
        if match is not None:
            result = (match.get('IF'), match.get('Quartile'))
        else:
            result = (None, None)
        
        self.if_matches_cache[issn] = result
        return result
    
    def find_cs_match(self, issn: str) -> Tuple[Optional[float], Optional[str]]:
        """
        Find CiteScore and Quartile for given ISSN
        Returns (CiteScore, Quartile)
        """
        if not issn:
            return None, None
        
        # Check cache
        if issn in self.cs_matches_cache:
            return self.cs_matches_cache[issn]
        
        # Load CS data
        cs_df = self.load_cs_data()
        if cs_df.empty:
            return None, None
        
        # Normalize input ISSN
        issn_norm = self.normalize_issn(issn)
        
        # Search in Print_ISSN_norm and E-ISSN_norm
        match = None
        if 'Print_ISSN_norm' in cs_df.columns:
            mask = (cs_df['Print_ISSN_norm'] == issn_norm)
            if mask.any():
                match = cs_df[mask].iloc[0]
        
        if match is None and 'E-ISSN_norm' in cs_df.columns:
            mask = (cs_df['E-ISSN_norm'] == issn_norm)
            if mask.any():
                match = cs_df[mask].iloc[0]
        
        if match is not None:
            result = (match.get('CiteScore'), match.get('Quartile'))
        else:
            result = (None, None)
        
        self.cs_matches_cache[issn] = result
        return result

# Initialize ISSN cache
issn_cache = ISSNCache()

# =============================================================================
# СТИЛИЗАЦИЯ STREAMLIT
# =============================================================================

def set_page_config():
    st.set_page_config(
        page_title="UnIst Analytics",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded"
    )

def load_custom_css():
    st.markdown("""
    <style>
    /* Main container styling */
    .main {
        padding: 0rem 1rem;
    }
    
    /* Title styling */
    .title-container {
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        padding: 2rem;
        border-radius: 10px;
        margin-bottom: 2rem;
        color: white;
        text-align: center;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }
    
    /* Logo styling */
    .logo-container {
        display: flex;
        justify-content: center;
        align-items: center;
        margin-bottom: 1rem;
    }
    
    /* Card styling */
    .card {
        background: white;
        padding: 1.5rem;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
        margin-bottom: 1rem;
        transition: transform 0.3s ease;
    }
    
    .card:hover {
        transform: translateY(-5px);
        box-shadow: 0 4px 8px rgba(0, 0, 0, 0.15);
    }
    
    /* Metric styling */
    .metric-container {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1rem;
        border-radius: 10px;
        color: white;
        text-align: center;
    }
    
    .metric-value {
        font-size: 2rem;
        font-weight: bold;
    }
    
    .metric-label {
        font-size: 0.9rem;
        opacity: 0.9;
    }
    
    /* Button styling */
    .stButton > button {
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        color: white;
        border: none;
        padding: 0.5rem 2rem;
        border-radius: 25px;
        font-weight: bold;
        transition: all 0.3s ease;
    }
    
    .stButton > button:hover {
        transform: scale(1.05);
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.2);
    }
    
    /* Progress bar styling */
    .stProgress > div > div {
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
    }
    
    /* Tab styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 2rem;
    }
    
    .stTabs [data-baseweb="tab"] {
        background: white;
        border-radius: 25px;
        padding: 0.5rem 1.5rem;
        transition: all 0.3s ease;
    }
    
    .stTabs [data-baseweb="tab"]:hover {
        background: #667eea;
        color: white;
    }
    
    /* Expander styling */
    .streamlit-expanderHeader {
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        color: white;
        border-radius: 10px;
    }
    
    /* Sidebar styling */
    .css-1d391kg {
        background: linear-gradient(180deg, #667eea 0%, #764ba2 100%);
    }
    
    /* Info box styling */
    .info-box {
        background: #e3f2fd;
        padding: 1rem;
        border-radius: 10px;
        border-left: 4px solid #2196f3;
        margin: 1rem 0;
    }
    
    .success-box {
        background: #d4edda;
        padding: 1rem;
        border-radius: 10px;
        border-left: 4px solid #28a745;
        margin: 1rem 0;
    }
    
    .warning-box {
        background: #fff3cd;
        padding: 1rem;
        border-radius: 10px;
        border-left: 4px solid #ffc107;
        margin: 1rem 0;
    }
    
    .error-box {
        background: #f8d7da;
        padding: 1rem;
        border-radius: 10px;
        border-left: 4px solid #dc3545;
        margin: 1rem 0;
    }
    
    /* Animation */
    @keyframes fadeIn {
        from { opacity: 0; transform: translateY(20px); }
        to { opacity: 1; transform: translateY(0); }
    }
    
    .fade-in {
        animation: fadeIn 0.5s ease-out;
    }
    
    /* Tooltip styling */
    .tooltip {
        position: relative;
        display: inline-block;
        cursor: help;
    }
    
    .tooltip .tooltiptext {
        visibility: hidden;
        width: 200px;
        background-color: #555;
        color: #fff;
        text-align: center;
        border-radius: 6px;
        padding: 5px;
        position: absolute;
        z-index: 1;
        bottom: 125%;
        left: 50%;
        margin-left: -100px;
        opacity: 0;
        transition: opacity 0.3s;
    }
    
    .tooltip:hover .tooltiptext {
        visibility: visible;
        opacity: 1;
    }
    </style>
    """, unsafe_allow_html=True)

# =============================================================================
# ОСНОВНОЙ КЭШ (из оригинального кода)
# =============================================================================

class Cache:
    """Cache for API results"""
    
    def __init__(self, cache_dir=CACHE_DIR):
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)
    
    def _get_cache_path(self, key):
        hash_key = hashlib.md5(key.encode('utf-8')).hexdigest()
        return os.path.join(self.cache_dir, f"{hash_key}.pkl")
    
    def get(self, key):
        cache_path = self._get_cache_path(key)
        if os.path.exists(cache_path):
            try:
                with open(cache_path, 'rb') as f:
                    return pickle.load(f)
            except Exception as e:
                logger.warning(f"Cache read error: {e}")
        return None
    
    def set(self, key, data):
        cache_path = self._get_cache_path(key)
        try:
            with open(cache_path, 'wb') as f:
                pickle.dump(data, f)
        except Exception as e:
            logger.warning(f"Cache write error: {e}")
    
    def clear(self):
        for filename in os.listdir(self.cache_dir):
            file_path = os.path.join(self.cache_dir, filename)
            try:
                if os.path.isfile(file_path):
                    os.unlink(file_path)
            except Exception as e:
                logger.warning(f"Error deleting {file_path}: {e}")

cache = Cache()

# =============================================================================
# ОРГАНИЗАЦИЯ (из оригинального кода)
# =============================================================================

def normalize_org_name(name):
    """
    Normalize organization name for search:
    - Lowercase
    - Remove extra spaces
    - Handle hyphens
    - Transliterate unicode to ascii
    """
    if not name:
        return ""
    
    name = str(name).lower().strip()
    name = unidecode(name)
    name = name.replace('-', ' ')
    name = re.sub(r'\s+', ' ', name)
    
    return name

def normalize_for_fuzzy(name):
    """Additional normalization for fuzzy search"""
    name = normalize_org_name(name)
    
    # Remove common stop words
    stop_words = ['the', 'of', 'and', 'in', 'for', 'at', 'university', 'institute', 
                  'college', 'school', 'laboratory', 'lab', 'center', 'centre']
    
    words = name.split()
    words = [w for w in words if w not in stop_words]
    
    return ' '.join(words)

@retry(
    retry=retry_if_exception_type((requests.exceptions.RequestException,)),
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_exponential(multiplier=1, min=2, max=30)
)
def search_organization_by_name(org_name):
    """
    Search organization in OpenAlex by name
    Returns list of possible matches
    """
    cache_key = f"org_search_{org_name}"
    cached = cache.get(cache_key)
    if cached is not None:
        logger.info(f"Loaded organization search results from cache: {org_name}")
        return cached
    
    logger.info(f"Searching organization in OpenAlex: {org_name}")
    
    normalized = normalize_org_name(org_name)
    
    search_queries = [
        normalized,
        normalized.replace(' ', '-'),
        normalized.replace('-', ' '),
    ]
    
    all_results = []
    
    for query in set(search_queries):
        url = "https://api.openalex.org/institutions"
        params = {
            'search': query,
            'per-page': 50,
            'sort': 'relevance_score:desc',
            'mailto': CROSSREF_EMAIL
        }
        
        try:
            response = requests.get(url, params=params, timeout=15)
            response.raise_for_status()
            data = response.json()
            
            results = data.get('results', [])
            for inst in results:
                inst_info = {
                    'id': inst.get('id', '').replace('https://openalex.org/', ''),
                    'ror': inst.get('ror'),
                    'display_name': inst.get('display_name'),
                    'country': inst.get('country_code'),
                    'type': inst.get('type'),
                    'relevance_score': inst.get('relevance_score', 0),
                    'works_count': inst.get('works_count', 0),
                    'cited_by_count': inst.get('cited_by_count', 0)
                }
                
                alt_names = [inst.get('display_name', '')]
                for label in inst.get('international', {}).get('display_name', {}).values():
                    alt_names.append(label)
                
                inst_info['all_names'] = list(set(alt_names))
                all_results.append(inst_info)
            
            time.sleep(RATE_LIMIT_DELAY)
            
        except Exception as e:
            logger.error(f"Error searching '{query}': {e}")
    
    # Remove duplicates by ROR
    unique_results = {}
    for r in all_results:
        if r['ror'] and r['ror'] not in unique_results:
            unique_results[r['ror']] = r
    
    results_list = list(unique_results.values())
    
    # Apply fuzzy matching for ranking
    if results_list and org_name:
        name_to_result = {}
        for r in results_list:
            for name in r['all_names']:
                name_to_result[normalize_for_fuzzy(name)] = r['ror']
        
        query_norm = normalize_for_fuzzy(org_name)
        all_names = list(name_to_result.keys())
        
        if all_names:
            best_matches = process.extract(query_norm, all_names, scorer=fuzz.token_sort_ratio, limit=10)
            
            matched_rors = []
            for match_name, score, _ in best_matches:
                ror = name_to_result.get(match_name)
                if ror and ror not in matched_rors:
                    matched_rors.append(ror)
            
            sorted_results = []
            for ror in matched_rors:
                for r in results_list:
                    if r['ror'] == ror:
                        sorted_results.append(r)
                        break
            
            for r in results_list:
                if r not in sorted_results:
                    sorted_results.append(r)
            
            results_list = sorted_results
    
    cache.set(cache_key, results_list)
    
    return results_list

def select_organization(org_input):
    """
    Determine organization ROR from input
    Returns (ror_id, error_or_results)
    """
    org_input = org_input.strip()
    
    # Check if input is ROR ID
    ror_pattern = r'^[a-zA-Z0-9]+$'
    if re.match(ror_pattern, org_input) and len(org_input) > 5:
        logger.info(f"ROR ID provided: {org_input}")
        return org_input, None
    
    # Check if URL with ROR
    if 'ror.org/' in org_input:
        match = re.search(r'ror\.org/([a-zA-Z0-9]+)', org_input)
        if match:
            ror_id = match.group(1)
            logger.info(f"Extracted ROR ID from URL: {ror_id}")
            return ror_id, None
    
    # Search by name
    logger.info(f"Searching organization by name: {org_input}")
    results = search_organization_by_name(org_input)
    
    if not results:
        logger.warning(f"No organizations found: {org_input}")
        return None, "Organization not found"
    
    if len(results) == 1:
        org = results[0]
        ror_id = org['ror'].replace('https://ror.org/', '') if org['ror'] else None
        logger.info(f"Found single organization: {org['display_name']} (ROR: {ror_id})")
        return ror_id, None
    
    logger.info(f"Found {len(results)} organizations")
    return None, results

# =============================================================================
# ПАРСИНГ ГОДОВ (из оригинального кода)
# =============================================================================

def parse_year_input(s):
    """Parse year string: 2023, 2022-2024, 2021,2023-2025"""
    years = set()
    s = re.sub(r'\s+', '', s.strip())
    if not s:
        return []
    for part in s.split(','):
        if '-' in part:
            try:
                a, b = map(int, part.split('-'))
                years.update(range(min(a, b), max(a, b) + 1))
            except:
                pass
        else:
            try:
                years.add(int(part))
            except:
                pass
    return sorted(years)

def get_expanded_years(original_years):
    """Expand period by +-1 year"""
    expanded = set()
    for y in original_years:
        expanded.update([y - 1, y, y + 1])
    return sorted(expanded)

def years_to_filter_str(years):
    """Convert years list to OpenAlex filter string"""
    if not years:
        return ""
    return "publication_year:" + "|".join(map(str, years))

def get_late_date(online_str, print_str):
    """Determine latest date from online and print"""
    candidates = []
    for ds in [online_str, print_str]:
        if not ds:
            continue
        try:
            if len(ds) == 4:
                candidates.append(datetime(int(ds), 1, 1))
            elif len(ds) == 7:
                y, m = map(int, ds.split('-'))
                candidates.append(datetime(y, m, 1))
            else:
                candidates.append(date_parse(ds))
        except:
            pass
    return max(candidates) if candidates else None

# =============================================================================
# СБОР ДАННЫХ (из оригинального кода с добавлением ISSN)
# =============================================================================

@retry(
    retry=retry_if_exception_type((requests.exceptions.RequestException,)),
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_exponential(multiplier=1, min=2, max=30)
)
def get_crossref_data(doi):
    """Get enhanced data from Crossref by DOI"""
    
    cache_key = f"crossref_{doi}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    
    doi_clean = re.sub(r'^(https?://(dx\.)?doi\.org/|doi:?)', '', doi.strip(), flags=re.I)
    url = f"https://api.crossref.org/works/{doi_clean}"
    headers = {'User-Agent': f'DataCollector/1.0 (mailto:{CROSSREF_EMAIL})'}
    
    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        
        data = response.json()
        msg = data.get('message', {})
        
        def fmt_date(date_parts):
            if not date_parts or not date_parts[0]:
                return None
            p = date_parts[0]
            if len(p) >= 3:
                return f"{p[0]}-{p[1]:02d}-{p[2]:02d}"
            if len(p) == 2:
                return f"{p[0]}-{p[1]:02d}"
            if len(p) == 1:
                return str(p[0])
            return None
        
        print_date = None
        if 'published-print' in msg:
            print_date = fmt_date(msg['published-print'].get('date-parts'))
        if not print_date and 'issued' in msg:
            print_date = fmt_date(msg['issued'].get('date-parts'))
        
        online_date = None
        if 'published-online' in msg:
            online_date = fmt_date(msg['published-online'].get('date-parts'))
        if not online_date and 'created' in msg:
            online_date = fmt_date(msg['created'].get('date-parts'))
        
        late_dt = get_late_date(online_date, print_date)
        
        authors = []
        orcids = []
        for author in msg.get('author', []):
            given = author.get('given', '')
            family = author.get('family', '')
            if given and family:
                authors.append(f"{family}, {given}")
            elif family:
                authors.append(family)
            elif given:
                authors.append(given)
            
            orcid = author.get('ORCID', '')
            if orcid:
                orcid_clean = re.sub(r'^https?://orcid\.org/', '', orcid)
                orcids.append(orcid_clean)
        
        authors_str = '; '.join(authors) if authors else ''
        orcids_str = '; '.join(orcids) if orcids else ''
        authors_count = len(authors)
        
        # Extract ISSN information
        issn_list = []
        
        # Get ISSN from ISSN field if present
        if 'ISSN' in msg and msg['ISSN']:
            issn_list.extend(msg['ISSN'])
        
        # Get ISSN from issn-type field if present (for additional validation/ordering)
        if 'issn-type' in msg and msg['issn-type']:
            for issn_type_item in msg['issn-type']:
                if 'value' in issn_type_item and issn_type_item['value'] not in issn_list:
                    issn_list.append(issn_type_item['value'])
        
        # Remove duplicates while preserving order
        seen = set()
        unique_issns = []
        for issn in issn_list:
            if issn not in seen:
                seen.add(issn)
                unique_issns.append(issn)
        
        # Format ISSN string: either single ISSN or multiple separated by semicolon
        if unique_issns:
            if len(unique_issns) == 1:
                issn_str = unique_issns[0]
            else:
                issn_str = '; '.join(unique_issns)
        else:
            issn_str = ''
        
        pages = msg.get('page', '')
        if not pages:
            article_number = msg.get('article-number', '')
            pages = article_number if article_number else 'not recorded'
        
        references_count = len(msg.get('reference', [])) if 'reference' in msg else 0
        citations_cr = msg.get('is-referenced-by-count', 0)
        
        # Получаем DOI в чистом виде
        doi_clean_final = msg.get('DOI', '')
        if not doi_clean_final:
            doi_clean_final = doi_clean
        
        result = {
            'doi': doi_clean_final,
            'print_date': print_date,
            'online_date': online_date,
            'late_dt': late_dt,
            'late_year': late_dt.year if late_dt else None,
            'title': (msg.get('title') or [''])[0] or 'No title',
            'authors': authors_str,
            'orcids': orcids_str,
            'authors_count': authors_count,
            'journal': msg.get('container-title', [''])[0] if msg.get('container-title') else '',
            'issn': issn_str,  # Added ISSN field
            'volume': msg.get('volume', ''),
            'issue': msg.get('issue', ''),
            'pages': pages,
            'references_count': references_count,
            'citations_cr': citations_cr,
            'publisher': msg.get('publisher', ''),
            'type': msg.get('type', '')
        }
        
        cache.set(cache_key, result)
        return result
        
    except Exception as e:
        logger.error(f"Error getting Crossref data for {doi}: {e}")
        return None

@retry(
    retry=retry_if_exception_type((requests.exceptions.RequestException,)),
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_exponential(multiplier=1, min=2, max=30)
)
def get_openalex_data(doi, target_ror=None):
    """Get enhanced data from OpenAlex by DOI"""
    
    cache_key = f"openalex_{doi}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    
    doi_clean = re.sub(r'^(https?://(dx\.)?doi\.org/|doi:?)', '', doi.strip(), flags=re.I)
    
    url = "https://api.openalex.org/works"
    params = {
        'filter': f'doi:{doi_clean}',
        'mailto': CROSSREF_EMAIL
    }
    
    try:
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        
        data = response.json()
        results = data.get('results', [])
        
        if not results:
            logger.warning(f"Work not found in OpenAlex: {doi}")
            return None
        
        work = results[0]
        
        affiliations = []
        countries = []
        
        for authorship in work.get('authorships', []):
            for inst in authorship.get('institutions', []):
                inst_name = inst.get('display_name', '')
                if inst_name:
                    affiliations.append(inst_name)
                
                country = inst.get('country_code', '')
                if country:
                    countries.append(country)
        
        affiliations = list(dict.fromkeys(affiliations))
        countries = list(dict.fromkeys(countries))
        
        affiliations_str = '; '.join(affiliations) if affiliations else ''
        countries_str = '; '.join(countries) if countries else ''
        
        is_oa = work.get('open_access', {}).get('is_oa', False)
        
        funders = []
        for grant in work.get('grants', []):
            funder = grant.get('funder_display_name', '')
            if funder:
                funders.append(funder)
        
        funding_str = '; '.join(funders) if funders else ''
        citations_oa = work.get('cited_by_count', 0)
        
        publication_year = work.get('publication_year')
        publication_date = work.get('publication_date')
        
        # Получаем DOI в чистом виде из OpenAlex
        doi_oa = work.get('doi', '')
        if doi_oa:
            doi_oa = re.sub(r'^(https?://(dx\.)?doi\.org/|doi:?)', '', doi_oa, flags=re.I)
        
        result = {
            'doi': doi_oa or doi_clean,
            'openalex_id': work.get('id', ''),
            'affiliations': affiliations_str,
            'countries': countries_str,
            'is_oa': is_oa,
            'funding': funding_str,
            'citations_oa': citations_oa,
            'publication_year': publication_year,
            'publication_date': publication_date,
            'type': work.get('type', ''),
            'language': work.get('language', '')
        }
        
        cache.set(cache_key, result)
        return result
        
    except Exception as e:
        logger.error(f"Error getting OpenAlex data for {doi}: {e}")
        return None

def fetch_all_dois_openalex(ror, years_expanded):
    """Fetch all DOIs from OpenAlex with cursor pagination"""
    if not ror or not years_expanded:
        return [], "No ROR or period"
    
    logger.info(f"Fetching DOIs from OpenAlex: ROR={ror}, years={years_expanded}")
    
    filter_parts = [
        f"institutions.ror:{ror}",
        "has_doi:true",
        years_to_filter_str(years_expanded)
    ]
    filter_str = ",".join(p for p in filter_parts if p)
    
    base_params = {
        "filter": filter_str,
        "select": "doi,title,publication_year,authorships",
        "per_page": 200,
        "sort": "publication_date:desc",
        "mailto": CROSSREF_EMAIL
    }
    
    all_dois = []
    cursor = "*"
    total_expected = None
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    while cursor:
        params = base_params.copy()
        params["cursor"] = cursor
        
        try:
            response = requests.get("https://api.openalex.org/works", params=params, timeout=15)
            response.raise_for_status()
            data = response.json()
            
            if total_expected is None:
                total_expected = data["meta"]["count"]
                logger.info(f"Total records in OpenAlex: {total_expected}")
            
            results = data.get("results", [])
            new_dois = [w["doi"] for w in results if w.get("doi")]
            all_dois.extend(new_dois)
            
            # Update progress
            if total_expected > 0:
                progress = min(len(all_dois) / total_expected, 1.0)
                progress_bar.progress(progress)
                status_text.text(f"📥 Collected {len(all_dois):,} of {total_expected:,} DOIs")
            
            cursor = data["meta"].get("next_cursor")
            if cursor:
                time.sleep(RATE_LIMIT_DELAY)
            
        except Exception as e:
            logger.error(f"Error at cursor {cursor}: {e}")
            return all_dois, f"Error: {str(e)}"
    
    progress_bar.empty()
    status_text.empty()
    
    all_dois = list(dict.fromkeys(all_dois))
    logger.info(f"Unique DOIs collected: {len(all_dois)}")
    
    return all_dois, None

# =============================================================================
# ПАРАЛЛЕЛЬНАЯ ОБРАБОТКА DOI (из оригинального кода)
# =============================================================================

def process_doi_complete(doi, target_ror=None):
    """
    Complete DOI processing: get data from Crossref and OpenAlex
    """
    result = {
        'doi': doi,
        'status': 'processing'
    }
    
    cr_data = get_crossref_data(doi)
    if cr_data:
        result.update(cr_data)
    else:
        result['status'] = 'crossref_error'
        return result
    
    oa_data = get_openalex_data(doi, target_ror)
    if oa_data:
        result.update(oa_data)
        result['status'] = 'success'
    else:
        result['status'] = 'openalex_error'
    
    return result

def process_dois_parallel(dois, target_ror=None, max_workers=MAX_WORKERS):
    """Parallel DOI processing with retries for errors"""
    
    results = []
    errors = []
    remaining_dois = dois.copy()
    attempt = 1
    max_attempts = 3
    
    logger.info(f"Starting processing of {len(dois)} DOIs, max attempts: {max_attempts}")
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    while remaining_dois and attempt <= max_attempts:
        logger.info(f"Attempt {attempt}/{max_attempts}, remaining DOIs: {len(remaining_dois)}")
        
        current_results = []
        current_errors = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_doi = {executor.submit(process_doi_complete, doi, target_ror): doi 
                            for doi in remaining_dois}
            
            completed = 0
            total = len(remaining_dois)
            
            for future in concurrent.futures.as_completed(future_to_doi):
                doi = future_to_doi[future]
                try:
                    res = future.result()
                    if res.get('status') == 'success':
                        current_results.append(res)
                    else:
                        current_errors.append(doi)
                        logger.debug(f"Error processing {doi}: {res.get('status')}")
                except Exception as e:
                    current_errors.append(doi)
                    logger.error(f"Exception processing {doi}: {e}")
                
                completed += 1
                progress_bar.progress(completed / total)
                status_text.text(f"🔍 Processing attempt {attempt}: {completed}/{total}")
        
        results.extend(current_results)
        
        if attempt < max_attempts:
            remaining_dois = current_errors
            errors.extend(current_errors)
            logger.info(f"Attempt {attempt} completed. Success: {len(current_results)}, errors: {len(current_errors)}")
            time.sleep(2)
        else:
            errors.extend(current_errors)
        
        attempt += 1
    
    progress_bar.empty()
    status_text.empty()
    
    logger.info(f"Processing completed. Total success: {len(results)}, errors: {len(errors)}")
    
    return results, errors

# =============================================================================
# АНАЛИТИЧЕСКИЕ ФУНКЦИИ (из оригинального кода)
# =============================================================================

def extract_authors(authors_str):
    """Extract author names from semicolon-separated string"""
    if not authors_str:
        return []
    return [a.strip() for a in authors_str.split(';') if a.strip()]

def extract_countries(countries_str):
    """Extract country codes from semicolon-separated string"""
    if not countries_str:
        return []
    return [c.strip() for c in countries_str.split(';') if c.strip()]

def extract_affiliations(affiliations_str):
    """Extract affiliation names from semicolon-separated string"""
    if not affiliations_str:
        return []
    return [a.strip() for a in affiliations_str.split(';') if a.strip()]

def extract_issns(issn_str):
    """Extract ISSNs from semicolon-separated string"""
    if not issn_str or pd.isna(issn_str):
        return []
    return [issn.strip() for issn in str(issn_str).split(';') if issn.strip()]

def create_affiliation_network(df, period_only=True):
    """
    Create co-affiliation network
    Returns graph and node statistics
    """
    data = df[df['belongs_to_period'] == True] if period_only else df
    
    G = nx.Graph()
    affiliation_stats = defaultdict(lambda: {'papers': 0, 'countries': set()})
    
    for _, row in data.iterrows():
        affiliations = extract_affiliations(row.get('affiliations', ''))
        countries = extract_countries(row.get('countries', ''))
        
        # Count papers per affiliation
        for aff in affiliations:
            affiliation_stats[aff]['papers'] += 1
        
        # Add edges between affiliations in the same paper
        if len(affiliations) > 1:
            for i in range(len(affiliations)):
                for j in range(i+1, len(affiliations)):
                    if G.has_edge(affiliations[i], affiliations[j]):
                        G[affiliations[i]][affiliations[j]]['weight'] += 1
                    else:
                        G.add_edge(affiliations[i], affiliations[j], weight=1)
        
        # Add country info
        if countries:
            for aff in affiliations:
                affiliation_stats[aff]['countries'].update(countries)
    
    # Add nodes with attributes
    for aff, stats in affiliation_stats.items():
        G.add_node(aff, 
                  papers=stats['papers'],
                  countries=', '.join(stats['countries']))
    
    return G, affiliation_stats

def create_country_network(df, period_only=True):
    """
    Create country collaboration network
    Returns graph and node statistics
    """
    data = df[df['belongs_to_period'] == True] if period_only else df
    
    G = nx.Graph()
    country_stats = defaultdict(lambda: {'papers': 0, 'affiliations': set()})
    
    for _, row in data.iterrows():
        countries = extract_countries(row.get('countries', ''))
        affiliations = extract_affiliations(row.get('affiliations', ''))
        
        # Count papers per country
        for country in countries:
            country_stats[country]['papers'] += 1
        
        # Add edges between countries in the same paper
        if len(countries) > 1:
            for i in range(len(countries)):
                for j in range(i+1, len(countries)):
                    if G.has_edge(countries[i], countries[j]):
                        G[countries[i]][countries[j]]['weight'] += 1
                    else:
                        G.add_edge(countries[i], countries[j], weight=1)
        
        # Add affiliation info
        if affiliations:
            for country in countries:
                country_stats[country]['affiliations'].update(affiliations)
    
    # Add nodes with attributes
    for country, stats in country_stats.items():
        G.add_node(country, 
                  papers=stats['papers'],
                  n_affiliations=len(stats['affiliations']))
    
    return G, country_stats

def generate_author_frequency(df, period_only=True):
    """Generate author frequency statistics"""
    data = df[df['belongs_to_period'] == True] if period_only else df
    
    author_counter = Counter()
    for authors_str in data['authors'].dropna():
        authors = extract_authors(authors_str)
        author_counter.update(authors)
    
    return author_counter

def generate_journal_frequency(df, period_only=True):
    """Generate journal frequency statistics"""
    data = df[df['belongs_to_period'] == True] if period_only else df
    return data['journal'].value_counts()

def generate_publisher_frequency(df, period_only=True):
    """Generate publisher frequency statistics"""
    data = df[df['belongs_to_period'] == True] if period_only else df
    return data['publisher'].value_counts()

def generate_citation_stats(df, period_only=True):
    """Generate citation statistics"""
    data = df[df['belongs_to_period'] == True] if period_only else df
    
    stats = {
        'citations_cr': {
            'total': data['citations_cr'].sum(),
            'mean': data['citations_cr'].mean(),
            'median': data['citations_cr'].median(),
            'max': data['citations_cr'].max(),
            'distribution': data['citations_cr'].value_counts().sort_index()
        },
        'citations_oa': {
            'total': data['citations_oa'].sum(),
            'mean': data['citations_oa'].mean(),
            'median': data['citations_oa'].median(),
            'max': data['citations_oa'].max(),
            'distribution': data['citations_oa'].value_counts().sort_index()
        }
    }
    
    return stats

def generate_country_frequency(df, period_only=True):
    """Generate country frequency statistics"""
    data = df[df['belongs_to_period'] == True] if period_only else df
    
    country_counter = Counter()
    for countries_str in data['countries'].dropna():
        countries = extract_countries(countries_str)
        country_counter.update(countries)
    
    return country_counter

def generate_oa_stats(df, period_only=True):
    """Generate Open Access statistics"""
    data = df[df['belongs_to_period'] == True] if period_only else df
    
    oa_count = data['is_oa'].sum()
    non_oa_count = len(data) - oa_count
    
    return {
        'oa_count': oa_count,
        'non_oa_count': non_oa_count,
        'oa_percentage': (oa_count / len(data)) * 100 if len(data) > 0 else 0,
        'oa_by_year': data.groupby('late_year')['is_oa'].mean() * 100
    }

# =============================================================================
# ФУНКЦИИ ДЛЯ ГРАФИКОВ
# =============================================================================

def create_publication_timeline(df, palette):
    """Create publication timeline plot"""
    if df.empty or 'late_year' not in df.columns:
        return None
    
    year_counts = df['late_year'].value_counts().sort_index()
    
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.bar(year_counts.index, year_counts.values, color=palette[0], alpha=0.8, edgecolor='black', linewidth=0.5)
    ax.set_xlabel('Year')
    ax.set_ylabel('Number of Publications')
    ax.set_title('Publication Timeline')
    ax.set_xticks(year_counts.index)
    ax.set_xticklabels(year_counts.index, rotation=45)
    
    # Add value labels on bars
    for i, (year, count) in enumerate(year_counts.items()):
        ax.text(year, count + 0.5, str(count), ha='center', va='bottom', fontsize=9)
    
    plt.tight_layout()
    return fig

def create_citation_distribution(df, palette):
    """Create citation distribution plot"""
    if df.empty:
        return None
    
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    
    # Crossref citations
    cr_data = df['citations_cr'].dropna()
    if not cr_data.empty:
        axes[0].hist(cr_data, bins=30, color=palette[1], alpha=0.7, edgecolor='black', linewidth=0.5)
        axes[0].axvline(cr_data.mean(), color='red', linestyle='--', linewidth=1.5, label=f'Mean: {cr_data.mean():.1f}')
        axes[0].axvline(cr_data.median(), color='blue', linestyle='--', linewidth=1.5, label=f'Median: {cr_data.median():.1f}')
        axes[0].set_xlabel('Citations (Crossref)')
        axes[0].set_ylabel('Frequency')
        axes[0].set_title('Crossref Citation Distribution')
        axes[0].legend()
        axes[0].grid(True, alpha=0.3)
    
    # OpenAlex citations
    oa_data = df['citations_oa'].dropna()
    if not oa_data.empty:
        axes[1].hist(oa_data, bins=30, color=palette[2], alpha=0.7, edgecolor='black', linewidth=0.5)
        axes[1].axvline(oa_data.mean(), color='red', linestyle='--', linewidth=1.5, label=f'Mean: {oa_data.mean():.1f}')
        axes[1].axvline(oa_data.median(), color='blue', linestyle='--', linewidth=1.5, label=f'Median: {oa_data.median():.1f}')
        axes[1].set_xlabel('Citations (OpenAlex)')
        axes[1].set_ylabel('Frequency')
        axes[1].set_title('OpenAlex Citation Distribution')
        axes[1].legend()
        axes[1].grid(True, alpha=0.3)
    
    plt.tight_layout()
    return fig

def create_top_journals_plot(df, palette, n=10):
    """Create top journals bar plot"""
    if df.empty or 'journal' not in df.columns:
        return None
    
    journal_counts = df['journal'].value_counts().head(n)
    
    fig, ax = plt.subplots(figsize=(12, 8))
    bars = ax.barh(range(len(journal_counts)), journal_counts.values, color=palette[3], alpha=0.8, edgecolor='black', linewidth=0.5)
    ax.set_yticks(range(len(journal_counts)))
    ax.set_yticklabels([str(j)[:40] + '...' if len(str(j)) > 40 else str(j) for j in journal_counts.index])
    ax.set_xlabel('Number of Publications')
    ax.set_title(f'Top {n} Journals')
    ax.invert_yaxis()
    
    # Add value labels
    for i, (bar, count) in enumerate(zip(bars, journal_counts.values)):
        ax.text(count + 0.5, bar.get_y() + bar.get_height()/2, str(count), va='center', fontsize=9)
    
    plt.tight_layout()
    return fig

def create_country_map(df, palette):
    """Create country frequency map"""
    if df.empty or 'countries' not in df.columns:
        return None
    
    country_counter = generate_country_frequency(df)
    
    if not country_counter:
        return None
    
    # Create DataFrame for plotly
    country_df = pd.DataFrame([
        {'country': country, 'papers': count}
        for country, count in country_counter.most_common(20)
    ])
    
    fig = px.choropleth(
        country_df,
        locations='country',
        locationmode='ISO-3',
        color='papers',
        hover_name='country',
        color_continuous_scale=px.colors.sequential.Plasma,
        title='Geographic Distribution of Publications'
    )
    
    fig.update_layout(
        geo=dict(
            showframe=True,
            showcoastlines=True,
            projection_type='natural earth'
        ),
        width=1000,
        height=600
    )
    
    return fig

def create_oa_trend(df, palette):
    """Create Open Access trend plot"""
    if df.empty or 'late_year' not in df.columns or 'is_oa' not in df.columns:
        return None
    
    oa_by_year = df.groupby('late_year')['is_oa'].agg(['count', 'sum'])
    oa_by_year['percentage'] = (oa_by_year['sum'] / oa_by_year['count']) * 100
    
    fig, ax = plt.subplots(figsize=(12, 6))
    
    ax.plot(oa_by_year.index, oa_by_year['percentage'], marker='o', linewidth=2, color=palette[4], markersize=8)
    ax.fill_between(oa_by_year.index, oa_by_year['percentage'], alpha=0.3, color=palette[4])
    
    ax.set_xlabel('Year')
    ax.set_ylabel('Open Access Percentage (%)')
    ax.set_title('Open Access Trend Over Time')
    ax.set_ylim(0, 100)
    ax.set_xticks(oa_by_year.index)
    ax.set_xticklabels(oa_by_year.index, rotation=45)
    ax.grid(True, alpha=0.3)
    
    # Add value labels
    for year, pct in oa_by_year['percentage'].items():
        ax.text(year, pct + 2, f'{pct:.1f}%', ha='center', va='bottom', fontsize=9)
    
    plt.tight_layout()
    return fig

def create_affiliation_network_plot(df, palette):
    """Create affiliation network visualization"""
    G_aff, aff_stats = create_affiliation_network(df)
    
    if len(G_aff.nodes()) == 0:
        return None
    
    # Filter to top nodes for readability
    node_degrees = dict(G_aff.degree())
    top_nodes = sorted(node_degrees, key=node_degrees.get, reverse=True)[:15]
    G_aff_sub = G_aff.subgraph(top_nodes)
    
    pos = nx.spring_layout(G_aff_sub, k=2, iterations=50)
    
    fig, ax = plt.subplots(figsize=(14, 10))
    
    # Draw edges
    edge_weights = [G_aff_sub[u][v]['weight'] for u, v in G_aff_sub.edges()]
    nx.draw_networkx_edges(G_aff_sub, pos, width=edge_weights, alpha=0.5, edge_color='gray', ax=ax)
    
    # Draw nodes
    node_sizes = [G_aff_sub.nodes[node]['papers'] * 100 for node in G_aff_sub.nodes()]
    node_colors = [palette[i % len(palette)] for i in range(len(G_aff_sub.nodes()))]
    
    nx.draw_networkx_nodes(G_aff_sub, pos, node_size=node_sizes, node_color=node_colors, alpha=0.8, ax=ax)
    
    # Draw labels
    labels = {node: node[:20] + '...' if len(node) > 20 else node for node in G_aff_sub.nodes()}
    nx.draw_networkx_labels(G_aff_sub, pos, labels, font_size=8, ax=ax)
    
    ax.set_title('Affiliation Collaboration Network (Top 15)')
    ax.axis('off')
    
    plt.tight_layout()
    return fig

def create_country_network_plot(df, palette):
    """Create country network visualization"""
    G_country, country_stats = create_country_network(df)
    
    if len(G_country.nodes()) == 0:
        return None
    
    pos = nx.spring_layout(G_country, k=1, iterations=50)
    
    fig, ax = plt.subplots(figsize=(12, 8))
    
    # Draw edges
    edge_weights = [G_country[u][v]['weight'] for u, v in G_country.edges()]
    nx.draw_networkx_edges(G_country, pos, width=edge_weights, alpha=0.5, edge_color='gray', ax=ax)
    
    # Draw nodes
    node_sizes = [G_country.nodes[node]['papers'] * 200 for node in G_country.nodes()]
    node_colors = [palette[i % len(palette)] for i in range(len(G_country.nodes()))]
    
    nx.draw_networkx_nodes(G_country, pos, node_size=node_sizes, node_color=node_colors, alpha=0.8, ax=ax)
    
    # Draw labels
    nx.draw_networkx_labels(G_country, pos, font_size=10, ax=ax)
    
    ax.set_title('Country Collaboration Network')
    ax.axis('off')
    
    plt.tight_layout()
    return fig

def create_author_collaboration_plot(df, palette, n=20):
    """Create author collaboration heatmap"""
    if df.empty or 'authors' not in df.columns:
        return None
    
    # Get top authors
    author_counter = generate_author_frequency(df)
    top_authors = [author for author, _ in author_counter.most_common(n)]
    
    # Create co-authorship matrix
    coauthor_matrix = pd.DataFrame(0, index=top_authors, columns=top_authors)
    
    for authors_str in df['authors'].dropna():
        authors = extract_authors(authors_str)
        authors_in_top = [a for a in authors if a in top_authors]
        
        for i in range(len(authors_in_top)):
            for j in range(i+1, len(authors_in_top)):
                coauthor_matrix.loc[authors_in_top[i], authors_in_top[j]] += 1
                coauthor_matrix.loc[authors_in_top[j], authors_in_top[i]] += 1
    
    if coauthor_matrix.sum().sum() == 0:
        return None
    
    fig, ax = plt.subplots(figsize=(14, 12))
    
    im = ax.imshow(coauthor_matrix, cmap='YlOrRd', aspect='auto')
    
    ax.set_xticks(range(len(top_authors)))
    ax.set_yticks(range(len(top_authors)))
    ax.set_xticklabels([a[:20] + '...' if len(a) > 20 else a for a in top_authors], rotation=90, fontsize=8)
    ax.set_yticklabels([a[:20] + '...' if len(a) > 20 else a for a in top_authors], fontsize=8)
    
    plt.colorbar(im, ax=ax, label='Number of Collaborations')
    ax.set_title(f'Top {n} Authors Collaboration Matrix')
    
    plt.tight_layout()
    return fig

def create_references_vs_citations_plot(df, palette):
    """Create references vs citations scatter plot"""
    if df.empty or 'references_count' not in df.columns or 'citations_cr' not in df.columns:
        return None
    
    fig, ax = plt.subplots(figsize=(10, 8))
    
    scatter = ax.scatter(
        df['references_count'], 
        df['citations_cr'],
        c=df['citations_oa'] if 'citations_oa' in df.columns else df['citations_cr'],
        s=df['authors_count'] * 20 if 'authors_count' in df.columns else 50,
        alpha=0.6,
        cmap='viridis',
        edgecolor='black',
        linewidth=0.5
    )
    
    ax.set_xlabel('Number of References')
    ax.set_ylabel('Citations (Crossref)')
    ax.set_title('References vs Citations')
    
    # Add trend line
    if len(df) > 1:
        z = np.polyfit(df['references_count'].fillna(0), df['citations_cr'].fillna(0), 1)
        p = np.poly1d(z)
        ax.plot(df['references_count'].sort_values(), p(df['references_count'].sort_values()), 
                "r--", alpha=0.8, label=f'Trend (slope: {z[0]:.3f})')
        ax.legend()
    
    plt.colorbar(scatter, ax=ax, label='OpenAlex Citations')
    plt.tight_layout()
    return fig

def create_publication_types_plot(df, palette):
    """Create publication types pie chart"""
    if df.empty or 'type' not in df.columns:
        return None
    
    type_counts = df['type'].value_counts()
    
    fig, ax = plt.subplots(figsize=(10, 8))
    
    wedges, texts, autotexts = ax.pie(
        type_counts.values,
        labels=type_counts.index,
        autopct='%1.1f%%',
        colors=palette[:len(type_counts)],
        startangle=90,
        wedgeprops={'edgecolor': 'black', 'linewidth': 0.5}
    )
    
    # Make percentage text bold and white
    for autotext in autotexts:
        autotext.set_color('white')
        autotext.set_fontweight('bold')
    
    ax.set_title('Publication Types Distribution')
    ax.axis('equal')
    
    plt.tight_layout()
    return fig

def create_language_distribution_plot(df, palette):
    """Create language distribution bar plot"""
    if df.empty or 'language' not in df.columns:
        return None
    
    lang_counts = df['language'].value_counts().head(10)
    
    fig, ax = plt.subplots(figsize=(10, 6))
    
    bars = ax.bar(range(len(lang_counts)), lang_counts.values, color=palette[5], alpha=0.8, edgecolor='black', linewidth=0.5)
    ax.set_xticks(range(len(lang_counts)))
    ax.set_xticklabels(lang_counts.index)
    ax.set_xlabel('Language')
    ax.set_ylabel('Number of Publications')
    ax.set_title('Top 10 Languages')
    
    # Add value labels
    for i, (bar, count) in enumerate(zip(bars, lang_counts.values)):
        ax.text(i, count + 0.5, str(count), ha='center', va='bottom', fontsize=9)
    
    plt.tight_layout()
    return fig

def create_funding_distribution_plot(df, palette):
    """Create funding distribution plot"""
    if df.empty or 'funding' not in df.columns:
        return None
    
    has_funding = df['funding'].notna() & (df['funding'] != '')
    funded_count = has_funding.sum()
    unfunded_count = len(df) - funded_count
    
    fig, ax = plt.subplots(figsize=(8, 8))
    
    wedges, texts, autotexts = ax.pie(
        [funded_count, unfunded_count],
        labels=['Funded', 'Unfunded'],
        autopct='%1.1f%%',
        colors=[palette[6], palette[7]],
        startangle=90,
        wedgeprops={'edgecolor': 'black', 'linewidth': 0.5}
    )
    
    for autotext in autotexts:
        autotext.set_color('white')
        autotext.set_fontweight('bold')
    
    ax.set_title('Funding Distribution')
    ax.axis('equal')
    
    plt.tight_layout()
    return fig

def create_quartile_distribution_plot(df, palette):
    """Create quartile distribution plot for IF and CS"""
    if df.empty:
        return None
    
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    
    # IF Quartiles
    if 'IF_Quartile' in df.columns:
        if_q_counts = df['IF_Quartile'].dropna().value_counts()
        if not if_q_counts.empty:
            axes[0].bar(if_q_counts.index, if_q_counts.values, color=palette[8], alpha=0.8, edgecolor='black', linewidth=0.5)
            axes[0].set_xlabel('Quartile')
            axes[0].set_ylabel('Number of Publications')
            axes[0].set_title('Impact Factor Quartile Distribution')
            axes[0].grid(True, alpha=0.3, axis='y')
            
            # Add value labels
            for i, (q, count) in enumerate(if_q_counts.items()):
                axes[0].text(i, count + 0.5, str(count), ha='center', va='bottom', fontsize=9)
    
    # CS Quartiles
    if 'CS_Quartile' in df.columns:
        cs_q_counts = df['CS_Quartile'].dropna().value_counts()
        if not cs_q_counts.empty:
            axes[1].bar(cs_q_counts.index, cs_q_counts.values, color=palette[9], alpha=0.8, edgecolor='black', linewidth=0.5)
            axes[1].set_xlabel('Quartile')
            axes[1].set_ylabel('Number of Publications')
            axes[1].set_title('CiteScore Quartile Distribution')
            axes[1].grid(True, alpha=0.3, axis='y')
            
            # Add value labels
            for i, (q, count) in enumerate(cs_q_counts.items()):
                axes[1].text(i, count + 0.5, str(count), ha='center', va='bottom', fontsize=9)
    
    plt.tight_layout()
    return fig

# =============================================================================
# СОЗДАНИЕ DATAFRAME (из оригинального кода с добавлением IF и CS)
# =============================================================================

def create_results_dataframe(results, target_years_set):
    """Create final DataFrame with results and add IF/CS data"""
    
    df = pd.DataFrame(results)
    
    if df.empty:
        return df
    
    df['belongs_to_period'] = df.apply(
        lambda row: row['late_dt'] is not None and row['late_dt'].year in target_years_set 
        if pd.notna(row['late_dt']) else False, 
        axis=1
    )
    
    if 'late_dt' in df.columns:
        df['late_date'] = df['late_dt'].apply(
            lambda x: x.strftime("%Y-%m-%d") if pd.notna(x) else None
        )
        df = df.drop('late_dt', axis=1)
    
    # Add IF and CS data based on ISSN
    if 'issn' in df.columns:
        # Initialize columns
        df['IF'] = None
        df['IF_Quartile'] = None
        df['CS'] = None
        df['CS_Quartile'] = None
        
        # Process each row
        for idx, row in df.iterrows():
            issn_str = row.get('issn', '')
            if issn_str and pd.notna(issn_str):
                issns = extract_issns(issn_str)
                
                # Try each ISSN until we find a match
                for issn in issns:
                    # Check IF
                    if pd.isna(df.at[idx, 'IF']):
                        if_val, if_q = issn_cache.find_if_match(issn)
                        if if_val is not None and pd.notna(if_val):
                            df.at[idx, 'IF'] = if_val
                            df.at[idx, 'IF_Quartile'] = if_q
                            break  # Stop if we found a match
                    
                    # Check CS
                    if pd.isna(df.at[idx, 'CS']):
                        cs_val, cs_q = issn_cache.find_cs_match(issn)
                        if cs_val is not None and pd.notna(cs_val):
                            df.at[idx, 'CS'] = cs_val
                            df.at[idx, 'CS_Quartile'] = cs_q
                            # Don't break here, continue to check IF if needed
    
    column_order = [
        'doi', 'title', 'belongs_to_period', 'late_date', 'late_year',
        'print_date', 'online_date', 'publication_year', 'publication_date',
        'authors', 'authors_count', 'orcids',
        'affiliations', 'countries', 'journal', 'issn', 'publisher', 'type',
        'volume', 'issue', 'pages',
        'is_oa', 'funding',
        'references_count', 'citations_cr', 'citations_oa',
        'IF', 'IF_Quartile', 'CS', 'CS_Quartile',
        'openalex_id', 'language', 'status'
    ]
    
    existing_cols = [col for col in column_order if col in df.columns]
    df = df[existing_cols]
    
    return df

# =============================================================================
# ЭКСПОРТ В EXCEL (из оригинального кода)
# =============================================================================

def export_to_excel(df, results, errors, selected_ror, orig_years_list, exp_years):
    """
    Export all data and analytics to Excel with multiple sheets
    """
    filename = f"analysis_ror_{selected_ror}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    
    # Create Excel writer with xlsxwriter
    output = io.BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    workbook = writer.book
    
    # Add formats
    header_format = workbook.add_format({
        'bold': True,
        'bg_color': '#4472C4',
        'font_color': 'white',
        'border': 1
    })
    
    # ===== SHEET 1: Main Data =====
    df.to_excel(writer, sheet_name='Main Data', index=False)
    worksheet = writer.sheets['Main Data']
    
    # Format headers
    for col_num, value in enumerate(df.columns.values):
        worksheet.write(0, col_num, value, header_format)
        worksheet.set_column(col_num, col_num, max(15, len(str(value))))
    
    # ===== SHEET 2: Period Statistics =====
    belong = df[df['belongs_to_period'] == True].copy()
    not_belong = df[df['belongs_to_period'] == False].copy()
    
    period_stats = pd.DataFrame({
        'Metric': [
            'Total DOIs found',
            'Successfully processed',
            'In target period',
            'Outside target period',
            'Open Access (in period)',
            'Non-Open Access (in period)',
            'Average authors per paper (in period)',
            'Average citations (Crossref) (in period)',
            'Average citations (OpenAlex) (in period)'
        ],
        'Value': [
            len(df) + len(errors),
            len(df),
            len(belong),
            len(not_belong),
            belong['is_oa'].sum() if not belong.empty else 0,
            len(belong) - belong['is_oa'].sum() if not belong.empty else 0,
            belong['authors_count'].mean() if not belong.empty else 0,
            belong['citations_cr'].mean() if not belong.empty else 0,
            belong['citations_oa'].mean() if not belong.empty else 0
        ]
    })
    
    period_stats.to_excel(writer, sheet_name='Period Statistics', index=False)
    
    # ===== SHEET 3: Author Frequency =====
    author_counter = generate_author_frequency(df)
    author_df = pd.DataFrame(author_counter.most_common(100), 
                            columns=['Author', 'Papers'])
    author_df.to_excel(writer, sheet_name='Top Authors', index=False)
    
    # ===== SHEET 4: Journal Frequency =====
    journal_freq = generate_journal_frequency(df)
    journal_df = pd.DataFrame({
        'Journal': journal_freq.index,
        'Papers': journal_freq.values
    })
    journal_df.to_excel(writer, sheet_name='Journal Frequency', index=False)
    
    # ===== SHEET 5: Publisher Frequency =====
    publisher_freq = generate_publisher_frequency(df)
    publisher_df = pd.DataFrame({
        'Publisher': publisher_freq.index,
        'Papers': publisher_freq.values
    })
    publisher_df.to_excel(writer, sheet_name='Publisher Frequency', index=False)
    
    # ===== SHEET 6: Country Frequency =====
    country_counter = generate_country_frequency(df)
    country_df = pd.DataFrame(country_counter.most_common(), 
                            columns=['Country', 'Papers'])
    country_df.to_excel(writer, sheet_name='Country Frequency', index=False)
    
    # ===== SHEET 7: Citation Statistics =====
    citation_stats = generate_citation_stats(df)
    
    citation_df = pd.DataFrame({
        'Metric': [
            'Total citations (Crossref)',
            'Mean citations (Crossref)',
            'Median citations (Crossref)',
            'Max citations (Crossref)',
            'Total citations (OpenAlex)',
            'Mean citations (OpenAlex)',
            'Median citations (OpenAlex)',
            'Max citations (OpenAlex)'
        ],
        'Value': [
            citation_stats['citations_cr']['total'],
            citation_stats['citations_cr']['mean'],
            citation_stats['citations_cr']['median'],
            citation_stats['citations_cr']['max'],
            citation_stats['citations_oa']['total'],
            citation_stats['citations_oa']['mean'],
            citation_stats['citations_oa']['median'],
            citation_stats['citations_oa']['max']
        ]
    })
    citation_df.to_excel(writer, sheet_name='Citation Statistics', index=False)
    
    # ===== SHEET 8: Year Distribution =====
    year_stats = belong['late_year'].value_counts().sort_index()
    year_df = pd.DataFrame({
        'Year': year_stats.index,
        'Papers': year_stats.values,
        'Percentage': (year_stats.values / len(belong)) * 100 if len(belong) > 0 else 0
    })
    year_df.to_excel(writer, sheet_name='Year Distribution', index=False)
    
    # ===== SHEET 9: OA Statistics =====
    oa_stats = generate_oa_stats(df)
    oa_df = pd.DataFrame({
        'Metric': ['Open Access', 'Non-Open Access', 'OA Percentage'],
        'Value': [oa_stats['oa_count'], oa_stats['non_oa_count'], oa_stats['oa_percentage']]
    })
    oa_df.to_excel(writer, sheet_name='OA Statistics', index=False)
    
    # ===== SHEET 10: IF Statistics =====
    if 'IF' in df.columns and 'IF_Quartile' in df.columns:
        if_data = df[['IF', 'IF_Quartile']].dropna(subset=['IF'])
        if not if_data.empty:
            if_stats = pd.DataFrame({
                'Metric': [
                    'Journals with IF',
                    'Average IF',
                    'Median IF',
                    'Max IF',
                    'Q1 Journals',
                    'Q2 Journals',
                    'Q3 Journals',
                    'Q4 Journals'
                ],
                'Value': [
                    len(if_data),
                    if_data['IF'].mean(),
                    if_data['IF'].median(),
                    if_data['IF'].max(),
                    (if_data['IF_Quartile'] == 'Q1').sum(),
                    (if_data['IF_Quartile'] == 'Q2').sum(),
                    (if_data['IF_Quartile'] == 'Q3').sum(),
                    (if_data['IF_Quartile'] == 'Q4').sum()
                ]
            })
            if_stats.to_excel(writer, sheet_name='IF Statistics', index=False)
    
    # ===== SHEET 11: CS Statistics =====
    if 'CS' in df.columns and 'CS_Quartile' in df.columns:
        cs_data = df[['CS', 'CS_Quartile']].dropna(subset=['CS'])
        if not cs_data.empty:
            cs_stats = pd.DataFrame({
                'Metric': [
                    'Journals with CiteScore',
                    'Average CiteScore',
                    'Median CiteScore',
                    'Max CiteScore',
                    'Q1 Journals',
                    'Q2 Journals',
                    'Q3 Journals',
                    'Q4 Journals'
                ],
                'Value': [
                    len(cs_data),
                    cs_data['CS'].mean(),
                    cs_data['CS'].median(),
                    cs_data['CS'].max(),
                    (cs_data['CS_Quartile'] == 'Q1').sum(),
                    (cs_data['CS_Quartile'] == 'Q2').sum(),
                    (cs_data['CS_Quartile'] == 'Q3').sum(),
                    (cs_data['CS_Quartile'] == 'Q4').sum()
                ]
            })
            cs_stats.to_excel(writer, sheet_name='CS Statistics', index=False)
    
    # ===== SHEET 12: Errors =====
    if errors:
        errors_df = pd.DataFrame({'DOI': errors, 'Status': 'Failed'})
        errors_df.to_excel(writer, sheet_name='Errors', index=False)
    
    # ===== SHEET 13: Metadata =====
    metadata = pd.DataFrame({
        'Parameter': [
            'Analysis Date',
            'ROR ID',
            'Original Period',
            'Search Period',
            'Total DOIs Found',
            'Successfully Processed',
            'Failed DOIs',
            'Cache Directory',
            'Log File',
            'Excel File'
        ],
        'Value': [
            datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            selected_ror,
            ', '.join(map(str, orig_years_list)),
            ', '.join(map(str, exp_years)),
            len(df) + len(errors),
            len(df),
            len(errors),
            CACHE_DIR,
            log_filename,
            filename
        ]
    })
    metadata.to_excel(writer, sheet_name='Metadata', index=False)
    
    # Auto-adjust column widths
    for sheet_name in writer.sheets:
        worksheet = writer.sheets[sheet_name]
        worksheet.set_column(0, 100, 20)
    
    writer.close()
    output.seek(0)
    
    return output, filename

# =============================================================================
# ОСНОВНОЕ ПРИЛОЖЕНИЕ STREAMLIT
# =============================================================================

def main():
    # Set page configuration
    set_page_config()
    
    # Load custom CSS
    load_custom_css()
    
    # Initialize session state
    if 'selected_ror' not in st.session_state:
        st.session_state.selected_ror = None
    if 'org_search_results' not in st.session_state:
        st.session_state.org_search_results = None
    if 'results_df' not in st.session_state:
        st.session_state.results_df = None
    if 'results_list' not in st.session_state:
        st.session_state.results_list = None
    if 'errors_list' not in st.session_state:
        st.session_state.errors_list = None
    if 'dois_list' not in st.session_state:
        st.session_state.dois_list = None
    if 'orig_years' not in st.session_state:
        st.session_state.orig_years = None
    if 'exp_years' not in st.session_state:
        st.session_state.exp_years = None
    if 'palette' not in st.session_state:
        st.session_state.palette = get_random_palette()
    
    # Sidebar for color palette selection
    with st.sidebar:
        # Logo
        if os.path.exists("logo.png"):
            st.image("logo.png", use_column_width=True)
        else:
            st.markdown("""
            <div style="text-align: center; padding: 20px; background: linear-gradient(90deg, #667eea 0%, #764ba2 100%); border-radius: 10px;">
                <h1 style="color: white; margin: 0;">UnIst</h1>
                <p style="color: white; opacity: 0.9;">analytics</p>
            </div>
            """, unsafe_allow_html=True)
        
        st.markdown("---")
        
        # Color palette selection
        st.markdown("### 🎨 Цветовая палитра")
        palette_names = list(COLOR_PALETTES.keys())
        
        col1, col2 = st.columns(2)
        with col1:
            selected_palette = st.selectbox(
                "Выберите палитру",
                palette_names,
                index=0
            )
        with col2:
            if st.button("🎲 Случайная"):
                st.session_state.palette = get_random_palette()
                st.rerun()
        
        if selected_palette:
            st.session_state.palette = get_palette_by_name(selected_palette)
        
        # Display palette preview
        st.markdown("**Предпросмотр:**")
        cols = st.columns(5)
        for i, color in enumerate(st.session_state.palette[:5]):
            cols[i].markdown(
                f'<div style="background-color: {color}; width: 30px; height: 30px; border-radius: 5px;"></div>',
                unsafe_allow_html=True
            )
        cols = st.columns(5)
        for i, color in enumerate(st.session_state.palette[5:10]):
            cols[i].markdown(
                f'<div style="background-color: {color}; width: 30px; height: 30px; border-radius: 5px;"></div>',
                unsafe_allow_html=True
            )
        
        st.markdown("---")
        
        # Cache info
        st.markdown("### 💾 Кэш")
        cache_size = sum(os.path.getsize(os.path.join(CACHE_DIR, f)) for f in os.listdir(CACHE_DIR) if os.path.isfile(os.path.join(CACHE_DIR, f))) if os.path.exists(CACHE_DIR) else 0
        st.markdown(f"**Файлов в кэше:** {len(os.listdir(CACHE_DIR)) if os.path.exists(CACHE_DIR) else 0}")
        st.markdown(f"**Размер кэша:** {cache_size / 1024:.2f} KB")
        
        if st.button("🗑️ Очистить кэш"):
            cache.clear()
            st.rerun()
    
    # Main content
    st.markdown("""
    <div class="title-container fade-in">
        <h1>📊 UnIst Analytics</h1>
        <p>Комплексный анализ публикаций организаций через OpenAlex и Crossref</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Input section
    col1, col2 = st.columns([3, 1])
    
    with col1:
        org_input = st.text_input(
            "🔍 Организация",
            placeholder="ROR ID, URL или название (например: 05wv0v765 или Ural Federal University)",
            key="org_input"
        )
    
    with col2:
        search_clicked = st.button("🔍 Найти организацию", use_container_width=True)
    
    if search_clicked and org_input:
        with st.spinner("Поиск организации..."):
            ror_id, error = select_organization(org_input)
            
            if ror_id:
                st.session_state.selected_ror = ror_id
                st.session_state.org_search_results = None
                st.markdown(f"""
                <div class="success-box fade-in">
                    ✅ Используется ROR: <b>{ror_id}</b>
                </div>
                """, unsafe_allow_html=True)
            elif error:
                st.markdown(f"""
                <div class="error-box fade-in">
                    ❌ {error}
                </div>
                """, unsafe_allow_html=True)
            else:
                st.session_state.org_search_results = error
    
    # Organization selection dropdown
    if st.session_state.org_search_results:
        results = st.session_state.org_search_results
        
        options = []
        for i, org in enumerate(results, 1):
            ror_id = org['ror'].replace('https://ror.org/', '') if org['ror'] else 'N/A'
            label = f"{i}. {org['display_name']} | {org.get('country', 'N/A')} | Works: {org['works_count']:,}"
            options.append(label)
        
        selected_idx = st.selectbox(
            "📋 Выберите организацию из списка:",
            range(len(options)),
            format_func=lambda x: options[x]
        )
        
        if st.button("✅ Подтвердить выбор", use_container_width=True):
            selected_org = results[selected_idx]
            ror_id = selected_org['ror'].replace('https://ror.org/', '') if selected_org['ror'] else None
            st.session_state.selected_ror = ror_id
            st.session_state.org_search_results = None
            st.markdown(f"""
            <div class="success-box fade-in">
                ✅ Выбрана организация: <b>{selected_org['display_name']}</b> (ROR: {ror_id})
            </div>
            """, unsafe_allow_html=True)
            st.rerun()
    
    # Show selected ROR
    if st.session_state.selected_ror:
        st.markdown(f"""
        <div class="info-box fade-in">
            <b>🏛 Выбранная организация:</b> ROR ID = {st.session_state.selected_ror}
        </div>
        """, unsafe_allow_html=True)
        
        # Year input
        years_input = st.text_input(
            "📅 Годы",
            placeholder="2023, 2022-2024, или 2021,2023-2025",
            key="years_input"
        )
        
        if st.button("🚀 Запустить анализ", use_container_width=True):
            if not years_input:
                st.markdown("""
                <div class="error-box fade-in">
                    ❌ Введите годы для анализа
                </div>
                """, unsafe_allow_html=True)
                return
            
            orig_years_list = parse_year_input(years_input)
            if not orig_years_list:
                st.markdown("""
                <div class="error-box fade-in">
                    ❌ Ошибка парсинга годов<br>
                    Примеры: 2023, 2022-2024, 2021,2023-2025
                </div>
                """, unsafe_allow_html=True)
                return
            
            st.session_state.orig_years = orig_years_list
            st.session_state.exp_years = get_expanded_years(orig_years_list)
            
            # Run analysis
            with st.spinner("Сбор данных..."):
                # Collect DOIs
                st.markdown("""
                <div class="info-box fade-in">
                    📥 Сбор DOI из OpenAlex...
                </div>
                """, unsafe_allow_html=True)
                
                dois, err = fetch_all_dois_openalex(
                    st.session_state.selected_ror, 
                    st.session_state.exp_years
                )
                
                if err:
                    st.markdown(f"""
                    <div class="error-box fade-in">
                        ❌ Ошибка: {err}
                    </div>
                    """, unsafe_allow_html=True)
                    return
                
                if not dois:
                    st.markdown("""
                    <div class="warning-box fade-in">
                        ⚠️ Не найдено публикаций с DOI за указанный период
                    </div>
                    """, unsafe_allow_html=True)
                    return
                
                st.markdown(f"""
                <div class="success-box fade-in">
                    ✅ Найдено {len(dois):,} уникальных DOI
                </div>
                """, unsafe_allow_html=True)
                
                st.session_state.dois_list = dois
                
                # Process DOIs
                st.markdown("""
                <div class="info-box fade-in">
                    🔍 Обработка данных из Crossref и OpenAlex...
                </div>
                """, unsafe_allow_html=True)
                
                results, errors = process_dois_parallel(dois, st.session_state.selected_ror)
                
                st.session_state.results_list = results
                st.session_state.errors_list = errors
                
                if results:
                    st.session_state.results_df = create_results_dataframe(
                        results, 
                        set(st.session_state.orig_years)
                    )
                    
                    st.markdown(f"""
                    <div class="success-box fade-in">
                        ✅ Успешно обработано: {len(results):,}<br>
                        ❌ Ошибок: {len(errors):,}
                    </div>
                    """, unsafe_allow_html=True)
                    
                    st.rerun()
    
    # Display results if available
    if st.session_state.results_df is not None and not st.session_state.results_df.empty:
        df = st.session_state.results_df
        belong = df[df['belongs_to_period'] == True].copy()
        not_belong = df[df['belongs_to_period'] == False].copy()
        
        # Summary metrics
        st.markdown("## 📊 Общая статистика")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.markdown("""
            <div class="metric-container">
                <div class="metric-value">{}</div>
                <div class="metric-label">Всего обработано</div>
            </div>
            """.format(len(df)), unsafe_allow_html=True)
        
        with col2:
            st.markdown("""
            <div class="metric-container">
                <div class="metric-value">{}</div>
                <div class="metric-label">В целевом периоде</div>
            </div>
            """.format(len(belong)), unsafe_allow_html=True)
        
        with col3:
            st.markdown("""
            <div class="metric-container">
                <div class="metric-value">{:.1f}%</div>
                <div class="metric-label">Open Access</div>
            </div>
            """.format(belong['is_oa'].mean() * 100 if not belong.empty else 0), unsafe_allow_html=True)
        
        with col4:
            st.markdown("""
            <div class="metric-container">
                <div class="metric-value">{:.1f}</div>
                <div class="metric-label">Ср. цитирований</div>
            </div>
            """.format(belong['citations_cr'].mean() if not belong.empty else 0), unsafe_allow_html=True)
        
        # Tabs for different visualizations
        tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
            "📋 Данные", 
            "📈 Графики", 
            "🌍 Сети",
            "📊 IF/CS анализ",
            "📑 Статистика",
            "❌ Ошибки"
        ])
        
        with tab1:
            st.markdown("### 📋 Данные публикаций")
            
            # Filter by period
            show_period_only = st.checkbox("Только целевой период", value=True)
            display_df = belong if show_period_only else df
            
            st.dataframe(
                display_df,
                use_container_width=True,
                height=500
            )
            
            # Download buttons
            col1, col2 = st.columns(2)
            
            with col1:
                # Excel download
                excel_output, excel_filename = export_to_excel(
                    df,
                    st.session_state.results_list,
                    st.session_state.errors_list,
                    st.session_state.selected_ror,
                    st.session_state.orig_years,
                    st.session_state.exp_years
                )
                
                st.download_button(
                    label="📥 Скачать Excel",
                    data=excel_output,
                    file_name=excel_filename,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )
            
            with col2:
                # CSV download
                csv = display_df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="📥 Скачать CSV",
                    data=csv,
                    file_name=f"data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    use_container_width=True
                )
        
        with tab2:
            st.markdown("### 📈 Визуализация данных")
            
            # Create two columns for plots
            col1, col2 = st.columns(2)
            
            with col1:
                # Publication timeline
                fig1 = create_publication_timeline(belong, st.session_state.palette)
                if fig1:
                    st.pyplot(fig1)
                    plt.close(fig1)
                
                # Top journals
                fig3 = create_top_journals_plot(belong, st.session_state.palette)
                if fig3:
                    st.pyplot(fig3)
                    plt.close(fig3)
                
                # Publication types
                fig6 = create_publication_types_plot(belong, st.session_state.palette)
                if fig6:
                    st.pyplot(fig6)
                    plt.close(fig6)
                
                # Funding distribution
                fig8 = create_funding_distribution_plot(belong, st.session_state.palette)
                if fig8:
                    st.pyplot(fig8)
                    plt.close(fig8)
            
            with col2:
                # Citation distribution
                fig2 = create_citation_distribution(belong, st.session_state.palette)
                if fig2:
                    st.pyplot(fig2)
                    plt.close(fig2)
                
                # OA trend
                fig4 = create_oa_trend(belong, st.session_state.palette)
                if fig4:
                    st.pyplot(fig4)
                    plt.close(fig4)
                
                # Language distribution
                fig7 = create_language_distribution_plot(belong, st.session_state.palette)
                if fig7:
                    st.pyplot(fig7)
                    plt.close(fig7)
                
                # References vs Citations
                fig5 = create_references_vs_citations_plot(belong, st.session_state.palette)
                if fig5:
                    st.pyplot(fig5)
                    plt.close(fig5)
        
        with tab3:
            st.markdown("### 🌍 Сетевой анализ")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("#### 🏢 Сеть аффилиаций")
                fig_aff = create_affiliation_network_plot(belong, st.session_state.palette)
                if fig_aff:
                    st.pyplot(fig_aff)
                    plt.close(fig_aff)
                else:
                    st.info("Недостаточно данных для построения сети аффилиаций")
            
            with col2:
                st.markdown("#### 🌍 Сеть стран")
                fig_country = create_country_network_plot(belong, st.session_state.palette)
                if fig_country:
                    st.pyplot(fig_country)
                    plt.close(fig_country)
                else:
                    st.info("Недостаточно данных для построения сети стран")
            
            # Author collaboration matrix
            st.markdown("#### 👥 Матрица соавторства")
            fig_auth = create_author_collaboration_plot(belong, st.session_state.palette)
            if fig_auth:
                st.pyplot(fig_auth)
                plt.close(fig_auth)
            else:
                st.info("Недостаточно данных для построения матрицы соавторства")
            
            # Country map
            st.markdown("#### 🗺️ Географическое распределение")
            fig_map = create_country_map(belong, st.session_state.palette)
            if fig_map:
                st.plotly_chart(fig_map, use_container_width=True)
            else:
                st.info("Недостаточно данных для построения карты")
        
        with tab4:
            st.markdown("### 📊 Анализ Impact Factor и CiteScore")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("#### 📈 Impact Factor")
                if 'IF' in belong.columns:
                    if_data = belong['IF'].dropna()
                    if not if_data.empty:
                        st.metric("Средний IF", f"{if_data.mean():.3f}")
                        st.metric("Медианный IF", f"{if_data.median():.3f}")
                        st.metric("Максимальный IF", f"{if_data.max():.3f}")
                        
                        # IF distribution
                        fig_if, ax = plt.subplots(figsize=(10, 6))
                        ax.hist(if_data, bins=20, color=st.session_state.palette[0], alpha=0.7, edgecolor='black')
                        ax.set_xlabel('Impact Factor')
                        ax.set_ylabel('Frequency')
                        ax.set_title('Distribution of Impact Factors')
                        ax.grid(True, alpha=0.3)
                        st.pyplot(fig_if)
                        plt.close(fig_if)
                    else:
                        st.info("Нет данных по Impact Factor")
                else:
                    st.info("Нет данных по Impact Factor")
            
            with col2:
                st.markdown("#### 📊 CiteScore")
                if 'CS' in belong.columns:
                    cs_data = belong['CS'].dropna()
                    if not cs_data.empty:
                        st.metric("Средний CiteScore", f"{cs_data.mean():.3f}")
                        st.metric("Медианный CiteScore", f"{cs_data.median():.3f}")
                        st.metric("Максимальный CiteScore", f"{cs_data.max():.3f}")
                        
                        # CS distribution
                        fig_cs, ax = plt.subplots(figsize=(10, 6))
                        ax.hist(cs_data, bins=20, color=st.session_state.palette[1], alpha=0.7, edgecolor='black')
                        ax.set_xlabel('CiteScore')
                        ax.set_ylabel('Frequency')
                        ax.set_title('Distribution of CiteScores')
                        ax.grid(True, alpha=0.3)
                        st.pyplot(fig_cs)
                        plt.close(fig_cs)
                    else:
                        st.info("Нет данных по CiteScore")
                else:
                    st.info("Нет данных по CiteScore")
            
            # Quartile distribution
            st.markdown("#### 📊 Распределение по квартилям")
            fig_quartile = create_quartile_distribution_plot(belong, st.session_state.palette)
            if fig_quartile:
                st.pyplot(fig_quartile)
                plt.close(fig_quartile)
        
        with tab5:
            st.markdown("### 📑 Детальная статистика")
            
            # Author frequency
            st.markdown("#### 👥 Топ авторов")
            author_counter = generate_author_frequency(belong)
            author_df = pd.DataFrame(author_counter.most_common(20), columns=['Author', 'Papers'])
            st.dataframe(author_df, use_container_width=True)
            
            # Journal frequency
            st.markdown("#### 📚 Топ журналов")
            journal_freq = generate_journal_frequency(belong).head(20)
            journal_df = pd.DataFrame({
                'Journal': journal_freq.index,
                'Papers': journal_freq.values
            })
            st.dataframe(journal_df, use_container_width=True)
            
            # Publisher frequency
            st.markdown("#### 🏢 Топ издателей")
            publisher_freq = generate_publisher_frequency(belong).head(20)
            publisher_df = pd.DataFrame({
                'Publisher': publisher_freq.index,
                'Papers': publisher_freq.values
            })
            st.dataframe(publisher_df, use_container_width=True)
            
            # Country frequency
            st.markdown("#### 🌍 Топ стран")
            country_counter = generate_country_frequency(belong)
            country_df = pd.DataFrame(country_counter.most_common(20), columns=['Country', 'Papers'])
            st.dataframe(country_df, use_container_width=True)
        
        with tab6:
            st.markdown("### ❌ Ошибки обработки")
            
            if st.session_state.errors_list:
                errors_df = pd.DataFrame({
                    'DOI': st.session_state.errors_list,
                    'Status': 'Failed'
                })
                st.dataframe(errors_df, use_container_width=True)
                
                # Download errors
                errors_csv = errors_df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="📥 Скачать список ошибок",
                    data=errors_csv,
                    file_name=f"errors_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
            else:
                st.success("✅ Ошибок не обнаружено")

if __name__ == "__main__":
    main()
