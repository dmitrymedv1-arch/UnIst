# app.py
import streamlit as st
import requests
import pandas as pd
import numpy as np
import re
from datetime import datetime
import time
import json
import hashlib
import os
from unidecode import unidecode
from rapidfuzz import fuzz, process
import pickle
from collections import defaultdict, Counter
import logging
from io import BytesIO
import sys
import networkx as nx
import matplotlib.pyplot as plt
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import base64
from dateutil.parser import parse as date_parse
import concurrent.futures
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import random

# Научный стиль для графиков matplotlib
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
    'ytick.minor.visible': True,
    'ytick.direction': 'out',
    'xtick.major.size': 4,
    'xtick.minor.size': 2,
    'ytick.major.size': 4,
    'ytick.minor.size': 2,
    'xtick.major.width': 0.8,
    'ytick.minor.width': 0.6,
    'ytick.major.width': 0.8,
    'ytick.minor.width': 0.6,
    
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

# Случайная цветовая палитра для интерфейса
COLOR_PALETTES = [
    {
        'primary': '#FF6B6B',
        'secondary': '#4ECDC4',
        'accent': '#45B7D1',
        'background': '#2C3E50',
        'text': '#FFFFFF',
        'success': '#95E1D3',
        'warning': '#F8A5C2',
        'info': '#B8E994'
    },
    {
        'primary': '#6C5B7B',
        'secondary': '#F8A5C2',
        'accent': '#F5CD79',
        'background': '#2A363B',
        'text': '#FFFFFF',
        'success': '#99B898',
        'warning': '#FECEAB',
        'info': '#E84A5F'
    },
    {
        'primary': '#2C3E50',
        'secondary': '#E74C3C',
        'accent': '#3498DB',
        'background': '#1A2634',
        'text': '#ECF0F1',
        'success': '#2ECC71',
        'warning': '#F39C12',
        'info': '#9B59B6'
    },
    {
        'primary': '#16A085',
        'secondary': '#F39C12',
        'accent': '#E74C3C',
        'background': '#2C3E50',
        'text': '#FFFFFF',
        'success': '#27AE60',
        'warning': '#F1C40F',
        'info': '#8E44AD'
    },
    {
        'primary': '#D35400',
        'secondary': '#1ABC9C',
        'accent': '#3498DB',
        'background': '#2C3E50',
        'text': '#FFFFFF',
        'success': '#27AE60',
        'warning': '#F39C12',
        'info': '#9B59B6'
    },
    {
        'primary': '#8E44AD',
        'secondary': '#E67E22',
        'accent': '#16A085',
        'background': '#2C3E50',
        'text': '#FFFFFF',
        'success': '#2ECC71',
        'warning': '#F1C40F',
        'info': '#E74C3C'
    },
    {
        'primary': '#2980B9',
        'secondary': '#E74C3C',
        'accent': '#F39C12',
        'background': '#1A2634',
        'text': '#ECF0F1',
        'success': '#27AE60',
        'warning': '#F1C40F',
        'info': '#8E44AD'
    },
    {
        'primary': '#27AE60',
        'secondary': '#E67E22',
        'accent': '#3498DB',
        'background': '#2C3E50',
        'text': '#FFFFFF',
        'success': '#16A085',
        'warning': '#F39C12',
        'info': '#9B59B6'
    },
    {
        'primary': '#E67E22',
        'secondary': '#8E44AD',
        'accent': '#16A085',
        'background': '#2C3E50',
        'text': '#FFFFFF',
        'success': '#27AE60',
        'warning': '#F1C40F',
        'info': '#3498DB'
    },
    {
        'primary': '#95A5A6',
        'secondary': '#E74C3C',
        'accent': '#3498DB',
        'background': '#2C3E50',
        'text': '#FFFFFF',
        'success': '#2ECC71',
        'warning': '#F39C12',
        'info': '#9B59B6'
    }
]

# Случайный выбор палитры при запуске
if 'color_palette' not in st.session_state:
    st.session_state.color_palette = random.choice(COLOR_PALETTES)

# Конфигурация страницы
st.set_page_config(
    page_title="UnIst Analytics",
    page_icon="📚",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Загрузка логотипа
def get_logo_html():
    if os.path.exists("logo.png"):
        with open("logo.png", "rb") as f:
            logo_data = base64.b64encode(f.read()).decode()
        return f'<img src="data:image/png;base64,{logo_data}" style="height: 60px; margin-right: 10px;">'
    return ""

# Кастомный CSS с динамической цветовой палитрой
def get_custom_css():
    colors = st.session_state.color_palette
    return f"""
    <style>
    /* Main container */
    .main {{
        background-color: {colors['background']};
        color: {colors['text']};
    }}
    
    /* Headers */
    h1, h2, h3 {{
        color: {colors['primary']} !important;
        font-weight: bold !important;
        border-bottom: 2px solid {colors['secondary']};
        padding-bottom: 10px;
    }}
    
    /* Buttons */
    .stButton > button {{
        background-color: {colors['primary']};
        color: white;
        border: none;
        border-radius: 25px;
        padding: 10px 25px;
        font-weight: bold;
        transition: all 0.3s ease;
        border: 2px solid {colors['secondary']};
    }}
    
    .stButton > button:hover {{
        background-color: {colors['secondary']};
        color: {colors['background']};
        transform: translateY(-2px);
        box-shadow: 0 5px 15px rgba(0,0,0,0.3);
    }}
    
    /* Input fields */
    .stTextInput > div > div > input {{
        border-radius: 20px;
        border: 2px solid {colors['secondary']};
        background-color: rgba(255,255,255,0.1);
        color: {colors['text']};
        padding: 10px 20px;
    }}
    
    .stTextInput > div > div > input:focus {{
        border-color: {colors['accent']};
        box-shadow: 0 0 0 2px {colors['accent']}40;
    }}
    
    /* Dropdown */
    .stSelectbox > div > div {{
        background-color: rgba(255,255,255,0.1);
        border-radius: 20px;
        border: 2px solid {colors['secondary']};
    }}
    
    /* Progress bar */
    .stProgress > div > div {{
        background-color: {colors['accent']};
    }}
    
    /* Metrics */
    .stMetric {{
        background: linear-gradient(135deg, {colors['primary']}20, {colors['secondary']}20);
        border-radius: 15px;
        padding: 20px;
        border: 2px solid {colors['accent']};
        transition: all 0.3s ease;
    }}
    
    .stMetric:hover {{
        transform: translateY(-5px);
        box-shadow: 0 5px 20px rgba(0,0,0,0.2);
    }}
    
    /* Tabs */
    .stTabs [data-baseweb="tab-list"] {{
        gap: 10px;
        background-color: rgba(255,255,255,0.05);
        border-radius: 30px;
        padding: 10px;
    }}
    
    .stTabs [data-baseweb="tab"] {{
        border-radius: 20px;
        padding: 10px 25px;
        background-color: transparent;
        color: {colors['text']};
        font-weight: bold;
        transition: all 0.3s ease;
    }}
    
    .stTabs [aria-selected="true"] {{
        background-color: {colors['primary']} !important;
        color: white !important;
    }}
    
    /* Dataframes */
    .stDataFrame {{
        border: 2px solid {colors['secondary']};
        border-radius: 15px;
        overflow: hidden;
    }}
    
    .stDataFrame th {{
        background-color: {colors['primary']};
        color: white;
        font-weight: bold;
    }}
    
    .stDataFrame td {{
        background-color: rgba(255,255,255,0.05);
    }}
    
    /* Success/Warning/Info boxes */
    .stSuccess {{
        background-color: {colors['success']}20;
        border-left-color: {colors['success']};
    }}
    
    .stWarning {{
        background-color: {colors['warning']}20;
        border-left-color: {colors['warning']};
    }}
    
    .stInfo {{
        background-color: {colors['info']}20;
        border-left-color: {colors['info']};
    }}
    
    /* Sidebar */
    .css-1d391kg {{
        background-color: {colors['background']}dd;
    }}
    
    /* Animations */
    @keyframes pulse {{
        0% {{ transform: scale(1); }}
        50% {{ transform: scale(1.05); }}
        100% {{ transform: scale(1); }}
    }}
    
    .pulse {{
        animation: pulse 2s infinite;
    }}
    
    /* Tooltips */
    .tooltip {{
        position: relative;
        display: inline-block;
        border-bottom: 1px dotted {colors['accent']};
    }}
    
    .tooltip .tooltiptext {{
        visibility: hidden;
        width: 120px;
        background-color: {colors['background']};
        color: {colors['text']};
        text-align: center;
        border-radius: 6px;
        padding: 5px;
        position: absolute;
        z-index: 1;
        bottom: 125%;
        left: 50%;
        margin-left: -60px;
        opacity: 0;
        transition: opacity 0.3s;
        border: 2px solid {colors['accent']};
    }}
    
    .tooltip:hover .tooltiptext {{
        visibility: visible;
        opacity: 1;
    }}
    </style>
    """

# Применяем кастомный CSS
st.markdown(get_custom_css(), unsafe_allow_html=True)

# Заголовок с логотипом
logo_html = get_logo_html()
st.markdown(f"""
<div style="display: flex; align-items: center; margin-bottom: 20px;">
    {logo_html}
    <h1 style="margin: 0;">UnIst Analytics</h1>
</div>
<div style="text-align: right; margin-bottom: 20px;">
    <span class="badge" style="background-color: {st.session_state.color_palette['secondary']}; color: white; padding: 5px 15px; border-radius: 20px;">
        Advanced Scientific Publication Analyzer
    </span>
</div>
""", unsafe_allow_html=True)

# ─── CONFIGURATION ────────────────────────────────────────────────────────────

CACHE_DIR = "cache"
LOG_DIR = "logs"
CROSSREF_EMAIL = "your.email@example.com"  # Replace with your email
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

# ─── INITIALIZE SESSION STATE ────────────────────────────────────────────

if 'selected_ror' not in st.session_state:
    st.session_state.selected_ror = None
if 'org_search_results' not in st.session_state:
    st.session_state.org_search_results = None
if 'analysis_complete' not in st.session_state:
    st.session_state.analysis_complete = False
if 'results_df' not in st.session_state:
    st.session_state.results_df = None
if 'errors_list' not in st.session_state:
    st.session_state.errors_list = []
if 'dois_list' not in st.session_state:
    st.session_state.dois_list = []
if 'orig_years_list' not in st.session_state:
    st.session_state.orig_years_list = []
if 'exp_years' not in st.session_state:
    st.session_state.exp_years = []
if 'if_data' not in st.session_state:
    st.session_state.if_data = None
if 'cs_data' not in st.session_state:
    st.session_state.cs_data = None
if 'issn_mapping' not in st.session_state:
    st.session_state.issn_mapping = {}

# ─── CACHING ───────────────────────────────────────────────────────────

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

# ─── ISSN NORMALIZATION AND MAPPING ────────────────────────────────────────

@st.cache_data
def load_if_data():
    """Load IF.xlsx file with caching"""
    if os.path.exists("IF.xlsx"):
        df = pd.read_excel("IF.xlsx")
        # Normalize ISSN columns
        if 'ISSN' in df.columns:
            df['ISSN_norm'] = df['ISSN'].astype(str).apply(normalize_issn)
        if 'eISSN' in df.columns:
            df['eISSN_norm'] = df['eISSN'].astype(str).apply(normalize_issn)
        return df
    return None

@st.cache_data
def load_cs_data():
    """Load CS.xlsx file with caching"""
    if os.path.exists("CS.xlsx"):
        df = pd.read_excel("CS.xlsx")
        # Normalize ISSN columns
        if 'Print ISSN' in df.columns:
            df['Print_ISSN_norm'] = df['Print ISSN'].astype(str).apply(normalize_issn)
        if 'E-ISSN' in df.columns:
            df['E-ISSN_norm'] = df['E-ISSN'].astype(str).apply(normalize_issn)
        
        # Process quartiles - take the highest quartile for each ISSN
        result_rows = []
        for _, row in df.iterrows():
            # Handle multiple quartiles - take the best (smallest number)
            if pd.notna(row.get('Quartile')):
                quartile_val = str(row['Quartile']).strip()
                if quartile_val.isdigit():
                    quartile_num = int(quartile_val)
                    # Add to processing list
                    result_rows.append({
                        'Title': row.get('Title', ''),
                        'Print_ISSN_norm': row.get('Print_ISSN_norm', ''),
                        'E-ISSN_norm': row.get('E-ISSN_norm', ''),
                        'CiteScore': row.get('CiteScore', ''),
                        'Quartile_raw': quartile_num
                    })
        
        # Group by ISSN and take min quartile number
        df_result = pd.DataFrame(result_rows)
        if not df_result.empty:
            # Group by Print ISSN
            grouped_print = df_result.groupby('Print_ISSN_norm').agg({
                'Title': 'first',
                'CiteScore': 'first',
                'Quartile_raw': 'min'
            }).reset_index()
            
            # Group by E-ISSN
            grouped_e = df_result.groupby('E-ISSN_norm').agg({
                'Title': 'first',
                'CiteScore': 'first',
                'Quartile_raw': 'min'
            }).reset_index()
            
            # Combine
            combined = pd.concat([grouped_print, grouped_e]).drop_duplicates()
            combined['Quartile'] = combined['Quartile_raw'].apply(lambda x: f"Q{x}" if pd.notna(x) else "")
            return combined
        return df
    return None

def normalize_issn(issn_str):
    """
    Normalize ISSN to format XXXX-XXXX
    Examples:
    24510769 -> 2451-0769
    5912385 -> 0591-2385
    445401 -> 0044-5401
    """
    if pd.isna(issn_str) or not issn_str:
        return ""
    
    # Convert to string and remove any non-digit characters
    issn_clean = re.sub(r'\D', '', str(issn_str))
    
    if not issn_clean:
        return ""
    
    # Pad with leading zeros to make 8 digits
    issn_padded = issn_clean.zfill(8)
    
    # Format as XXXX-XXXX
    if len(issn_padded) == 8:
        return f"{issn_padded[:4]}-{issn_padded[4:]}"
    
    return ""

@st.cache_data
def create_issn_mapping(if_df, cs_df):
    """Create mapping from ISSN to IF and CiteScore data"""
    mapping = {}
    
    # Process IF data
    if if_df is not None:
        for _, row in if_df.iterrows():
            # Process ISSN
            if pd.notna(row.get('ISSN_norm')):
                issn = row['ISSN_norm']
                if issn:
                    mapping[issn] = {
                        'if': mapping.get(issn, {}).get('if', row.get('IF', '')),
                        'if_quartile': mapping.get(issn, {}).get('if_quartile', row.get('Quartile', '')),
                        'if_name': mapping.get(issn, {}).get('if_name', row.get('Name', ''))
                    }
            
            # Process eISSN
            if pd.notna(row.get('eISSN_norm')):
                eissn = row['eISSN_norm']
                if eissn:
                    mapping[eissn] = {
                        'if': mapping.get(eissn, {}).get('if', row.get('IF', '')),
                        'if_quartile': mapping.get(eissn, {}).get('if_quartile', row.get('Quartile', '')),
                        'if_name': mapping.get(eissn, {}).get('if_name', row.get('Name', ''))
                    }
    
    # Process CS data
    if cs_df is not None:
        for _, row in cs_df.iterrows():
            # Process Print ISSN
            if pd.notna(row.get('Print_ISSN_norm')):
                issn = row['Print_ISSN_norm']
                if issn:
                    mapping[issn] = {
                        **mapping.get(issn, {}),
                        'cs': mapping.get(issn, {}).get('cs', row.get('CiteScore', '')),
                        'cs_quartile': mapping.get(issn, {}).get('cs_quartile', row.get('Quartile', '')),
                        'cs_title': mapping.get(issn, {}).get('cs_title', row.get('Title', ''))
                    }
            
            # Process E-ISSN
            if pd.notna(row.get('E-ISSN_norm')):
                eissn = row['E-ISSN_norm']
                if eissn:
                    mapping[eissn] = {
                        **mapping.get(eissn, {}),
                        'cs': mapping.get(eissn, {}).get('cs', row.get('CiteScore', '')),
                        'cs_quartile': mapping.get(eissn, {}).get('cs_quartile', row.get('Quartile', '')),
                        'cs_title': mapping.get(eissn, {}).get('cs_title', row.get('Title', ''))
                    }
    
    return mapping

# Load IF and CS data
if 'if_df' not in st.session_state:
    st.session_state.if_df = load_if_data()
if 'cs_df' not in st.session_state:
    st.session_state.cs_df = load_cs_data()

# Create ISSN mapping
if st.session_state.if_df is not None or st.session_state.cs_df is not None:
    if 'issn_mapping' not in st.session_state or not st.session_state.issn_mapping:
        st.session_state.issn_mapping = create_issn_mapping(st.session_state.if_df, st.session_state.cs_df)

# ─── ORGANIZATION NAME NORMALIZATION ─────────────────────────────────────

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

# ─── ORGANIZATION SEARCH ────────────────────────────────────────

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

# ─── YEAR PARSING ───────────────────────────────────────────────────────

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

def is_date_in_original_period(dt, original_years_set):
    """Check if date falls in original period"""
    return dt is not None and dt.year in original_years_set

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

# ─── DATA COLLECTION ───────────────────────────────────────────────

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
            'issn': issn_str,
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
                status_text.text(f"📥 Collected {len(all_dois)} DOIs from {total_expected}")
            
            cursor = data["meta"].get("next_cursor")
            if cursor:
                time.sleep(RATE_LIMIT_DELAY)
            
        except Exception as e:
            logger.error(f"Error at cursor {cursor}: {e}")
            progress_bar.empty()
            status_text.empty()
            return all_dois, f"Error: {str(e)}"
    
    progress_bar.empty()
    status_text.empty()
    
    all_dois = list(dict.fromkeys(all_dois))
    logger.info(f"Unique DOIs collected: {len(all_dois)}")
    
    return all_dois, None

# ─── PARALLEL DOI PROCESSING ────────────────────────────────────────────

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
    
    # Setup progress tracking
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    total_processed = 0
    total_dois = len(dois)
    
    while remaining_dois and attempt <= max_attempts:
        logger.info(f"Attempt {attempt}/{max_attempts}, remaining DOIs: {len(remaining_dois)}")
        
        current_results = []
        current_errors = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_doi = {executor.submit(process_doi_complete, doi, target_ror): doi 
                            for doi in remaining_dois}
            
            completed = 0
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
                total_processed += 1
                
                # Update progress
                progress = total_processed / total_dois
                progress_bar.progress(progress)
                status_text.text(f"🔍 Processing DOIs: {total_processed}/{total_dois} (Attempt {attempt})")
        
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

# ─── ANALYSIS FUNCTIONS ────────────────────────────────────────

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

def add_issn_metrics_to_df(df, issn_mapping):
    """Add IF and CiteScore metrics to dataframe based on ISSN"""
    
    def get_metrics(issn_str):
        if pd.isna(issn_str) or not issn_str:
            return pd.Series({
                'IF': '',
                'IF_Q': '',
                'CS': '',
                'CS_Q': ''
            })
        
        # Split multiple ISSNs
        issns = [i.strip() for i in str(issn_str).split(';')]
        
        if_vals = []
        if_q_vals = []
        cs_vals = []
        cs_q_vals = []
        
        for issn in issns:
            # Normalize ISSN for lookup
            issn_norm = normalize_issn(issn)
            if issn_norm in issn_mapping:
                mapping = issn_mapping[issn_norm]
                
                if 'if' in mapping and mapping['if']:
                    if_vals.append(str(mapping['if']))
                if 'if_quartile' in mapping and mapping['if_quartile']:
                    if_q_vals.append(str(mapping['if_quartile']))
                if 'cs' in mapping and mapping['cs']:
                    cs_vals.append(str(mapping['cs']))
                if 'cs_quartile' in mapping and mapping['cs_quartile']:
                    cs_q_vals.append(str(mapping['cs_quartile']))
        
        # Take first non-empty value
        if_val = if_vals[0] if if_vals else ''
        if_q_val = if_q_vals[0] if if_q_vals else ''
        cs_val = cs_vals[0] if cs_vals else ''
        cs_q_val = cs_q_vals[0] if cs_q_vals else ''
        
        return pd.Series({
            'IF': if_val,
            'IF_Q': if_q_val,
            'CS': cs_val,
            'CS_Q': cs_q_val
        })
    
    # Apply the function to create new columns
    metrics_df = df['issn'].apply(get_metrics)
    df = pd.concat([df, metrics_df], axis=1)
    
    return df

# ─── EXCEL EXPORT ────────────────────────────────────────

def export_to_excel(df, results, errors, selected_ror, orig_years_list, exp_years, 
                    filename=None):
    """
    Export all data and analytics to Excel with multiple sheets
    """
    if filename is None:
        filename = f"analysis_ror_{selected_ror}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    
    # Create Excel writer with xlsxwriter
    writer = pd.ExcelWriter(filename, engine='xlsxwriter')
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
    
    # ===== SHEET 10: Network Statistics (Affiliations) =====
    G_aff, aff_stats = create_affiliation_network(df)
    
    aff_network_data = []
    for aff, stats in aff_stats.items():
        aff_network_data.append({
            'Affiliation': aff,
            'Papers': stats['papers'],
            'Countries': ', '.join(stats['countries']),
            'Degree': G_aff.degree(aff) if aff in G_aff else 0
        })
    
    aff_network_df = pd.DataFrame(aff_network_data)
    if not aff_network_df.empty:
        aff_network_df = aff_network_df.sort_values('Papers', ascending=False)
        aff_network_df.to_excel(writer, sheet_name='Affiliation Network', index=False)
    
    # ===== SHEET 11: Network Statistics (Countries) =====
    G_country, country_stats = create_country_network(df)
    
    country_network_data = []
    for country, stats in country_stats.items():
        country_network_data.append({
            'Country': country,
            'Papers': stats['papers'],
            'Affiliations': len(stats['affiliations']),
            'Collaboration Partners': G_country.degree(country) if country in G_country else 0
        })
    
    country_network_df = pd.DataFrame(country_network_data)
    if not country_network_df.empty:
        country_network_df = country_network_df.sort_values('Papers', ascending=False)
        country_network_df.to_excel(writer, sheet_name='Country Network', index=False)
    
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
    
    # Auto-adjust column widths for all sheets
    for sheet_name in writer.sheets:
        worksheet = writer.sheets[sheet_name]
        worksheet.set_column(0, 100, 20)
    
    # Save the file
    writer.close()
    
    return filename

def create_enhanced_visualizations(df):
    """Create enhanced scientific visualizations"""
    colors = st.session_state.color_palette
    
    # Filter data for period
    belong = df[df['belongs_to_period'] == True].copy()
    
    if belong.empty:
        return None
    
    figs = {}
    
    # 1. Publications over time
    fig1, ax1 = plt.subplots(figsize=(10, 6))
    year_counts = belong['late_year'].value_counts().sort_index()
    ax1.bar(year_counts.index, year_counts.values, 
            color=colors['primary'], edgecolor='black', linewidth=1)
    ax1.set_xlabel('Year', fontweight='bold')
    ax1.set_ylabel('Number of Publications', fontweight='bold')
    ax1.set_title('Publication Timeline', fontweight='bold', pad=20)
    ax1.grid(True, alpha=0.3, linestyle='--')
    plt.xticks(rotation=45)
    plt.tight_layout()
    figs['timeline'] = fig1
    
    # 2. Top journals
    fig2, ax2 = plt.subplots(figsize=(10, 6))
    journal_counts = belong['journal'].value_counts().head(10)
    ax2.barh(range(len(journal_counts)), journal_counts.values, 
             color=colors['secondary'], edgecolor='black', linewidth=1)
    ax2.set_yticks(range(len(journal_counts)))
    ax2.set_yticklabels([j[:30] + '...' if len(j) > 30 else j for j in journal_counts.index])
    ax2.set_xlabel('Number of Publications', fontweight='bold')
    ax2.set_title('Top 10 Journals', fontweight='bold', pad=20)
    ax2.grid(True, alpha=0.3, linestyle='--', axis='x')
    plt.tight_layout()
    figs['journals'] = fig2
    
    # 3. Citation distribution
    fig3, (ax3, ax4) = plt.subplots(1, 2, figsize=(12, 5))
    
    # Crossref citations
    cr_citations = belong['citations_cr'].dropna()
    if not cr_citations.empty:
        ax3.hist(cr_citations, bins=30, color=colors['primary'], 
                edgecolor='black', alpha=0.7)
        ax3.set_xlabel('Citations (Crossref)', fontweight='bold')
        ax3.set_ylabel('Frequency', fontweight='bold')
        ax3.set_title('Crossref Citation Distribution', fontweight='bold')
        ax3.grid(True, alpha=0.3, linestyle='--')
    
    # OpenAlex citations
    oa_citations = belong['citations_oa'].dropna()
    if not oa_citations.empty:
        ax4.hist(oa_citations, bins=30, color=colors['secondary'], 
                edgecolor='black', alpha=0.7)
        ax4.set_xlabel('Citations (OpenAlex)', fontweight='bold')
        ax4.set_ylabel('Frequency', fontweight='bold')
        ax4.set_title('OpenAlex Citation Distribution', fontweight='bold')
        ax4.grid(True, alpha=0.3, linestyle='--')
    
    plt.tight_layout()
    figs['citations'] = fig3
    
    # 4. Country collaboration heatmap
    country_counter = generate_country_frequency(df)
    if len(country_counter) > 1:
        fig4, ax5 = plt.subplots(figsize=(10, 8))
        
        # Create country co-occurrence matrix
        countries_list = []
        for countries_str in belong['countries'].dropna():
            countries = extract_countries(countries_str)
            if len(countries) > 1:
                countries_list.extend([(c1, c2) for c1 in countries for c2 in countries if c1 < c2])
        
        if countries_list:
            # Create matrix
            top_countries = [c for c, _ in country_counter.most_common(10)]
            matrix = np.zeros((len(top_countries), len(top_countries)))
            
            for c1, c2 in countries_list:
                if c1 in top_countries and c2 in top_countries:
                    i, j = top_countries.index(c1), top_countries.index(c2)
                    matrix[i, j] += 1
                    matrix[j, i] += 1
            
            im = ax5.imshow(matrix, cmap='YlOrRd', aspect='auto')
            ax5.set_xticks(range(len(top_countries)))
            ax5.set_yticks(range(len(top_countries)))
            ax5.set_xticklabels(top_countries, rotation=45, ha='right')
            ax5.set_yticklabels(top_countries)
            ax5.set_title('Country Collaboration Heatmap', fontweight='bold', pad=20)
            plt.colorbar(im, ax=ax5, label='Collaboration Frequency')
            plt.tight_layout()
            figs['countries'] = fig4
    
    # 5. Open Access trend
    if 'late_year' in belong.columns and 'is_oa' in belong.columns:
        fig5, ax6 = plt.subplots(figsize=(10, 6))
        oa_by_year = belong.groupby('late_year')['is_oa'].mean() * 100
        ax6.plot(oa_by_year.index, oa_by_year.values, marker='o', 
                color=colors['accent'], linewidth=2, markersize=8)
        ax6.fill_between(oa_by_year.index, oa_by_year.values, alpha=0.3, color=colors['accent'])
        ax6.set_xlabel('Year', fontweight='bold')
        ax6.set_ylabel('Open Access (%)', fontweight='bold')
        ax6.set_title('Open Access Trend Over Time', fontweight='bold', pad=20)
        ax6.grid(True, alpha=0.3, linestyle='--')
        ax6.set_ylim(0, 100)
        plt.tight_layout()
        figs['oa_trend'] = fig5
    
    return figs

def create_results_dataframe(results, target_years_set):
    """Create final DataFrame with results"""
    
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
    
    # Add IF and CiteScore metrics
    if st.session_state.issn_mapping:
        df = add_issn_metrics_to_df(df, st.session_state.issn_mapping)
    
    column_order = [
        'doi', 'title', 'belongs_to_period', 'late_date', 'late_year',
        'print_date', 'online_date', 'publication_year', 'publication_date',
        'authors', 'authors_count', 'orcids',
        'affiliations', 'countries', 'journal', 'issn', 'publisher', 'type',
        'volume', 'issue', 'pages', 'IF', 'IF_Q', 'CS', 'CS_Q',
        'is_oa', 'funding',
        'references_count', 'citations_cr', 'citations_oa',
        'openalex_id', 'language', 'status'
    ]
    
    existing_cols = [col for col in column_order if col in df.columns]
    df = df[existing_cols]
    
    return df

# ─── STREAMLIT UI ────────────────────────────────────────────────────

# Sidebar
with st.sidebar:
    st.markdown(f"""
    <div style="text-align: center; padding: 20px;">
        <h2 style="color: {st.session_state.color_palette['primary']};">🔬 UnIst Analytics</h2>
        <p>Advanced Scientific Publication Analyzer</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Информация о статусе
    st.markdown("---")
    st.markdown("### 📊 Status")
    
    if st.session_state.selected_ror:
        st.success(f"✅ Organization selected: `{st.session_state.selected_ror}`")
    else:
        st.warning("⏳ No organization selected")
    
    if st.session_state.analysis_complete:
        st.success(f"✅ Analysis complete: {len(st.session_state.results_df) if st.session_state.results_df is not None else 0} papers")
    
    # Информация о кэше
    st.markdown("---")
    st.markdown("### 💾 Cache Info")
    cache_size = len([f for f in os.listdir(CACHE_DIR) if f.endswith('.pkl')]) if os.path.exists(CACHE_DIR) else 0
    st.info(f"📁 Cached items: {cache_size}")
    
    if st.button("🗑️ Clear Cache", use_container_width=True):
        cache.clear()
        st.success("Cache cleared!")
        st.rerun()
    
    # Информация о IF/CS файлах
    st.markdown("---")
    st.markdown("### 📚 Journal Metrics")
    if os.path.exists("IF.xlsx"):
        st.success("✅ IF.xlsx loaded")
    else:
        st.warning("⚠️ IF.xlsx not found")
    
    if os.path.exists("CS.xlsx"):
        st.success("✅ CS.xlsx loaded")
    else:
        st.warning("⚠️ CS.xlsx not found")
    
    # Кнопка сброса
    st.markdown("---")
    if st.button("🔄 Reset Application", use_container_width=True):
        for key in list(st.session_state.keys()):
            if key != 'color_palette':  # Сохраняем цветовую палитру
                del st.session_state[key]
        st.rerun()

# Main content area with tabs
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "🔍 Search & Analysis", 
    "📊 Results", 
    "📈 Visualizations", 
    "🌐 Networks",
    "📥 Export"
])

# Tab 1: Search & Analysis
with tab1:
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("### 🏛 Organization Input")
        org_input = st.text_input(
            "Enter ROR or organization name",
            placeholder="e.g., 05wv0v765 or Ural Federal University",
            key="org_input"
        )
        
        st.markdown("### 📅 Period Input")
        years_input = st.text_input(
            "Enter years",
            placeholder="e.g., 2023, 2022-2024, or 2021,2023-2025",
            key="years_input"
        )
    
    with col2:
        st.markdown("### ℹ️ Examples")
        with st.expander("Organization Examples"):
            st.markdown("""
            - ROR ID: `05wv0v765`
            - URL: `https://ror.org/05wv0v765`
            - Name: `Ural Federal University`
            - Name with hyphen: `Institute of High-Temperature Electrochemistry`
            """)
        
        with st.expander("Year Examples"):
            st.markdown("""
            - Single year: `2023`
            - Range: `2022-2024`
            - Mixed: `2021,2023-2025`
            """)
    
    # Search button
    col1, col2, col3 = st.columns([1, 1, 2])
    with col1:
        search_clicked = st.button("🔍 Find Organization", use_container_width=True, type="primary")
    
    with col2:
        analyze_clicked = st.button("📊 Run Analysis", use_container_width=True, type="secondary", 
                                   disabled=st.session_state.selected_ror is None)
    
    # Handle search
    if search_clicked and org_input:
        with st.spinner("🔍 Searching for organization..."):
            ror_id, error = select_organization(org_input)
            
            if ror_id:
                st.session_state.selected_ror = ror_id
                st.success(f"✅ Using ROR: {ror_id}")
                st.rerun()
            elif error:
                st.error(f"❌ {error}")
            else:
                st.session_state.org_search_results = error
                st.info(f"Found {len(error)} organizations. Please select:")
                
                # Create selection dropdown
                options = []
                for i, org in enumerate(error, 1):
                    ror_id = org['ror'].replace('https://ror.org/', '') if org['ror'] else 'N/A'
                    label = f"{i}. {org['display_name']} | {org.get('country', 'N/A')} | Works: {org['works_count']:,}"
                    options.append((label, org['ror']))
                
                selected_org = st.selectbox(
                    "Select organization:",
                    options=options,
                    format_func=lambda x: x[0] if isinstance(x, tuple) else x,
                    key="org_selector"
                )
                
                if selected_org and st.button("✅ Confirm Selection", use_container_width=True):
                    ror_id = selected_org[1].replace('https://ror.org/', '')
                    st.session_state.selected_ror = ror_id
                    st.success(f"✅ Selected organization with ROR: {ror_id}")
                    st.rerun()
    
    # Handle analysis
    if analyze_clicked and st.session_state.selected_ror and years_input:
        with st.spinner("🚀 Starting comprehensive analysis..."):
            # Parse years
            orig_years_list = parse_year_input(years_input)
            if not orig_years_list:
                st.error("❌ Error: could not parse years")
            else:
                st.session_state.orig_years_list = orig_years_list
                orig_years_set = set(orig_years_list)
                st.session_state.exp_years = get_expanded_years(orig_years_list)
                
                # Display parameters
                st.info(f"""
                **Analysis Parameters:**
                - 🏛 ROR ID: {st.session_state.selected_ror}
                - 📅 Original period: {', '.join(map(str, orig_years_list))}
                - 🔍 Search period: {', '.join(map(str, st.session_state.exp_years))}
                """)
                
                # Collect DOIs
                st.write("1. Collecting DOIs from OpenAlex...")
                dois, err = fetch_all_dois_openalex(st.session_state.selected_ror, st.session_state.exp_years)
                
                if err:
                    st.error(f"❌ Error: {err}")
                elif not dois:
                    st.warning("❌ No publications with DOI in expanded period")
                else:
                    st.success(f"✅ Unique DOIs collected: {len(dois):,}")
                    st.session_state.dois_list = dois
                    
                    # Process DOIs
                    st.write("2. Retrieving enhanced data from Crossref and OpenAlex...")
                    st.write(f"   → Using {MAX_WORKERS} parallel threads")
                    st.write(f"   → Max retries: {MAX_RETRIES}")
                    
                    start = time.time()
                    results, errors = process_dois_parallel(dois, st.session_state.selected_ror)
                    duration = time.time() - start
                    
                    st.session_state.errors_list = errors
                    
                    if results:
                        df = create_results_dataframe(results, orig_years_set)
                        st.session_state.results_df = df
                        st.session_state.analysis_complete = True
                        
                        st.success(f"""
                        ✅ Processing complete!
                        - Successful: {len(results):,}
                        - Errors: {len(errors):,}
                        - Time: {duration:.1f} sec
                        """)
                        
                        # Show summary
                        belong = df[df['belongs_to_period'] == True].copy()
                        not_belong = df[df['belongs_to_period'] == False].copy()
                        
                        st.metric("Papers in target period", f"{len(belong):,}", 
                                 f"{len(belong)/len(results):.1%}")
                        st.metric("Papers outside period", f"{len(not_belong):,}", 
                                 f"{len(not_belong)/len(results):.1%}")
                    else:
                        st.error("❌ No successfully processed papers")

# Tab 2: Results
with tab2:
    if st.session_state.analysis_complete and st.session_state.results_df is not None:
        df = st.session_state.results_df
        
        # Filters
        st.markdown("### 🔍 Filter Results")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            show_only_period = st.checkbox("Show only papers in target period", value=True)
        
        with col2:
            if 'journal' in df.columns:
                journals = ['All'] + sorted(df['journal'].dropna().unique().tolist())
                selected_journal = st.selectbox("Filter by journal", journals)
        
        with col3:
            if 'type' in df.columns:
                types = ['All'] + sorted(df['type'].dropna().unique().tolist())
                selected_type = st.selectbox("Filter by type", types)
        
        # Apply filters
        filtered_df = df.copy()
        if show_only_period:
            filtered_df = filtered_df[filtered_df['belongs_to_period'] == True]
        
        if selected_journal != 'All':
            filtered_df = filtered_df[filtered_df['journal'] == selected_journal]
        
        if selected_type != 'All':
            filtered_df = filtered_df[filtered_df['type'] == selected_type]
        
        # Display statistics
        st.markdown("### 📊 Summary Statistics")
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Papers", len(filtered_df))
        
        with col2:
            if 'authors_count' in filtered_df.columns:
                st.metric("Avg Authors", f"{filtered_df['authors_count'].mean():.1f}")
        
        with col3:
            if 'citations_cr' in filtered_df.columns:
                st.metric("Avg Citations (CR)", f"{filtered_df['citations_cr'].mean():.1f}")
        
        with col4:
            if 'citations_oa' in filtered_df.columns:
                st.metric("Avg Citations (OA)", f"{filtered_df['citations_oa'].mean():.1f}")
        
        # Display dataframe
        st.markdown("### 📋 Publications")
        
        # Select columns to display
        display_cols = ['title', 'authors', 'journal', 'late_year', 'citations_cr', 'citations_oa']
        display_cols = [col for col in display_cols if col in filtered_df.columns]
        
        # Add IF/CS columns if they exist
        if 'IF' in filtered_df.columns:
            display_cols.extend(['IF', 'IF_Q'])
        if 'CS' in filtered_df.columns:
            display_cols.extend(['CS', 'CS_Q'])
        
        st.dataframe(
            filtered_df[display_cols].head(100),
            use_container_width=True,
            height=400
        )
        
        st.caption(f"Showing first 100 of {len(filtered_df)} records")
        
    else:
        st.info("👈 Please run an analysis first in the Search & Analysis tab")

# Tab 3: Visualizations
with tab3:
    if st.session_state.analysis_complete and st.session_state.results_df is not None:
        st.markdown("### 📈 Scientific Visualizations")
        
        with st.spinner("Creating visualizations..."):
            figs = create_enhanced_visualizations(st.session_state.results_df)
            
            if figs:
                # Create rows of visualizations
                col1, col2 = st.columns(2)
                
                with col1:
                    if 'timeline' in figs:
                        st.pyplot(figs['timeline'])
                        st.caption("Figure 1: Publication timeline showing distribution over years")
                
                with col2:
                    if 'journals' in figs:
                        st.pyplot(figs['journals'])
                        st.caption("Figure 2: Top 10 journals by number of publications")
                
                col3, col4 = st.columns(2)
                
                with col3:
                    if 'citations' in figs:
                        st.pyplot(figs['citations'])
                        st.caption("Figure 3: Citation distribution comparison")
                
                with col4:
                    if 'oa_trend' in figs:
                        st.pyplot(figs['oa_trend'])
                        st.caption("Figure 4: Open Access trend over time")
                
                if 'countries' in figs:
                    st.pyplot(figs['countries'])
                    st.caption("Figure 5: Country collaboration heatmap")
            else:
                st.warning("No data available for visualization")
    else:
        st.info("👈 Please run an analysis first in the Search & Analysis tab")

# Tab 4: Networks
with tab4:
    if st.session_state.analysis_complete and st.session_state.results_df is not None:
        st.markdown("### 🌐 Network Analysis")
        
        df = st.session_state.results_df
        belong = df[df['belongs_to_period'] == True].copy()
        
        if not belong.empty:
            # Network type selector
            network_type = st.radio(
                "Select network type:",
                ["Country Collaboration", "Affiliation Collaboration"],
                horizontal=True
            )
            
            if network_type == "Country Collaboration":
                G, stats = create_country_network(df)
                
                if len(G.nodes()) > 0:
                    st.markdown("#### Country Collaboration Network")
                    
                    # Network statistics
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Countries", len(G.nodes()))
                    with col2:
                        st.metric("Collaborations", len(G.edges()))
                    with col3:
                        st.metric("Avg Partners", f"{sum(dict(G.degree()).values())/len(G.nodes()):.2f}")
                    
                    # Create interactive plot
                    pos = nx.spring_layout(G, k=2, iterations=50)
                    
                    # Create edge trace
                    edge_trace = []
                    for edge in G.edges():
                        x0, y0 = pos[edge[0]]
                        x1, y1 = pos[edge[1]]
                        edge_trace.append(go.Scatter(
                            x=[x0, x1, None], y=[y0, y1, None],
                            line=dict(width=G[edge[0]][edge[1]]['weight'], color='#888'),
                            hoverinfo='none',
                            mode='lines'
                        ))
                    
                    # Create node trace
                    node_x = []
                    node_y = []
                    node_text = []
                    node_size = []
                    
                    for node in G.nodes():
                        x, y = pos[node]
                        node_x.append(x)
                        node_y.append(y)
                        node_text.append(f"{node}<br>Papers: {G.nodes[node]['papers']}<br>Partners: {G.degree(node)}")
                        node_size.append(G.nodes[node]['papers'] * 3)
                    
                    node_trace = go.Scatter(
                        x=node_x, y=node_y,
                        mode='markers+text',
                        text=list(G.nodes()),
                        textposition="top center",
                        hovertext=node_text,
                        hoverinfo='text',
                        marker=dict(
                            size=node_size,
                            color=st.session_state.color_palette['primary'],
                            line=dict(color='darkblue', width=2)
                        )
                    )
                    
                    fig = go.Figure(data=edge_trace + [node_trace],
                                   layout=go.Layout(
                                       title='Country Collaboration Network',
                                       showlegend=False,
                                       hovermode='closest',
                                       xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                                       yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                                       height=600
                                   ))
                    
                    st.plotly_chart(fig, use_container_width=True)
                    
                    # Show top collaborations
                    st.markdown("#### Top Collaborations")
                    edges_data = []
                    for edge in G.edges():
                        edges_data.append({
                            'Country 1': edge[0],
                            'Country 2': edge[1],
                            'Collaborations': G[edge[0]][edge[1]]['weight']
                        })
                    
                    edges_df = pd.DataFrame(edges_data)
                    edges_df = edges_df.sort_values('Collaborations', ascending=False).head(10)
                    st.dataframe(edges_df, use_container_width=True)
                
                else:
                    st.warning("No country collaboration data available")
            
            else:  # Affiliation Collaboration
                G, stats = create_affiliation_network(df)
                
                if len(G.nodes()) > 0:
                    st.markdown("#### Affiliation Collaboration Network")
                    
                    # Network statistics
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Affiliations", len(G.nodes()))
                    with col2:
                        st.metric("Collaborations", len(G.edges()))
                    with col3:
                        st.metric("Avg Partners", f"{sum(dict(G.degree()).values())/len(G.nodes()):.2f}")
                    
                    # Filter to top nodes for better visualization
                    node_degrees = dict(G.degree())
                    top_nodes = sorted(node_degrees, key=node_degrees.get, reverse=True)[:20]
                    G_sub = G.subgraph(top_nodes)
                    
                    if len(G_sub.nodes()) > 0:
                        pos = nx.spring_layout(G_sub, k=3, iterations=50)
                        
                        # Create edge trace
                        edge_trace = []
                        for edge in G_sub.edges():
                            x0, y0 = pos[edge[0]]
                            x1, y1 = pos[edge[1]]
                            edge_trace.append(go.Scatter(
                                x=[x0, x1, None], y=[y0, y1, None],
                                line=dict(width=G_sub[edge[0]][edge[1]]['weight'], color='#888'),
                                hoverinfo='none',
                                mode='lines'
                            ))
                        
                        # Create node trace
                        node_x = []
                        node_y = []
                        node_text = []
                        node_size = []
                        
                        for node in G_sub.nodes():
                            x, y = pos[node]
                            node_x.append(x)
                            node_y.append(y)
                            node_text.append(f"{node[:50]}...<br>Papers: {G_sub.nodes[node]['papers']}<br>Partners: {G_sub.degree(node)}")
                            node_size.append(G_sub.nodes[node]['papers'] * 2)
                        
                        node_trace = go.Scatter(
                            x=node_x, y=node_y,
                            mode='markers+text',
                            text=[n[:20] + '...' if len(n) > 20 else n for n in G_sub.nodes()],
                            textposition="top center",
                            hovertext=node_text,
                            hoverinfo='text',
                            marker=dict(
                                size=node_size,
                                color=st.session_state.color_palette['secondary'],
                                line=dict(color='darkgreen', width=2)
                            )
                        )
                        
                        fig = go.Figure(data=edge_trace + [node_trace],
                                       layout=go.Layout(
                                           title='Top 20 Affiliations Collaboration Network',
                                           showlegend=False,
                                           hovermode='closest',
                                           xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                                           yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                                           height=600
                                       ))
                        
                        st.plotly_chart(fig, use_container_width=True)
                        
                        # Show top affiliations
                        st.markdown("#### Top Affiliations")
                        nodes_data = []
                        for node in G.nodes(data=True):
                            nodes_data.append({
                                'Affiliation': node[0],
                                'Papers': node[1].get('papers', 0),
                                'Countries': node[1].get('countries', ''),
                                'Collaboration Partners': G.degree(node[0])
                            })
                        
                        nodes_df = pd.DataFrame(nodes_data)
                        nodes_df = nodes_df.sort_values('Papers', ascending=False).head(20)
                        st.dataframe(nodes_df, use_container_width=True)
                    
                    else:
                        st.warning("Not enough affiliation data for visualization")
                else:
                    st.warning("No affiliation collaboration data available")
        else:
            st.warning("No papers in target period for network analysis")
    else:
        st.info("👈 Please run an analysis first in the Search & Analysis tab")

# Tab 5: Export
with tab5:
    if st.session_state.analysis_complete and st.session_state.results_df is not None:
        st.markdown("### 📥 Export Data")
        
        df = st.session_state.results_df
        
        # Export options
        st.markdown("#### Export Format")
        export_format = st.radio(
            "Select export format:",
            ["Excel (Full Report)", "CSV (Main Data Only)", "JSON (Complete)"],
            horizontal=True
        )
        
        if export_format == "Excel (Full Report)":
            st.markdown("#### Excel Export Options")
            st.info("""
            The Excel export includes multiple sheets:
            - Main Data: Complete publication data
            - Period Statistics: Summary statistics
            - Top Authors: Author frequency
            - Journal Frequency: Journal rankings
            - Publisher Frequency: Publisher rankings
            - Country Frequency: Country statistics
            - Citation Statistics: Detailed citation metrics
            - Year Distribution: Publication timeline
            - OA Statistics: Open Access metrics
            - Affiliation Network: Collaboration statistics
            - Country Network: International collaboration
            - Errors: Failed DOIs
            - Metadata: Analysis parameters
            """)
            
            if st.button("📊 Generate Excel Report", use_container_width=True, type="primary"):
                with st.spinner("Generating Excel report..."):
                    filename = export_to_excel(
                        df, 
                        df.to_dict('records'), 
                        st.session_state.errors_list,
                        st.session_state.selected_ror,
                        st.session_state.orig_years_list,
                        st.session_state.exp_years
                    )
                    
                    if os.path.exists(filename):
                        with open(filename, 'rb') as f:
                            st.download_button(
                                label="📥 Download Excel File",
                                data=f,
                                file_name=filename,
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                use_container_width=True
                            )
                    else:
                        st.error("Failed to generate Excel file")
        
        elif export_format == "CSV (Main Data Only)":
            st.markdown("#### CSV Export")
            
            # Select columns for CSV
            available_cols = df.columns.tolist()
            selected_cols = st.multiselect(
                "Select columns to export:",
                available_cols,
                default=['doi', 'title', 'authors', 'journal', 'late_year', 'citations_cr', 'citations_oa']
            )
            
            if selected_cols:
                csv_data = df[selected_cols].to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="📥 Download CSV",
                    data=csv_data,
                    file_name=f"analysis_{st.session_state.selected_ror}.csv",
                    mime="text/csv",
                    use_container_width=True
                )
        
        else:  # JSON
            st.markdown("#### JSON Export")
            
            # Prepare JSON data
            json_data = {
                'metadata': {
                    'analysis_date': datetime.now().isoformat(),
                    'ror_id': st.session_state.selected_ror,
                    'original_period': st.session_state.orig_years_list,
                    'search_period': st.session_state.exp_years,
                    'total_papers': len(df),
                    'papers_in_period': len(df[df['belongs_to_period'] == True])
                },
                'papers': df.to_dict('records'),
                'errors': st.session_state.errors_list
            }
            
            json_str = json.dumps(json_data, indent=2, default=str).encode('utf-8')
            st.download_button(
                label="📥 Download JSON",
                data=json_str,
                file_name=f"analysis_{st.session_state.selected_ror}.json",
                mime="application/json",
                use_container_width=True
            )
        
        # Preview
        st.markdown("#### Data Preview")
        st.dataframe(df.head(10), use_container_width=True)
        
    else:
        st.info("👈 Please run an analysis first in the Search & Analysis tab")

# Footer
st.markdown("---")
col1, col2, col3 = st.columns(3)
with col1:
    st.markdown(f"**Cache:** `{CACHE_DIR}`")
with col2:
    st.markdown(f"**Logs:** `{LOG_DIR}`")
with col3:
    st.markdown(f"**Version:** 2.0.0")

# Display random color palette info
with st.expander("🎨 Current Color Palette"):
    colors = st.session_state.color_palette
    st.markdown(f"""
    <div style="display: flex; gap: 10px; padding: 10px;">
        <div style="background-color: {colors['primary']}; width: 50px; height: 50px; border-radius: 10px;"></div>
        <div style="background-color: {colors['secondary']}; width: 50px; height: 50px; border-radius: 10px;"></div>
        <div style="background-color: {colors['accent']}; width: 50px; height: 50px; border-radius: 10px;"></div>
        <div style="background-color: {colors['background']}; width: 50px; height: 50px; border-radius: 10px; border: 1px solid white;"></div>
        <div style="background-color: {colors['success']}; width: 50px; height: 50px; border-radius: 10px;"></div>
        <div style="background-color: {colors['warning']}; width: 50px; height: 50px; border-radius: 10px;"></div>
        <div style="background-color: {colors['info']}; width: 50px; height: 50px; border-radius: 10px;"></div>
    </div>
    """, unsafe_allow_html=True)
    if st.button("🎲 Randomize Palette"):
        st.session_state.color_palette = random.choice(COLOR_PALETTES)
        st.rerun()
