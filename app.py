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
from typing import List, Dict, Tuple, Optional, Set, Any
import warnings
warnings.filterwarnings('ignore')

# Scientific style for matplotlib plots
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

# Random color palette for interface
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

# Plot color palettes (15 options for user selection)
PLOT_COLOR_PALETTES = [
    {
        'name': 'Viridis (Default)',
        'sequential': 'Viridis',
        'categorical': ['#440154', '#3b528b', '#21918c', '#5ec962', '#fde725'],
        'diverging': ['#d73027', '#f46d43', '#fdae61', '#fee08b', '#ffffbf', '#d9ef8b', '#a6d96a', '#66bd63', '#1a9850']
    },
    {
        'name': 'Plasma',
        'sequential': 'Plasma',
        'categorical': ['#0d0887', '#6a00a8', '#b12a90', '#e16462', '#fca636'],
        'diverging': ['#d73027', '#f46d43', '#fdae61', '#fee08b', '#ffffbf', '#d9ef8b', '#a6d96a', '#66bd63', '#1a9850']
    },
    {
        'name': 'Inferno',
        'sequential': 'Inferno',
        'categorical': ['#000004', '#320a5e', '#781c6d', '#bb3754', '#fcffa4'],
        'diverging': ['#d73027', '#f46d43', '#fdae61', '#fee08b', '#ffffbf', '#d9ef8b', '#a6d96a', '#66bd63', '#1a9850']
    },
    {
        'name': 'Magma',
        'sequential': 'Magma',
        'categorical': ['#000004', '#2d0c4b', '#711f81', '#b63679', '#fcfdbf'],
        'diverging': ['#d73027', '#f46d43', '#fdae61', '#fee08b', '#ffffbf', '#d9ef8b', '#a6d96a', '#66bd63', '#1a9850']
    },
    {
        'name': 'Cividis',
        'sequential': 'Cividis',
        'categorical': ['#00204d', '#2b4e7a', '#557e9c', '#88b0b1', '#fdeba9'],
        'diverging': ['#d73027', '#f46d43', '#fdae61', '#fee08b', '#ffffbf', '#d9ef8b', '#a6d96a', '#66bd63', '#1a9850']
    },
    {
        'name': 'Turbo',
        'sequential': 'Turbo',
        'categorical': ['#30123b', '#4668b2', '#32a674', '#c6da37', '#fcfc5c'],
        'diverging': ['#d73027', '#f46d43', '#fdae61', '#fee08b', '#ffffbf', '#d9ef8b', '#a6d96a', '#66bd63', '#1a9850']
    },
    {
        'name': 'Blues',
        'sequential': 'Blues',
        'categorical': ['#08306b', '#2171b5', '#4292c6', '#6baed6', '#c6dbef'],
        'diverging': ['#d73027', '#f46d43', '#fdae61', '#fee08b', '#ffffbf', '#d9ef8b', '#a6d96a', '#66bd63', '#1a9850']
    },
    {
        'name': 'Reds',
        'sequential': 'Reds',
        'categorical': ['#67000d', '#a50f15', '#cb181d', '#ef3b2c', '#fcae91'],
        'diverging': ['#d73027', '#f46d43', '#fdae61', '#fee08b', '#ffffbf', '#d9ef8b', '#a6d96a', '#66bd63', '#1a9850']
    },
    {
        'name': 'Greens',
        'sequential': 'Greens',
        'categorical': ['#00441b', '#238b45', '#41ab5d', '#74c476', '#c7e9c0'],
        'diverging': ['#d73027', '#f46d43', '#fdae61', '#fee08b', '#ffffbf', '#d9ef8b', '#a6d96a', '#66bd63', '#1a9850']
    },
    {
        'name': 'Purples',
        'sequential': 'Purples',
        'categorical': ['#3f007d', '#6a51a3', '#807dba', '#9e9ac8', '#dadaeb'],
        'diverging': ['#d73027', '#f46d43', '#fdae61', '#fee08b', '#ffffbf', '#d9ef8b', '#a6d96a', '#66bd63', '#1a9850']
    },
    {
        'name': 'Oranges',
        'sequential': 'Oranges',
        'categorical': ['#7f3b08', '#b85a0e', '#e68216', '#f39c2b', '#fddfb2'],
        'diverging': ['#d73027', '#f46d43', '#fdae61', '#fee08b', '#ffffbf', '#d9ef8b', '#a6d96a', '#66bd63', '#1a9850']
    },
    {
        'name': 'Spectral',
        'sequential': 'Spectral',
        'categorical': ['#9e0142', '#d53e4f', '#f46d43', '#fdae61', '#fee08b', '#ffffbf', '#e6f598', '#abdda4', '#66c2a5', '#3288bd', '#5e4fa2'],
        'diverging': ['#9e0142', '#d53e4f', '#f46d43', '#fdae61', '#fee08b', '#ffffbf', '#e6f598', '#abdda4', '#66c2a5', '#3288bd', '#5e4fa2']
    },
    {
        'name': 'Coolwarm',
        'sequential': 'Coolwarm',
        'categorical': ['#3b4cc0', '#5d71d0', '#8195e0', '#a5b8f0', '#d5e3ff'],
        'diverging': ['#b40426', '#d94e3c', '#f68b5b', '#fdbb84', '#f7f7f7', '#b9e0f2', '#74add1', '#3f7eb6', '#2c5c8a']
    },
    {
        'name': 'Viridis (Alternative)',
        'sequential': 'Viridis',
        'categorical': ['#440154', '#3b528b', '#21918c', '#5ec962', '#fde725'],
        'diverging': ['#d73027', '#f46d43', '#fdae61', '#fee08b', '#ffffbf', '#d9ef8b', '#a6d96a', '#66bd63', '#1a9850']
    },
    {
        'name': 'Electric',
        'sequential': 'Electric',
        'categorical': ['#0d0887', '#5e01a6', '#b12a90', '#e16462', '#fca636'],
        'diverging': ['#d73027', '#f46d43', '#fdae61', '#fee08b', '#ffffbf', '#d9ef8b', '#a6d96a', '#66bd63', '#1a9850']
    }
]

# Random palette selection at startup
if 'color_palette' not in st.session_state:
    st.session_state.color_palette = random.choice(COLOR_PALETTES)

# Random plot palette selection at startup (10 interesting palettes, randomly chosen)
if 'plot_palette' not in st.session_state:
    # Select from first 10 palettes (all are interesting)
    interesting_palettes = PLOT_COLOR_PALETTES[:10]
    st.session_state['plot_palette'] = random.choice(interesting_palettes)

# Page configuration
st.set_page_config(
    page_title="UnIst Analytics",
    page_icon="📚",
    layout="wide",
    initial_sidebar_state="collapsed"  # Sidebar collapsed by default
)

# Logo loading
def get_logo_html():
    if os.path.exists("logo.png"):
        with open("logo.png", "rb") as f:
            logo_data = base64.b64encode(f.read()).decode()
        return f'<img src="data:image/png;base64,{logo_data}" style="height: 250px; margin-right: 10px;">'  # Increased height
    return ""

# Custom CSS with dynamic colors - Scientific Academic Style
def get_custom_css():
    colors = {
        'background': '#f0f9ff',
        'text': '#002b36',
        'primary': '#006994',
        'secondary': '#00b4d8',
        'accent': '#03045e',
        'success': '#2ecc71',
        'warning': '#f39c12',
        'info': '#3498db',
        'gradient_start': '#023e8a',
        'gradient_end': '#0077be',
        'card_bg': '#ffffff',
        'card_border': '#caf0f8',
        'metric_bg': '#e6f3ff'
    }
    
    return f"""
    <style>
        /* Global styles */
        .stApp {{
            background-color: {colors['background']};
        }}
        
        /* Main header - only logo now */
        .main-header {{
            font-size: 2.5rem;
            font-weight: 700;
            background: linear-gradient(135deg, {colors['gradient_start']}, {colors['gradient_end']});
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            margin-bottom: 0.5rem;
        }}
        
        /* Sub headers */
        h1, h2, h3 {{
            color: {colors['text']} !important;
            font-weight: 600 !important;
            border-bottom: 3px solid {colors['primary']};
            padding-bottom: 0.5rem;
            margin-top: 1rem;
        }}
        
        /* Cards */
        .card {{
            background: {colors['card_bg']};
            border-radius: 15px;
            padding: 1.5rem;
            box-shadow: 0 8px 20px rgba(0,0,0,0.05);
            border: 1px solid {colors['card_border']};
            margin-bottom: 1rem;
            transition: transform 0.2s, box-shadow 0.2s;
        }}
        
        .card:hover {{
            transform: translateY(-2px);
            box-shadow: 0 12px 25px rgba(0,0,0,0.1);
        }}
        
        /* Metric cards */
        .metric-card {{
            background: linear-gradient(135deg, {colors['gradient_start']}10, {colors['gradient_end']}10);
            border-radius: 12px;
            padding: 1rem;
            border-left: 4px solid {colors['primary']};
            text-align: center;
            box-shadow: 0 4px 6px rgba(0,0,0,0.02);
        }}
        
        .metric-card .value {{
            font-size: 2rem;
            font-weight: 700;
            color: {colors['primary']};
            line-height: 1.2;
        }}
        
        .metric-card .label {{
            font-size: 0.9rem;
            color: {colors['text']};
            opacity: 0.8;
            margin-top: 0.3rem;
        }}
        
        /* Step indicator */
        .step-container {{
            display: flex;
            justify-content: space-between;
            margin: 2rem 0;
            position: relative;
        }}
        
        .step {{
            flex: 1;
            text-align: center;
            padding: 1rem;
            background: {colors['card_bg']};
            border: 2px solid {colors['card_border']};
            border-radius: 10px;
            position: relative;
            transition: all 0.3s;
            margin: 0 5px;
        }}
        
        .step.active {{
            border-color: {colors['primary']};
            background: linear-gradient(135deg, {colors['gradient_start']}10, {colors['gradient_end']}10);
        }}
        
        .step.completed {{
            border-color: {colors['success']};
            background: {colors['success']}10;
        }}
        
        .step-number {{
            width: 30px;
            height: 30px;
            background: {colors['primary']};
            color: white;
            border-radius: 50%;
            display: inline-block;
            line-height: 30px;
            margin-bottom: 0.5rem;
        }}
        
        .step.completed .step-number {{
            background: {colors['success']};
        }}
        
        /* Buttons */
        .stButton > button {{
            background: linear-gradient(135deg, {colors['gradient_start']}, {colors['gradient_end']});
            color: white;
            border: none;
            border-radius: 8px;
            padding: 0.5rem 2rem;
            font-weight: 600;
            transition: all 0.3s;
            box-shadow: 0 4px 10px {colors['primary']}30;
        }}
        
        .stButton > button:hover {{
            transform: translateY(-2px);
            box-shadow: 0 6px 15px {colors['primary']}50;
        }}
        
        .stButton > button[kind="secondary"] {{
            background: white;
            color: {colors['primary']};
            border: 2px solid {colors['primary']};
            box-shadow: none;
        }}
        
        .stButton > button[kind="secondary"]:hover {{
            background: {colors['primary']}10;
        }}
        
        /* Info boxes */
        .info-box {{
            background: {colors['primary']}10;
            border-left: 4px solid {colors['primary']};
            border-radius: 8px;
            padding: 1rem;
            margin: 1rem 0;
        }}
        
        .success-box {{
            background: {colors['success']}10;
            border-left: 4px solid {colors['success']};
            border-radius: 8px;
            padding: 1rem;
            margin: 1rem 0;
        }}
        
        .warning-box {{
            background: {colors['warning']}10;
            border-left: 4px solid {colors['warning']};
            border-radius: 8px;
            padding: 1rem;
            margin: 1rem 0;
        }}
        
        .error-box {{
            background: {colors['warning']}20;
            border-left: 4px solid {colors['warning']};
            border-radius: 8px;
            padding: 1rem;
            margin: 1rem 0;
        }}
        
        /* Tabs */
        .stTabs [data-baseweb="tab-list"] {{
            gap: 8px;
            background-color: {colors['card_bg']};
            padding: 0.5rem;
            border-radius: 10px;
            border: 1px solid {colors['card_border']};
        }}
        
        .stTabs [data-baseweb="tab"] {{
            border-radius: 8px;
            padding: 0.5rem 1rem;
            color: {colors['text']};
        }}
        
        .stTabs [aria-selected="true"] {{
            background: linear-gradient(135deg, {colors['gradient_start']}, {colors['gradient_end']});
            color: white !important;
        }}
        
        /* Progress bar */
        .stProgress > div > div > div > div {{
            background: linear-gradient(90deg, {colors['gradient_start']}, {colors['gradient_end']});
        }}
        
        /* Dataframe */
        .stDataFrame {{
            border: 1px solid {colors['card_border']};
            border-radius: 10px;
            overflow: hidden;
        }}
        
        .stDataFrame th {{
            background: linear-gradient(135deg, {colors['gradient_start']}, {colors['gradient_end']});
            color: white;
            padding: 0.75rem;
            font-weight: 600;
        }}
        
        .stDataFrame td {{
            padding: 0.5rem 0.75rem;
            border-bottom: 1px solid {colors['card_border']};
        }}
        
        .stDataFrame tr:hover {{
            background: {colors['primary']}05;
        }}
        
        /* Recent institutions */
        .recent-inst {{
            background: {colors['card_bg']};
            border: 1px solid {colors['card_border']};
            border-radius: 8px;
            padding: 0.5rem;
            margin: 0.2rem 0;
            cursor: pointer;
            transition: all 0.2s;
        }}
        
        .recent-inst:hover {{
            border-color: {colors['primary']};
            background: {colors['primary']}05;
        }}
        
        /* Hide sidebar completely */
        section[data-testid="stSidebar"] {{
            display: none;
        }}
    </style>
    """

# Apply custom CSS
st.markdown(get_custom_css(), unsafe_allow_html=True)

# Header with logo only (no text)
logo_html = get_logo_html()
st.markdown(f"""
<div style="display: flex; align-items: center; margin-bottom: 20px;">
    {logo_html}
</div>
""", unsafe_allow_html=True)

# Configuration
CACHE_DIR = "cache"
LOG_DIR = "logs"
CROSSREF_EMAIL = "your.email@example.com"  # Replace with your email
MAX_WORKERS = 7
RATE_LIMIT_DELAY = 0.7
MAX_RETRIES = 5
MAX_PAPERS_TO_ANALYZE = 10000  # Maximum papers to process
MAX_PAGES = 50  # Maximum pages to fetch (200 papers per page)
WARN_PAPERS_THRESHOLD = 5000  # Show warning above this

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

# Initialize session state for step-by-step workflow
if 'step' not in st.session_state:
    st.session_state['step'] = 1  # 1: Search, 2: Period, 3: Results
if 'selected_ror' not in st.session_state:
    st.session_state.selected_ror = None
if 'selected_org_name' not in st.session_state:
    st.session_state.selected_org_name = None
if 'selected_org_country' not in st.session_state:
    st.session_state.selected_org_country = None
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
if 'recent_institutions' not in st.session_state:
    st.session_state['recent_institutions'] = []
if 'expanded_details' not in st.session_state:
    st.session_state['expanded_details'] = {}
if 'search_query' not in st.session_state:
    st.session_state['search_query'] = ''
if 'search_performed' not in st.session_state:
    st.session_state['search_performed'] = False
if 'year_input_text' not in st.session_state:
    st.session_state['year_input_text'] = ''
if 'papers_data' not in st.session_state:
    st.session_state['papers_data'] = None
if 'validation_stats' not in st.session_state:
    st.session_state['validation_stats'] = None
if 'total_papers_estimate' not in st.session_state:
    st.session_state['total_papers_estimate'] = 0

# Cache class
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

# Database loading functions from second code
def normalize_issn(issn_str):
    """
    Normalize ISSN to 8-digit format without hyphens.
    Handles:
    - With hyphens: 0007-9235 -> 00079235
    - Without hyphens: 15299732 -> 15299732
    - With X at the end: 1234-567X -> 1234567X
    """
    if pd.isna(issn_str) or not issn_str:
        return ""
    
    # Convert to string and remove any whitespace
    issn_str = str(issn_str).strip().upper()
    
    # Remove hyphens and spaces
    clean = re.sub(r'[\s-]', '', issn_str)
    
    # If it's all digits or digits with X at the end, pad to 8 digits
    if re.match(r'^\d{7}[\dX]?$', clean) or re.match(r'^\d{1,7}$', clean):
        if len(clean) < 8:
            clean = clean.zfill(8)
        if len(clean) == 8:
            return clean
    
    return ""

def format_issn_with_hyphen(issn: str) -> Optional[str]:
    """
    Format ISSN to standard format with hyphen: XXXX-XXXX
    Handles:
    - 20734352 -> 2073-4352
    - 69358 -> 0006-9358 (pad with zeros)
    - 2073-4352 -> 2073-4352 (already formatted)
    """
    if pd.isna(issn) or not issn:
        return None
    
    # Convert to string and remove any whitespace
    issn_str = str(issn).strip().upper()
    
    # Remove existing hyphens first
    clean = re.sub(r'[\s-]', '', issn_str)
    
    # If it's all digits or digits with X, pad to 8 characters
    if re.match(r'^\d+$', clean) or re.match(r'^\d+X$', clean):
        if len(clean) < 8:
            clean = clean.zfill(8)
        if len(clean) == 8:
            # Insert hyphen after 4th character
            return f"{clean[:4]}-{clean[4:]}"
    
    return None

@st.cache_data
def load_if_data():
    """Load IF.xlsx file with caching (WoS database)"""
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
    """Load CS.xlsx file with caching (Scopus database)"""
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
                # Extract the highest quartile (lowest number) if multiple
                if ',' in quartile_val:
                    quartile_parts = [q.strip() for q in quartile_val.split(',')]
                    # Find the quartile with the smallest number
                    quartile_numbers = []
                    for q in quartile_parts:
                        # Extract number from Q1, Q2, etc. or just number
                        q_num_match = re.search(r'(\d+)', q)
                        if q_num_match:
                            quartile_numbers.append(int(q_num_match.group(1)))
                    if quartile_numbers:
                        quartile_num = min(quartile_numbers)
                    else:
                        quartile_num = None
                else:
                    # Single quartile value
                    q_num_match = re.search(r'(\d+)', quartile_val)
                    if q_num_match:
                        quartile_num = int(q_num_match.group(1))
                    else:
                        quartile_num = None
                
                if quartile_num is not None:
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

@st.cache_data
def create_issn_mapping(if_df, cs_df):
    """Create mapping from ISSN to IF and CiteScore data"""
    mapping = {}
    
    # Process IF data (WoS)
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
    
    # Process CS data (Scopus)
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

# Organization name normalization
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

def is_ror_id(text: str) -> bool:
    """Check if text is a valid ROR ID"""
    pattern = r'^[a-z0-9]{9,10}$'
    return bool(re.match(pattern, text.strip()))

def add_to_recent_institutions(inst: Dict):
    """Add institution to recent list"""
    recent = st.session_state['recent_institutions']
    
    # Check if already exists
    for i, existing in enumerate(recent):
        if existing['ror'] == inst['ror']:
            # Move to front
            recent.pop(i)
            recent.insert(0, inst)
            break
    else:
        # Add new
        recent.insert(0, inst)
    
    # Keep only last 5
    st.session_state['recent_institutions'] = recent[:5]

# Organization search functions
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

def get_institution_by_ror(ror_id: str) -> Optional[Dict]:
    """Get institution by ROR ID"""
    params = {
        'filter': f'ror:{ror_id}',
        'mailto': CROSSREF_EMAIL
    }
    
    try:
        response = requests.get("https://api.openalex.org/institutions", params=params, timeout=15)
        response.raise_for_status()
        data = response.json()
        
        if data and 'results' in data and len(data['results']) > 0:
            inst = data['results'][0]
            return {
                'id': inst.get('id', '').replace('https://openalex.org/', ''),
                'ror': inst.get('ror'),
                'display_name': inst.get('display_name'),
                'country': inst.get('country_code'),
                'type': inst.get('type'),
                'works_count': inst.get('works_count', 0)
            }
    except Exception as e:
        logger.error(f"Error getting institution by ROR {ror_id}: {e}")
    
    return None

def select_organization(org_input):
    """
    Determine organization ROR from input
    Returns (ror_id, error_or_results, org_name, org_country)
    """
    org_input = org_input.strip()
    
    # Check if input is ROR ID
    if is_ror_id(org_input):
        logger.info(f"ROR ID provided: {org_input}")
        inst = get_institution_by_ror(org_input)
        if inst:
            return org_input, None, inst['display_name'], inst.get('country', '')
        else:
            return None, "Organization not found", None, None
    
    # Check if URL with ROR
    if 'ror.org/' in org_input:
        match = re.search(r'ror\.org/([a-zA-Z0-9]+)', org_input)
        if match:
            ror_id = match.group(1)
            logger.info(f"Extracted ROR ID from URL: {ror_id}")
            inst = get_institution_by_ror(ror_id)
            if inst:
                return ror_id, None, inst['display_name'], inst.get('country', '')
            else:
                return None, "Organization not found", None, None
    
    # Search by name
    logger.info(f"Searching organization by name: {org_input}")
    results = search_organization_by_name(org_input)
    
    if not results:
        logger.warning(f"No organizations found: {org_input}")
        return None, "Organization not found", None, None
    
    if len(results) == 1:
        org = results[0]
        ror_id = org['ror'].replace('https://ror.org/', '') if org['ror'] else None
        logger.info(f"Found single organization: {org['display_name']} (ROR: {ror_id})")
        return ror_id, None, org['display_name'], org.get('country', '')
    
    logger.info(f"Found {len(results)} organizations")
    return None, results, None, None

# Year parsing functions
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

def validate_year_range(years: List[int]) -> Tuple[bool, str]:
    """Validate year range for reasonableness"""
    current_year = datetime.now().year
    
    if not years:
        return False, "No years specified"
    
    if min(years) < 1900:
        return False, "Year cannot be before 1900"
    
    if max(years) > current_year + 1:
        return False, f"Year cannot be after {current_year + 1}"
    
    if len(years) > 30:
        return False, "Period cannot exceed 30 years (performance reasons)"
    
    return True, "Valid"

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

def get_total_papers_count(ror: str, years: List[int]) -> int:
    """Get total number of papers for institution in given years (expanded range)"""
    expanded_years = get_expanded_years(years)
    filter_parts = [
        f"institutions.ror:{ror}",
        "has_doi:true",
        years_to_filter_str(expanded_years)
    ]
    filter_str = ",".join(p for p in filter_parts if p)
    
    params = {
        'filter': filter_str,
        'per-page': 1,
        'mailto': CROSSREF_EMAIL
    }
    
    try:
        response = requests.get("https://api.openalex.org/works", params=params, timeout=15)
        response.raise_for_status()
        data = response.json()
        return data.get('meta', {}).get('count', 0)
    except Exception as e:
        logger.error(f"Error getting total papers count: {e}")
        return 0

# Crossref data functions
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
        # Собираем все аффилиации из Crossref
        crossref_affiliations = []
        
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
            
            # Собираем аффилиации из Crossref (как в тестовом коде)
            if 'affiliation' in author:
                for aff in author['affiliation']:
                    if isinstance(aff, dict) and 'name' in aff and aff['name']:
                        aff_name = aff['name'].strip().lower()
                        if aff_name and aff_name not in crossref_affiliations:
                            crossref_affiliations.append(aff_name)
        
        authors_str = '; '.join(authors) if authors else ''
        orcids_str = '; '.join(orcids) if orcids else ''
        authors_count = len(authors)
        
        # Extract ISSN information
        issn_list = []
        
        if 'ISSN' in msg and msg['ISSN']:
            issn_list.extend(msg['ISSN'])
        
        if 'issn-type' in msg and msg['issn-type']:
            for issn_type_item in msg['issn-type']:
                if 'value' in issn_type_item and issn_type_item['value'] not in issn_list:
                    issn_list.append(issn_type_item['value'])
        
        seen = set()
        unique_issns = []
        for issn in issn_list:
            if issn not in seen:
                seen.add(issn)
                unique_issns.append(issn)
        
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
            'type': msg.get('type', ''),
            # Сохраняем все аффилиации из Crossref в нижнем регистре для сравнения
            'crossref_affiliations': crossref_affiliations,
            'has_crossref_affiliations': len(crossref_affiliations) > 0
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
        
        # Get DOI in clean form from OpenAlex
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

def get_openalex_work_details(doi):
    """
    Получение детальной информации о работе из OpenAlex
    Аналог функции из тестового кода
    """
    cache_key = f"openalex_work_details_{doi}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    
    doi_clean = re.sub(r'^(https?://(dx\.)?doi\.org/|doi:?)', '', doi.strip(), flags=re.I)
    
    # Форматируем DOI для OpenAlex
    if doi_clean.startswith('10.'):
        doi_url = f"https://doi.org/{doi_clean}"
    else:
        doi_url = doi_clean
    
    url = f"https://api.openalex.org/works/{doi_url}"
    
    try:
        response = requests.get(url, timeout=15)
        if response.status_code == 200:
            data = response.json()
            cache.set(cache_key, data)
            return data
        else:
            logger.error(f"OpenAlex error for {doi}: status {response.status_code}")
            return None
    except Exception as e:
        logger.error(f"Error getting OpenAlex work details for {doi}: {e}")
        return None

def verify_affiliations_with_crossref(openalex_work, crossref_affiliations, target_org_name):
    """
    Верификация аффилиаций OpenAlex с помощью Crossref
    Аналог функции из тестового кода
    Возвращает: (принадлежит_институту, статус_верификации, детали)
    """
    
    if not openalex_work:
        return False, 'no_openalex_data', []
    
    # Нормализуем название искомого института
    target_lower = target_org_name.lower().strip()
    
    # Получаем авторов из OpenAlex
    openalex_authorships = openalex_work.get('authorships', [])
    
    verified_authors = []
    found_target = False
    verification_status = 'no_match'
    
    for authorship in openalex_authorships:
        author_name = authorship.get('author', {}).get('display_name', 'Unknown')
        
        # Получаем институты из OpenAlex
        openalex_institutions = []
        for inst in authorship.get('institutions', []):
            inst_name = inst.get('display_name', '')
            if inst_name:
                openalex_institutions.append({
                    'name': inst_name,
                    'name_lower': inst_name.lower(),
                    'raw': inst.get('raw_affiliation_string', '')
                })
        
        # Получаем raw affiliation strings
        raw_affiliations = authorship.get('raw_affiliation_strings', [])
        
        # Проверяем каждую аффилиацию
        author_institutions = []
        
        # Если есть данные из Crossref
        if crossref_affiliations:
            # Проверяем институты
            for inst in openalex_institutions:
                inst_lower = inst['name_lower']
                
                # Проверяем, совпадает ли с искомым институтом
                matches_target = (target_lower in inst_lower or inst_lower in target_lower)
                
                # Ищем соответствие в Crossref
                found_in_crossref = any(
                    inst_lower in cr_aff or cr_aff in inst_lower 
                    for cr_aff in crossref_affiliations
                )
                
                inst_info = {
                    'name': inst['name'],
                    'raw': inst['raw'],
                    'matches_target': matches_target,
                    'verified': found_in_crossref
                }
                
                if matches_target:
                    found_target = True
                    if found_in_crossref:
                        inst_info['status'] = '✅ Подтверждено Crossref'
                        verification_status = 'verified'
                    else:
                        inst_info['status'] = '❌ НЕ подтверждено Crossref (ложное срабатывание)'
                        verification_status = 'false_positive'
                
                author_institutions.append(inst_info)
            
            # Проверяем raw affiliations
            for raw_aff in raw_affiliations:
                raw_lower = raw_aff.lower()
                matches_target = (target_lower in raw_lower or raw_lower in target_lower)
                
                if matches_target and not any(inst['raw'] == raw_aff for inst in author_institutions):
                    found_in_crossref = any(
                        cr_aff in raw_lower or raw_lower in cr_aff
                        for cr_aff in crossref_affiliations
                    )
                    
                    inst_info = {
                        'name': 'Raw affiliation',
                        'raw': raw_aff,
                        'matches_target': True,
                        'verified': found_in_crossref
                    }
                    
                    if found_target:
                        if found_in_crossref:
                            inst_info['status'] = '✅ Подтверждено Crossref (raw)'
                            verification_status = 'verified'
                        else:
                            inst_info['status'] = '❌ НЕ подтверждено Crossref (ложное срабатывание, raw)'
                            verification_status = 'false_positive'
                    
                    author_institutions.append(inst_info)
        
        else:
            # Нет данных Crossref - используем OpenAlex
            for inst in openalex_institutions:
                inst_lower = inst['name_lower']
                matches_target = (target_lower in inst_lower or inst_lower in target_lower)
                
                if matches_target:
                    found_target = True
                    verification_status = 'openalex_only'
                    inst_info = {
                        'name': inst['name'],
                        'raw': inst['raw'],
                        'matches_target': True,
                        'verified': False,
                        'status': 'ℹ️ Только OpenAlex (нет данных Crossref)'
                    }
                    author_institutions.append(inst_info)
            
            # Проверяем raw affiliations
            for raw_aff in raw_affiliations:
                raw_lower = raw_aff.lower()
                matches_target = (target_lower in raw_lower or raw_lower in target_lower)
                
                if matches_target and not any(inst.get('raw') == raw_aff for inst in author_institutions):
                    found_target = True
                    verification_status = 'openalex_only'
                    author_institutions.append({
                        'name': 'Raw affiliation',
                        'raw': raw_aff,
                        'matches_target': True,
                        'verified': False,
                        'status': 'ℹ️ Только OpenAlex (нет данных Crossref)'
                    })
        
        verified_authors.append({
            'name': author_name,
            'institutions': author_institutions
        })
    
    # Определяем итоговый статус
    if not found_target:
        return False, 'not_found', verified_authors
    
    if verification_status == 'verified':
        return True, 'verified', verified_authors
    elif verification_status == 'false_positive':
        return False, 'false_positive', verified_authors
    elif verification_status == 'openalex_only':
        return True, 'openalex_only', verified_authors
    else:
        return False, 'unknown', verified_authors

def check_affiliation_match(paper_data: Dict, target_org_name: str) -> Tuple[bool, str]:
    """
    Проверяет, принадлежит ли статья указанному институту.
    Возвращает: (принадлежит, источник_проверки)
    """
    
    logger.debug(f"Checking affiliation for: {target_org_name}")
    
    # Нормализуем название искомого института для сравнения
    target_normalized = normalize_org_name(target_org_name)
    target_words = set(target_normalized.split())
    
    logger.debug(f"Target normalized: '{target_normalized}', words: {target_words}")
    
    # Функция для проверки совпадения с нормализованным названием
    def matches_target(affiliation_name: str) -> bool:
        if not affiliation_name:
            return False
        aff_normalized = normalize_org_name(affiliation_name)
        
        logger.debug(f"Comparing: '{aff_normalized}' with '{target_normalized}'")
        
        # Проверяем точное совпадение
        if aff_normalized == target_normalized:
            logger.debug(f"Exact match found")
            return True
        
        # Проверяем, содержится ли target в аффилиации
        if target_normalized in aff_normalized:
            logger.debug(f"Target found in affiliation")
            return True
        
        # Проверяем, совпадают ли ключевые слова
        aff_words = set(aff_normalized.split())
        common_words = target_words & aff_words
        logger.debug(f"Common words: {common_words}")
        
        # Если совпадает больше 2 слов или все слова target'а
        if len(common_words) >= 2 or common_words == target_words:
            logger.debug(f"Word match found: {common_words}")
            return True
        
        return False
    
    # 1. Проверяем данные из Crossref (приоритет)
    if paper_data.get('has_crossref_affiliations', False):
        crossref_affs = paper_data.get('crossref_affiliations', [])
        logger.debug(f"Crossref affiliations: {crossref_affs}")
        
        for aff in crossref_affs:
            if matches_target(aff):
                logger.debug(f"Affiliation matched in Crossref: {aff}")
                return True, 'crossref'
        # Если в Crossref есть аффилиации, но наша не найдена - статья не принадлежит нам
        logger.debug("No match in Crossref, rejecting")
        return False, 'crossref'
    
    # 2. Если в Crossref нет аффилиаций, проверяем OpenAlex
    logger.debug("No Crossref affiliations, checking OpenAlex")
    oa_affs_str = paper_data.get('affiliations', '')
    if oa_affs_str:
        oa_affs = [a.strip() for a in oa_affs_str.split(';') if a.strip()]
        logger.debug(f"OpenAlex affiliations: {oa_affs}")
        
        for aff in oa_affs:
            if matches_target(aff):
                logger.debug(f"Affiliation matched in OpenAlex: {aff}")
                return True, 'openalex'
        # Если в OpenAlex есть аффилиации, но наша не найдена
        logger.debug("No match in OpenAlex, rejecting")
        return False, 'openalex'
    
    # 3. Нет данных ни в одном источнике
    logger.debug("No affiliation data available")
    return False, 'none'

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
                # Гарантируем, что значение строго в пределах [0.0, 1.0]
                progress = max(0.0, min(progress, 1.0))
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

# Parallel DOI processing
def process_doi_complete(doi, target_ror=None, target_org_name=None):
    """
    Complete DOI processing with affiliation verification
    """
    result = {
        'doi': doi,
        'status': 'processing'
    }
    
    # Получаем данные из Crossref
    cr_data = get_crossref_data(doi)
    if cr_data:
        result.update(cr_data)
    else:
        result['status'] = 'crossref_error'
        return result
    
    # Получаем детальные данные из OpenAlex
    oa_work = get_openalex_work_details(doi)
    
    if oa_work:
        # Добавляем базовые данные из OpenAlex
        oa_data = get_openalex_data(doi, target_ror)
        if oa_data:
            result.update(oa_data)
        
        # Верифицируем аффилиации если указан институт
        if target_org_name:
            belongs, verification_status, verified_authors = verify_affiliations_with_crossref(
                oa_work, 
                result.get('crossref_affiliations', []),
                target_org_name
            )
            
            result['belongs_to_org'] = belongs
            result['verification_status'] = verification_status
            result['verified_authors'] = json.dumps(verified_authors, ensure_ascii=False) if verified_authors else ''
            
            logger.debug(f"DOI {doi}: belongs={belongs}, status={verification_status}")
        else:
            result['belongs_to_org'] = True
            result['verification_status'] = 'not_checked'
        
        result['status'] = 'success'
    else:
        # Если OpenAlex не ответил, но есть Crossref данные
        if target_org_name and result.get('crossref_affiliations'):
            # Проверяем наличие института в Crossref
            target_lower = target_org_name.lower()
            found_in_crossref = any(
                target_lower in aff or aff in target_lower 
                for aff in result['crossref_affiliations']
            )
            result['belongs_to_org'] = found_in_crossref
            result['verification_status'] = 'crossref_only' if found_in_crossref else 'not_in_crossref'
        else:
            result['belongs_to_org'] = False
            result['verification_status'] = 'no_openalex_data'
        
        result['status'] = 'openalex_error'
    
    return result

def process_dois_parallel(dois, target_ror=None, target_org_name=None, max_workers=MAX_WORKERS):
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
            future_to_doi = {
                executor.submit(process_doi_complete, doi, target_ror, target_org_name): doi 
                for doi in remaining_dois
            }
            
            completed = 0
            for future in concurrent.futures.as_completed(future_to_doi):
                doi = future_to_doi[future]
                try:
                    res = future.result()
                    if res.get('status') == 'success' or res.get('status') == 'openalex_error':
                        # Принимаем даже если OpenAlex ошибся, но есть Crossref данные
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
                progress = max(0.0, min(progress, 1.0))
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

# Analysis functions
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

def check_issn_in_mapping(issn_str, issn_mapping):
    """
    Check if any ISSN matches WoS or Scopus databases.
    Returns: (wos_info, scopus_info)
    """
    wos_info = {'indexed': False, 'if': None, 'quartile': None, 'title': None}
    scopus_info = {'indexed': False, 'citescore': None, 'quartile': None, 'title': None}
    
    if pd.isna(issn_str) or not issn_str:
        return wos_info, scopus_info
    
    # Split multiple ISSNs
    issns = [i.strip() for i in str(issn_str).split(';')]
    
    for issn in issns:
        # Try different formats
        issn_variants = [issn]
        
        # Add normalized version (without hyphen)
        normalized = normalize_issn(issn)
        if normalized and normalized != issn:
            issn_variants.append(normalized)
        
        # Add formatted version (with hyphen)
        formatted = format_issn_with_hyphen(issn)
        if formatted and formatted != issn and formatted != normalized:
            issn_variants.append(formatted)
        
        for issn_var in issn_variants:
            if issn_var in issn_mapping:
                mapping = issn_mapping[issn_var]
                
                # WoS info
                if 'if' in mapping and mapping['if']:
                    wos_info = {
                        'indexed': True,
                        'if': mapping['if'],
                        'quartile': mapping.get('if_quartile', ''),
                        'title': mapping.get('if_name', '')
                    }
                
                # Scopus info
                if 'cs' in mapping and mapping['cs']:
                    scopus_info = {
                        'indexed': True,
                        'citescore': mapping['cs'],
                        'quartile': mapping.get('cs_quartile', ''),
                        'title': mapping.get('cs_title', '')
                    }
    
    return wos_info, scopus_info

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
            # Try different formats
            issn_variants = [issn]
            
            # Add normalized version (without hyphen)
            normalized = normalize_issn(issn)
            if normalized and normalized != issn:
                issn_variants.append(normalized)
            
            # Add formatted version (with hyphen)
            formatted = format_issn_with_hyphen(issn)
            if formatted and formatted != issn and formatted != normalized:
                issn_variants.append(formatted)
            
            for issn_var in issn_variants:
                if issn_var in issn_mapping:
                    mapping = issn_mapping[issn_var]
                    
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
    
    # Add WoS and Scopus indexing flags
    def get_indexing_flags(row):
        wos_indexed = False
        scopus_indexed = False
        
        if pd.notna(row.get('IF')) and row['IF']:
            wos_indexed = True
        if pd.notna(row.get('CS')) and row['CS']:
            scopus_indexed = True
        
        return pd.Series({
            'wos_indexed': wos_indexed,
            'scopus_indexed': scopus_indexed
        })
    
    indexing_df = df.apply(get_indexing_flags, axis=1)
    df = pd.concat([df, indexing_df], axis=1)
    
    return df

def create_results_dataframe(results, target_years_set):
    """Create final DataFrame with results"""
    
    df = pd.DataFrame(results)
    
    if df.empty:
        return df
    
    # Убеждаемся, что есть все необходимые колонки
    if 'belongs_to_org' not in df.columns:
        df['belongs_to_org'] = True
    if 'verification_status' not in df.columns:
        df['verification_status'] = 'unknown'
    
    # Проверяем принадлежность к периоду
    df['belongs_to_period'] = df.apply(
        lambda row: row['late_dt'] is not None and row['late_dt'].year in target_years_set 
        if pd.notna(row['late_dt']) else False, 
        axis=1
    )
    
    # Флаг для включения в анализ:
    # 1. Статья в нужном периоде
    # 2. И принадлежит институту (с проверкой)
    # 3. И не является ложным срабатыванием
    df['include_in_analysis'] = df.apply(
        lambda row: (
            row['belongs_to_period'] and 
            row['belongs_to_org'] and 
            row.get('verification_status') != 'false_positive'
        ),
        axis=1
    )
    
    # Добавляем понятное описание статуса верификации
    status_descriptions = {
        'verified': '✅ Подтверждено Crossref',
        'false_positive': '❌ Ложное срабатывание OpenAlex',
        'openalex_only': '⚠️ Только OpenAlex (нет данных Crossref)',
        'crossref_only': '✅ Только Crossref',
        'not_in_crossref': '❌ Не найдено в Crossref',
        'not_found': '❌ Институт не найден',
        'no_openalex_data': '⚠️ Нет данных OpenAlex',
        'unknown': '❓ Статус неизвестен',
        'not_checked': 'ℹ️ Проверка не выполнялась'
    }
    
    df['verification_description'] = df['verification_status'].map(
        lambda x: status_descriptions.get(x, x)
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
        'doi', 'title', 'include_in_analysis', 'belongs_to_period', 'belongs_to_org',
        'verification_status', 'verification_description', 'late_date', 'late_year',
        'print_date', 'online_date', 'publication_year', 'publication_date',
        'authors', 'authors_count', 'orcids',
        'affiliations', 'countries', 'journal', 'issn', 'publisher', 'type',
        'volume', 'issue', 'pages', 'IF', 'IF_Q', 'CS', 'CS_Q',
        'wos_indexed', 'scopus_indexed', 'is_oa', 'funding',
        'references_count', 'citations_cr', 'citations_oa',
        'openalex_id', 'language', 'status', 'verified_authors'
    ]
    
    existing_cols = [col for col in column_order if col in df.columns]
    df = df[existing_cols]
    
    return df

# Run analysis with progress
def run_analysis_with_progress(ror, years, total_estimated, progress_container, status_container):
    """Run complete analysis with progress tracking"""
    try:
        # Parse years
        orig_years_set = set(years)
        exp_years = get_expanded_years(years)
        
        status_container.text("Loading data from OpenAlex...")
        
        papers_to_fetch = min(total_estimated, MAX_PAPERS_TO_ANALYZE)
        status_container.text(f"Loading up to {papers_to_fetch:,} papers...")
        
        # Collect DOIs
        dois, err = fetch_all_dois_openalex(ror, exp_years)
        
        if err:
            status_container.text(f"❌ Error: {err}")
            return False
        elif not dois:
            status_container.text("❌ No publications with DOI in expanded period")
            return False
        
        status_container.text(f"✅ Unique DOIs collected: {len(dois):,}")
        
        # Process DOIs
        status_container.text("Retrieving enhanced data from Crossref and OpenAlex...")
        status_container.text(f"   → Using {MAX_WORKERS} parallel threads")
        status_container.text(f"   → Max retries: {MAX_RETRIES}")
        
        results, errors = process_dois_parallel(
            dois, 
            target_ror=ror,
            target_org_name=st.session_state.selected_org_name
        )
        
        if results:
            df = create_results_dataframe(results, orig_years_set)
            
            # Логируем статистику по источникам аффилиаций
            if 'affiliation_source' in df.columns:
                source_stats = df['affiliation_source'].value_counts()
                logger.info(f"Affiliation sources: {source_stats.to_dict()}")
            
            st.session_state.results_df = df
            st.session_state.errors_list = errors
            st.session_state.orig_years_list = years
            st.session_state.exp_years = exp_years
            st.session_state.analysis_complete = True
            st.session_state.papers_data = df.to_dict('records')
            
            # Generate validation stats
            validation_stats = {
                'total': len(dois),
                'with_doi': len(dois),
                'validated': len(results),
                'kept': len(df[df['include_in_analysis'] == True]),  # Используем include_in_analysis
                'rejected': len(df[df['include_in_analysis'] == False]),
                'no_doi': 0,
                'not_found': len(errors),
                'year_mismatch': 0,
                'affiliation_mismatch': len(df[df['belongs_to_period'] & ~df['belongs_to_org']])  # Добавляем статистику по аффилиациям
            }
            st.session_state.validation_stats = validation_stats
            
            status_container.text(f"""
            ✅ Processing complete!
            - Successful: {len(results):,}
            - Errors: {len(errors):,}
            """)
            
            return True
        else:
            status_container.text("❌ No successfully processed papers")
            return False
        
    except Exception as e:
        status_container.text(f"❌ Error: {str(e)}")
        return False

# Excel export function
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
            'Average citations (OpenAlex) (in period)',
            'WoS Indexed (in period)',
            'Scopus Indexed (in period)',
            'Indexed in Both (in period)'
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
            belong['citations_oa'].mean() if not belong.empty else 0,
            belong['wos_indexed'].sum() if not belong.empty else 0,
            belong['scopus_indexed'].sum() if not belong.empty else 0,
            ((belong['wos_indexed'] & belong['scopus_indexed']).sum()) if not belong.empty else 0
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
    
    # ===== SHEET 10: Database Indexing =====
    indexing_stats = pd.DataFrame({
        'Metric': ['WoS Indexed', 'Scopus Indexed', 'Both Databases', 'Neither'],
        'Count': [
            belong['wos_indexed'].sum() if not belong.empty else 0,
            belong['scopus_indexed'].sum() if not belong.empty else 0,
            ((belong['wos_indexed'] & belong['scopus_indexed']).sum()) if not belong.empty else 0,
            len(belong) - ((belong['wos_indexed'] | belong['scopus_indexed']).sum()) if not belong.empty else 0
        ],
        'Percentage': [
            (belong['wos_indexed'].sum() / len(belong) * 100) if not belong.empty else 0,
            (belong['scopus_indexed'].sum() / len(belong) * 100) if not belong.empty else 0,
            ((belong['wos_indexed'] & belong['scopus_indexed']).sum() / len(belong) * 100) if not belong.empty else 0,
            ((len(belong) - (belong['wos_indexed'] | belong['scopus_indexed']).sum()) / len(belong) * 100) if not belong.empty else 0
        ]
    })
    indexing_stats.to_excel(writer, sheet_name='Database Indexing', index=False)
    
    # ===== SHEET 11: Network Statistics (Affiliations) =====
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
    
    # ===== SHEET 12: Network Statistics (Countries) =====
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
    
    # ===== SHEET 13: Errors =====
    if errors:
        errors_df = pd.DataFrame({'DOI': errors, 'Status': 'Failed'})
        errors_df.to_excel(writer, sheet_name='Errors', index=False)
    
    # ===== SHEET 14: Metadata =====
    metadata = pd.DataFrame({
        'Parameter': [
            'Analysis Date',
            'ROR ID',
            'Organization Name',
            'Original Period',
            'Search Period',
            'Total DOIs Found',
            'Successfully Processed',
            'Failed DOIs',
            'WoS Indexed (in period)',
            'Scopus Indexed (in period)',
            'Cache Directory',
            'Log File',
            'Excel File'
        ],
        'Value': [
            datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            selected_ror,
            st.session_state.get('selected_org_name', ''),
            ', '.join(map(str, orig_years_list)),
            ', '.join(map(str, exp_years)),
            len(df) + len(errors),
            len(df),
            len(errors),
            belong['wos_indexed'].sum() if not belong.empty else 0,
            belong['scopus_indexed'].sum() if not belong.empty else 0,
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

# Visualization functions
def create_enhanced_visualizations(df):
    """Create enhanced scientific visualizations"""
    colors = st.session_state.color_palette
    plot_palette = st.session_state['plot_palette']
    
    # Filter data for period
    belong = df[df['belongs_to_period'] == True].copy()
    
    if belong.empty:
        return None
    
    figs = {}
    
    # 1. Publications over time
    fig1, ax1 = plt.subplots(figsize=(10, 6))
    year_counts = belong['late_year'].value_counts().sort_index()
    ax1.bar(year_counts.index, year_counts.values, 
            color=plot_palette['categorical'][0], edgecolor='black', linewidth=1)
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
             color=plot_palette['categorical'][1], edgecolor='black', linewidth=1)
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
        ax3.hist(cr_citations, bins=30, color=plot_palette['categorical'][0], 
                edgecolor='black', alpha=0.7)
        ax3.set_xlabel('Citations (Crossref)', fontweight='bold')
        ax3.set_ylabel('Frequency', fontweight='bold')
        ax3.set_title('Crossref Citation Distribution', fontweight='bold')
        ax3.grid(True, alpha=0.3, linestyle='--')
    
    # OpenAlex citations
    oa_citations = belong['citations_oa'].dropna()
    if not oa_citations.empty:
        ax4.hist(oa_citations, bins=30, color=plot_palette['categorical'][1], 
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
                color=plot_palette['categorical'][2], linewidth=2, markersize=8)
        ax6.fill_between(oa_by_year.index, oa_by_year.values, alpha=0.3, color=plot_palette['categorical'][2])
        ax6.set_xlabel('Year', fontweight='bold')
        ax6.set_ylabel('Open Access (%)', fontweight='bold')
        ax6.set_title('Open Access Trend Over Time', fontweight='bold', pad=20)
        ax6.grid(True, alpha=0.3, linestyle='--')
        ax6.set_ylim(0, 100)
        plt.tight_layout()
        figs['oa_trend'] = fig5
    
    # 6. Database indexing comparison
    if 'wos_indexed' in belong.columns and 'scopus_indexed' in belong.columns:
        fig6, ax7 = plt.subplots(figsize=(8, 6))
        
        wos_count = belong['wos_indexed'].sum()
        scopus_count = belong['scopus_indexed'].sum()
        both_count = (belong['wos_indexed'] & belong['scopus_indexed']).sum()
        neither_count = len(belong) - (belong['wos_indexed'] | belong['scopus_indexed']).sum()
        
        categories = ['WoS Only', 'Scopus Only', 'Both', 'Neither']
        values = [
            wos_count - both_count,
            scopus_count - both_count,
            both_count,
            neither_count
        ]
        
        colors_list = [
            plot_palette['categorical'][0],
            plot_palette['categorical'][1],
            plot_palette['categorical'][2],
            plot_palette['categorical'][3] if len(plot_palette['categorical']) > 3 else '#95a5a6'
        ]
        
        bars = ax7.bar(categories, values, color=colors_list, edgecolor='black', linewidth=1)
        ax7.set_ylabel('Number of Publications', fontweight='bold')
        ax7.set_title('Database Indexing Distribution', fontweight='bold', pad=20)
        ax7.grid(True, alpha=0.3, linestyle='--', axis='y')
        
        # Add value labels on bars
        for bar in bars:
            height = bar.get_height()
            ax7.text(bar.get_x() + bar.get_width()/2., height,
                    f'{int(height)}', ha='center', va='bottom')
        
        plt.tight_layout()
        figs['indexing'] = fig6
    
    return figs

# Plotly visualization functions
def apply_scientific_style(fig: go.Figure) -> go.Figure:
    """Apply scientific style to plotly figures"""
    fig.update_layout(
        font=dict(
            family="serif",
            size=10,
        ),
        title_font=dict(
            family="serif",
            size=12,
            weight="bold"
        ),
        title=dict(
            x=0.5,  # Center title
            xanchor='center'
        ),
        plot_bgcolor='white',
        paper_bgcolor='white',
        hoverlabel=dict(
            font_family="serif",
            font_size=10
        ),
        margin=dict(l=60, r=30, t=60, b=60)
    )
    
    fig.update_xaxes(
        showline=True,
        linewidth=1,
        linecolor='black',
        mirror=True,
        ticks='outside',
        tickwidth=1,
        tickcolor='black',
        ticklen=4,
        gridcolor='lightgrey',
        griddash='dot',
        gridwidth=0.5,
        showgrid=False,
        title_font=dict(family="serif", size=11, weight="bold"),
        tickfont=dict(family="serif", size=10)
    )
    
    fig.update_yaxes(
        showline=True,
        linewidth=1,
        linecolor='black',
        mirror=True,
        ticks='outside',
        tickwidth=1,
        tickcolor='black',
        ticklen=4,
        gridcolor='lightgrey',
        griddash='dot',
        gridwidth=0.5,
        showgrid=False,
        title_font=dict(family="serif", size=11, weight="bold"),
        tickfont=dict(family="serif", size=10)
    )
    
    return fig

def plot_yearly_publications(yearly_data: Dict[int, int], plot_palette: Dict):
    """Plot yearly publications"""
    years = sorted(yearly_data.keys())
    counts = [yearly_data[y] for y in years]
    
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=years,
        y=counts,
        marker_color=plot_palette['categorical'][0],
        marker_line_color='black',
        marker_line_width=1,
        name='Publications'
    ))
    
    fig.update_layout(
        title='Publications by Year',
        xaxis_title='Year',
        yaxis_title='Number of Publications',
        hovermode='x',
        showlegend=False
    )
    
    fig = apply_scientific_style(fig)
    fig.update_xaxes(tickangle=45)
    return fig

def plot_database_comparison(df, plot_palette: Dict):
    """Plot database indexing comparison"""
    belong = df[df['belongs_to_period'] == True].copy()
    
    if belong.empty:
        return None
    
    wos_count = belong['wos_indexed'].sum()
    scopus_count = belong['scopus_indexed'].sum()
    both_count = (belong['wos_indexed'] & belong['scopus_indexed']).sum()
    
    fig = go.Figure()
    
    categories = ['WoS', 'Scopus', 'Both']
    values = [wos_count, scopus_count, both_count]
    
    colors = [
        plot_palette['categorical'][0],
        plot_palette['categorical'][1],
        plot_palette['categorical'][2]
    ]
    
    fig.add_trace(go.Bar(
        x=categories,
        y=values,
        marker_color=colors,
        marker_line_color='black',
        marker_line_width=1,
        text=values,
        textposition='auto',
    ))
    
    fig.update_layout(
        title='Database Indexing Comparison',
        xaxis_title='Database',
        yaxis_title='Number of Publications',
        showlegend=False
    )
    
    fig = apply_scientific_style(fig)
    return fig

def plot_quartile_distribution(df, database, plot_palette: Dict):
    """Plot quartile distribution for WoS or Scopus"""
    belong = df[df['belongs_to_period'] == True].copy()
    
    if database == 'WoS':
        quartiles = belong[belong['wos_indexed']]['IF_Q'].dropna()
        title = 'WoS Quartile Distribution'
        color_idx = 0
    else:
        quartiles = belong[belong['scopus_indexed']]['CS_Q'].dropna()
        title = 'Scopus Quartile Distribution'
        color_idx = 1
    
    if quartiles.empty:
        return None
    
    quartile_counts = quartiles.value_counts()
    # Ensure all Q1-Q4 are present
    for q in ['Q1', 'Q2', 'Q3', 'Q4']:
        if q not in quartile_counts.index:
            quartile_counts[q] = 0
    
    # Sort in order Q1, Q2, Q3, Q4
    quartile_counts = quartile_counts.sort_index()
    
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=quartile_counts.index,
        y=quartile_counts.values,
        marker_color=plot_palette['categorical'][color_idx],
        marker_line_color='black',
        marker_line_width=1,
        text=quartile_counts.values,
        textposition='auto',
    ))
    
    fig.update_layout(
        title=title,
        xaxis_title='Quartile',
        yaxis_title='Number of Papers'
    )
    
    fig = apply_scientific_style(fig)
    return fig

# Main content area - Step 1: Organization Search
if st.session_state.step == 1:
    st.markdown("## 🔍 Step 1: Organization Search")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("""
        <div class="info-box">
        Enter an organization name or ROR ID to analyze its publications.
        
        **Examples:**
        - ROR ID: `05wv0v765`
        - URL: `https://ror.org/05wv0v765`
        - Name: `Ural Federal University`
        - Name with hyphen: `Institute of High-Temperature Electrochemistry`
        </div>
        """, unsafe_allow_html=True)
        
        # Use value from session_state for display
        query = st.text_input(
            "Organization or ROR ID",
            value=st.session_state['search_query'],
            placeholder="Enter name or ROR ID...",
            key="inst_query_input"
        )
        # Update session_state on change
        if query != st.session_state['search_query']:
            st.session_state['search_query'] = query
            # Reset results when query changes
            st.session_state['search_performed'] = False
            st.session_state.org_search_results = None
        
        col_search1, col_search2 = st.columns([1, 3])
        with col_search1:
            search_clicked = st.button("🔍 Search", type="primary", use_container_width=True)
        
        # Perform search only when Search button is clicked
        if search_clicked and query:
            with st.spinner("Searching for organization..."):
                ror_id, error, org_name, org_country = select_organization(query)
                
                if ror_id:
                    # Single result found
                    st.session_state.selected_ror = ror_id
                    st.session_state.selected_org_name = org_name
                    st.session_state.selected_org_country = org_country
                    
                    add_to_recent_institutions({
                        'ror': ror_id,
                        'name': org_name,
                        'country': org_country
                    })
                    
                    st.session_state.step = 2
                    st.rerun()
                elif error and error != "Organization not found":
                    # Multiple results
                    st.session_state.org_search_results = error
                    st.session_state.search_performed = True
                else:
                    # No results
                    st.session_state.org_search_results = []
                    st.session_state.search_performed = True
                    st.markdown(f"""
                    <div class="error-box">
                        ❌ Organization not found. Try:
                        - Using a more general name
                        - Checking spelling
                        - Using ROR ID
                    </div>
                    """, unsafe_allow_html=True)
        
        # Display search results
        if st.session_state.search_performed and st.session_state.org_search_results is not None:
            results = st.session_state.org_search_results
            
            if results and isinstance(results, list) and len(results) > 0:
                st.markdown("### Found organizations:")
                
                # Initialize expanded details state if not exists
                if 'expanded_details' not in st.session_state:
                    st.session_state['expanded_details'] = {}
                
                for i, org in enumerate(results):
                    # Create a unique key for this organization
                    org_key = f"{org['ror']}_{i}"
                    
                    # Create a container for each organization
                    org_container = st.container()
                    
                    with org_container:
                        col_a, col_b, col_c = st.columns([3, 1, 1])
                        
                        with col_a:
                            st.markdown(f"**{org['display_name']}**")
                            st.markdown(f"ROR: {org['ror']} | Country: {org.get('country', 'N/A')} | Works: {org['works_count']:,}")
                        
                        with col_b:
                            # Select button - sets organization and moves to step 2
                            if st.button("Select", key=f"select_{org_key}", use_container_width=True):
                                ror_id = org['ror'].replace('https://ror.org/', '') if org['ror'] else None
                                st.session_state.selected_ror = ror_id
                                st.session_state.selected_org_name = org['display_name']
                                st.session_state.selected_org_country = org.get('country', '')
                                
                                add_to_recent_institutions({
                                    'ror': ror_id,
                                    'name': org['display_name'],
                                    'country': org.get('country', '')
                                })
                                
                                st.session_state.step = 2
                                st.rerun()
                        
                        with col_c:
                            # Details button - toggles details without rerun
                            if st.button("Details", key=f"details_{org_key}", use_container_width=True):
                                # Toggle details for this organization
                                if org_key in st.session_state['expanded_details']:
                                    st.session_state['expanded_details'][org_key] = not st.session_state['expanded_details'][org_key]
                                else:
                                    st.session_state['expanded_details'][org_key] = True
                        
                        # Show details if expanded
                        if st.session_state['expanded_details'].get(org_key, False):
                            st.markdown(f"""
                            <div style="background-color: rgba(255,255,255,0.05); padding: 1rem; border-radius: 8px; margin: 0.5rem 0;">
                                <h4>📋 Detailed Information</h4>
                                <p><strong>Full Name:</strong> {org['display_name']}</p>
                                <p><strong>ROR ID:</strong> {org['ror']}</p>
                                <p><strong>OpenAlex ID:</strong> {org['id']}</p>
                                <p><strong>Country:</strong> {org.get('country', 'N/A')}</p>
                                <p><strong>Type:</strong> {org.get('type', 'N/A')}</p>
                                <p><strong>Total Works:</strong> {org['works_count']:,}</p>
                                <p><strong>Total Citations:</strong> {org['cited_by_count']:,}</p>
                                <p><em>Click 'Select' to analyze this organization.</em></p>
                            </div>
                            """, unsafe_allow_html=True)
                        
                        st.markdown("---")
    
    with col2:
        st.markdown("### ℹ️ Search Tips")
        st.markdown("""
        **ROR ID format:**
        - `05wv0v765`
        - `https://ror.org/05wv0v765`
        
        **Name search tips:**
        - Use full official names
        - Try without "University" or "Institute"
        - Include country for better results
        - Check spelling
        
        **Examples:**
        - `Ural Federal University`
        - `Institute of High-Temperature Electrochemistry`
        - `Massachusetts Institute of Technology`
        """)
        
        # Recent institutions
        if st.session_state['recent_institutions']:
            st.markdown("### 🕒 Recent Institutions")
            for inst in st.session_state['recent_institutions']:
                if st.button(
                    f"🏛️ {inst['name'][:30]}...",
                    key=f"recent_{inst['ror']}",
                    help=f"ROR: {inst['ror']}",
                    use_container_width=True
                ):
                    st.session_state.selected_ror = inst['ror']
                    st.session_state.selected_org_name = inst['name']
                    st.session_state.selected_org_country = inst.get('country', '')
                    st.session_state.step = 2
                    st.rerun()
        
        st.markdown("---")
        
        # Database status
        st.markdown("### 📚 Database Status")
        
        if st.session_state.if_df is not None:
            st.success("✅ WoS (IF.xlsx) loaded")
        else:
            st.warning("⚠️ WoS (IF.xlsx) not found")
        
        if st.session_state.cs_df is not None:
            st.success("✅ Scopus (CS.xlsx) loaded")
        else:
            st.warning("⚠️ Scopus (CS.xlsx) not found")
        
        st.markdown("---")
        
        # Plot color palette selector
        st.markdown("### 🎨 Plot Colors")
        palette_names = [p['name'] for p in PLOT_COLOR_PALETTES]
        selected_palette_idx = palette_names.index(st.session_state['plot_palette']['name']) if st.session_state['plot_palette']['name'] in palette_names else 0
        
        selected_palette_name = st.selectbox(
            "Color scheme for plots",
            options=palette_names,
            index=selected_palette_idx,
            key="plot_palette_selector"
        )
        
        # Update selected palette
        for p in PLOT_COLOR_PALETTES:
            if p['name'] == selected_palette_name:
                st.session_state['plot_palette'] = p
                break
        
        st.markdown("---")
        
        # Cache info
        st.markdown("### 💾 Cache Info")
        cache_size = len([f for f in os.listdir(CACHE_DIR) if f.endswith('.pkl')]) if os.path.exists(CACHE_DIR) else 0
        st.info(f"📁 Cached items: {cache_size}")

# Step 2: Period Selection
elif st.session_state.step == 2:
    st.markdown("## 📅 Step 2: Analysis Period")
    
    
    if not st.session_state.selected_ror:
        st.warning("Please select an organization first.")
        if st.button("← Back to Search", use_container_width=True):
            st.session_state.step = 1
            st.rerun()
    else:
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.markdown(f"""
            <div class="info-box">
            <strong>Selected Organization:</strong> {st.session_state.selected_org_name}<br>
            <strong>ROR:</strong> {st.session_state.selected_ror}<br>
            <strong>Country:</strong> {st.session_state.selected_org_country or 'N/A'}
            </div>
            """, unsafe_allow_html=True)
            
            st.markdown("""
            **Select analysis period:**
            
            **Input formats:**
            - Single year: `2023`
            - Range: `2022-2024`
            - Mixed: `2021,2023-2025`
            
            *Note: Period limited to 30 years for performance. The system will search ±1 year around your selected period to ensure complete data collection.*
            """)
            
            def on_year_input_change():
                st.session_state['year_input_text'] = st.session_state['year_input_widget']
            
            year_input = st.text_input(
                "Analysis Period",
                value=st.session_state['year_input_text'],
                placeholder="e.g., 2020-2024 or 2023,2025-2026",
                key="year_input_widget",
                on_change=on_year_input_change
            )
            
            col_btn1, col_btn2, col_btn3 = st.columns([1, 1, 2])
            
            with col_btn1:
                if st.button("← Back", use_container_width=True):
                    st.session_state.step = 1
                    st.rerun()
            
            with col_btn2:
                if st.button("Check Availability", type="secondary", use_container_width=True):
                    if year_input:
                        years = parse_year_input(year_input)
                        if years:
                            is_valid, message = validate_year_range(years)
                            if not is_valid:
                                st.markdown(f"""
                                <div class="error-box">
                                    ❌ {message}
                                </div>
                                """, unsafe_allow_html=True)
                            else:
                                with st.spinner("Checking data availability..."):
                                    total = get_total_papers_count(st.session_state.selected_ror, years)
                                    
                                    st.session_state.orig_years_list = years
                                    st.session_state.total_papers_estimate = total
                                    
                                    if total > 0:
                                        expanded = get_expanded_years(years)
                                        st.session_state.exp_years = expanded
                                        
                                        if total > WARN_PAPERS_THRESHOLD:
                                            st.markdown(f"""
                                            <div class="warning-box">
                                                <strong>⚠️ Large Dataset Warning</strong><br>
                                                Found {total:,} papers. Analysis will be limited to {MAX_PAPERS_TO_ANALYZE:,} papers for performance.
                                                This may take several minutes.
                                            </div>
                                            """, unsafe_allow_html=True)
                                        else:
                                            st.markdown(f"""
                                            <div class="success-box">
                                                <strong>✅ Data found</strong><br>
                                                Total papers: {total:,}<br>
                                                Search period: {min(expanded)}-{max(expanded)}
                                            </div>
                                            """, unsafe_allow_html=True)
                                    else:
                                        st.markdown(f"""
                                        <div class="warning-box">
                                            ⚠️ No publications found for this period
                                        </div>
                                        """, unsafe_allow_html=True)
                        else:
                            st.markdown("""
                            <div class="error-box">
                                ❌ Invalid period format
                            </div>
                            """, unsafe_allow_html=True)
            
            with col_btn3:
                if st.session_state.total_papers_estimate > 0:
                    if st.button("▶️ Start Analysis", type="primary", use_container_width=True):
                        with st.spinner("Starting analysis..."):
                            progress_container = st.empty()
                            status_container = st.empty()
                            
                            success = run_analysis_with_progress(
                                st.session_state.selected_ror,
                                st.session_state.orig_years_list,
                                st.session_state.total_papers_estimate,
                                progress_container,
                                status_container
                            )
                            
                            if success:
                                st.session_state.step = 3
                                st.rerun()
        
        with col2:
            st.markdown("### ℹ️ Period Tips")
            st.markdown("""
            **Why search ±1 year?**
            
            OpenAlex uses publication year from metadata, which may differ from the actual publication date. Searching ±1 year ensures we capture all relevant papers.
            
            **After collection:**
            
            Papers are validated against Crossref data and filtered to exactly match your selected years.
            
            **Examples:**
            - `2023` → searches 2022-2024, filters to 2023
            - `2020-2022` → searches 2019-2023, filters to 2020-2022
            """)

# Step 3: Results
elif st.session_state.step == 3 and st.session_state.analysis_complete:
    st.markdown("## 📊 Step 3: Analysis Results")
    
    
    # Navigation buttons
    col1, col2, col3 = st.columns([1, 1, 4])
    with col1:
        if st.button("← Back to Period", use_container_width=True):
            st.session_state.step = 2
            st.rerun()
    with col2:
        if st.button("🔄 New Search", use_container_width=True):
            palette = st.session_state.color_palette
            plot_palette = st.session_state['plot_palette']
            recent = st.session_state['recent_institutions']
            for key in list(st.session_state.keys()):
                if key not in ['color_palette', 'plot_palette', 'recent_institutions']:
                    del st.session_state[key]
            st.session_state.color_palette = palette
            st.session_state['plot_palette'] = plot_palette
            st.session_state['recent_institutions'] = recent
            st.session_state.step = 1
            st.rerun()
    
    df = st.session_state.results_df
    validation = st.session_state.validation_stats
    belong = df[df['belongs_to_period'] == True].copy()
    
    # Summary metrics
    st.markdown("### 📈 Summary Statistics")
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("Total Papers", len(belong))
    
    with col2:
        if 'citations_oa' in belong.columns:
            total_citations = belong['citations_oa'].sum()
            st.metric("Total Citations", f"{total_citations:,}")
    
    with col3:
        if 'citations_oa' in belong.columns:
            avg_citations = belong['citations_oa'].mean()
            st.metric("Avg Citations", f"{avg_citations:.1f}")
    
    with col4:
        wos_count = belong['wos_indexed'].sum() if 'wos_indexed' in belong.columns else 0
        st.metric("WoS Indexed", f"{wos_count:,}")
    
    with col5:
        scopus_count = belong['scopus_indexed'].sum() if 'scopus_indexed' in belong.columns else 0
        st.metric("Scopus Indexed", f"{scopus_count:,}")
    
    # Database filtering
    st.markdown("### 🔍 Filter by Database")
    filter_option = st.radio(
        "Show papers:",
        ["All Papers", "WoS Only", "Scopus Only", "Both Databases", "Neither"],
        horizontal=True,
        key="database_filter"
    )
    
    # Apply filter
    excluded_count = len(belong) - len(belong[belong['include_in_analysis'] == True])
    if excluded_count > 0:
        false_positives = len(belong[belong['verification_status'] == 'false_positive'])
        other_excluded = excluded_count - false_positives
        
        warning_msg = f"⚠️ Исключено статей: {excluded_count}\n"
        if false_positives > 0:
            warning_msg += f"   • Ложные срабатывания OpenAlex: {false_positives}\n"
        if other_excluded > 0:
            warning_msg += f"   • Другие причины: {other_excluded}\n"
        
        st.warning(warning_msg)
    
    # Применяем фильтр по include_in_analysis и затем по выбранной опции
    if filter_option == "All Papers":
        filtered_df = belong[belong['include_in_analysis'] == True].copy()
    elif filter_option == "WoS Only":
        filtered_df = belong[(belong['include_in_analysis'] == True) & (belong['wos_indexed'] == True)].copy()
    elif filter_option == "Scopus Only":
        filtered_df = belong[(belong['include_in_analysis'] == True) & (belong['scopus_indexed'] == True)].copy()
    elif filter_option == "Both Databases":
        filtered_df = belong[(belong['include_in_analysis'] == True) & (belong['wos_indexed'] == True) & (belong['scopus_indexed'] == True)].copy()
    elif filter_option == "Neither":
        filtered_df = belong[(belong['include_in_analysis'] == True) & (belong['wos_indexed'] == False) & (belong['scopus_indexed'] == False)].copy()
    
    st.info(f"Showing {len(filtered_df)} papers ({len(filtered_df)/len(belong)*100:.1f}% of total)")
    
    # Validation stats
    with st.expander("📊 Validation Statistics"):
        # Статистика по верификации
        verification_counts = df['verification_status'].value_counts()
        
        st.markdown(f"""
        <div class="info-box">
        <strong>Date Validation (Crossref):</strong><br>
        - Total papers with DOI: {validation['with_doi']:,}<br>
        - Successfully validated: {validation['validated']:,} ({validation['validated']/validation['with_doi']*100:.1f}%)<br>
        - Kept in period: {validation['kept']:,}<br>
        - Rejected (year mismatch): {validation['rejected']:,}<br>
        - Not found in Crossref: {validation['not_found']:,}
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("### 🔍 Affiliation Verification Results")
        
        col_v1, col_v2, col_v3, col_v4 = st.columns(4)
        
        with col_v1:
            verified_count = verification_counts.get('verified', 0)
            st.metric("✅ Verified by Crossref", verified_count, 
                     help="Аффилиация подтверждена данными Crossref")
        
        with col_v2:
            false_positive = verification_counts.get('false_positive', 0)
            st.metric("❌ False Positives", false_positive,
                     help="OpenAlex ошибся - аффилиация не подтверждена Crossref")
        
        with col_v3:
            openalex_only = verification_counts.get('openalex_only', 0)
            st.metric("⚠️ OpenAlex Only", openalex_only,
                     help="Нет данных Crossref, используется OpenAlex")
        
        with col_v4:
            crossref_only = verification_counts.get('crossref_only', 0)
            st.metric("📚 Crossref Only", crossref_only,
                     help="Только данные Crossref (OpenAlex не ответил)")
        
        # Показываем детальную статистику
        if false_positive > 0:
            st.warning(f"⚠️ Обнаружено {false_positive} ложных срабатываний OpenAlex. Эти статьи исключены из анализа.")
        
        # Таблица со статусами
        status_df = pd.DataFrame([
            {'Status': desc, 'Count': count}
            for status, count in verification_counts.items()
            if (desc := {
                'verified': '✅ Подтверждено Crossref',
                'false_positive': '❌ Ложное срабатывание OpenAlex',
                'openalex_only': '⚠️ Только OpenAlex (нет данных Crossref)',
                'crossref_only': '✅ Только Crossref',
                'not_in_crossref': '❌ Не найдено в Crossref',
                'not_found': '❌ Институт не найден',
                'no_openalex_data': '⚠️ Нет данных OpenAlex',
                'unknown': '❓ Статус неизвестен',
                'not_checked': 'ℹ️ Проверка не выполнялась'
            }.get(status, status))
        ])
        if not status_df.empty:
            st.dataframe(status_df, use_container_width=True)
    
    # Tabs for different views
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "📈 Overview", "📚 Journals", "👥 Authors", "🌍 Collaborations", "📊 Citations", "🔬 Database Analysis"
    ])
    
    with tab1:
        st.markdown("### Publications Timeline")
        
        col_plot1, col_plot2 = st.columns(2)
        
        with col_plot1:
            # Yearly publications
            year_counts = filtered_df['late_year'].value_counts().sort_index()
            fig_yearly = plot_yearly_publications(year_counts.to_dict(), st.session_state['plot_palette'])
            st.plotly_chart(fig_yearly, use_container_width=True)
        
        with col_plot2:
            # Database comparison
            fig_db = plot_database_comparison(filtered_df, st.session_state['plot_palette'])
            if fig_db:
                st.plotly_chart(fig_db, use_container_width=True)
        
        # Matplotlib visualizations
        figs = create_enhanced_visualizations(filtered_df)
        if figs:
            if 'timeline' in figs:
                st.pyplot(figs['timeline'])
            if 'indexing' in figs:
                st.pyplot(figs['indexing'])
    
    with tab2:
        st.markdown("### Journal Analysis")
        
        col_j1, col_j2 = st.columns(2)
        
        with col_j1:
            # Top journals
            journal_freq = filtered_df['journal'].value_counts().head(20)
            fig_journals = go.Figure()
            fig_journals.add_trace(go.Bar(
                y=journal_freq.index[::-1],
                x=journal_freq.values[::-1],
                orientation='h',
                marker_color=st.session_state['plot_palette']['categorical'][1],
                marker_line_color='black',
                marker_line_width=1
            ))
            fig_journals.update_layout(
                title='Top 20 Journals',
                xaxis_title='Number of Publications',
                yaxis_title='Journal',
                height=500
            )
            fig_journals = apply_scientific_style(fig_journals)
            st.plotly_chart(fig_journals, use_container_width=True)
        
        with col_j2:
            # Top publishers
            publisher_freq = filtered_df['publisher'].value_counts().head(15)
            fig_publishers = go.Figure()
            fig_publishers.add_trace(go.Pie(
                labels=publisher_freq.index,
                values=publisher_freq.values,
                marker_colors=st.session_state['plot_palette']['categorical'],
                textinfo='percent+label',
                insidetextorientation='radial'
            ))
            fig_publishers.update_layout(
                title='Top 15 Publishers',
                height=500
            )
            fig_publishers = apply_scientific_style(fig_publishers)
            st.plotly_chart(fig_publishers, use_container_width=True)
        
        # Journal table
        with st.expander("View Journal Frequency Table"):
            journal_df = pd.DataFrame({
                'Journal': journal_freq.index,
                'Publications': journal_freq.values,
                'Percentage': (journal_freq.values / len(filtered_df) * 100).round(1)
            })
            st.dataframe(journal_df, use_container_width=True)
    
    with tab3:
        st.markdown("### Author Analysis")
        
        author_counter = generate_author_frequency(filtered_df)
        top_authors = author_counter.most_common(30)
        
        if top_authors:
            fig_authors = go.Figure()
            authors_display = [a[0][:30] + '...' if len(a[0]) > 30 else a[0] for a in top_authors[:20]]
            counts = [a[1] for a in top_authors[:20]]
            
            fig_authors.add_trace(go.Bar(
                y=authors_display[::-1],
                x=counts[::-1],
                orientation='h',
                marker_color=st.session_state['plot_palette']['categorical'][2],
                marker_line_color='black',
                marker_line_width=1
            ))
            fig_authors.update_layout(
                title='Top 20 Authors',
                xaxis_title='Number of Publications',
                yaxis_title='Author',
                height=600
            )
            fig_authors = apply_scientific_style(fig_authors)
            st.plotly_chart(fig_authors, use_container_width=True)
            
            # Author table
            with st.expander("View Author Frequency Table"):
                author_df = pd.DataFrame(top_authors, columns=['Author', 'Publications'])
                st.dataframe(author_df, use_container_width=True)
        else:
            st.info("No author data available")
    
    with tab4:
        st.markdown("### Collaboration Analysis")
        
        col_c1, col_c2 = st.columns(2)
        
        with col_c1:
            # Countries
            country_counter = generate_country_frequency(filtered_df)
            if country_counter:
                fig_countries = go.Figure()
                countries_display = [c for c, _ in country_counter.most_common(15)]
                country_counts = [v for _, v in country_counter.most_common(15)]
                
                fig_countries.add_trace(go.Bar(
                    x=countries_display,
                    y=country_counts,
                    marker_color=st.session_state['plot_palette']['categorical'][0],
                    marker_line_color='black',
                    marker_line_width=1
                ))
                fig_countries.update_layout(
                    title='Top 15 Countries',
                    xaxis_title='Country',
                    yaxis_title='Number of Publications',
                    xaxis_tickangle=45
                )
                fig_countries = apply_scientific_style(fig_countries)
                st.plotly_chart(fig_countries, use_container_width=True)
        
        with col_c2:
            # Collaboration types
            if 'countries' in filtered_df.columns:
                def get_collab_type(row):
                    countries = extract_countries(row.get('countries', ''))
                    if len(countries) <= 1:
                        return 'Domestic'
                    else:
                        return 'International'
                
                collab_types = filtered_df.apply(get_collab_type, axis=1).value_counts()
                
                fig_collab = go.Figure()
                fig_collab.add_trace(go.Pie(
                    labels=collab_types.index,
                    values=collab_types.values,
                    marker_colors=st.session_state['plot_palette']['categorical'],
                    textinfo='percent+label'
                ))
                fig_collab.update_layout(
                    title='Collaboration Types',
                    height=400
                )
                fig_collab = apply_scientific_style(fig_collab)
                st.plotly_chart(fig_collab, use_container_width=True)
        
        # Country network (if data available)
        if len(country_counter) > 1:
            with st.expander("View Country Collaboration Network"):
                G_country, _ = create_country_network(filtered_df)
                
                if len(G_country.nodes()) > 0:
                    # Create network visualization
                    pos = nx.spring_layout(G_country, k=2, iterations=50)
                    
                    edge_trace = []
                    for edge in G_country.edges():
                        x0, y0 = pos[edge[0]]
                        x1, y1 = pos[edge[1]]
                        edge_trace.append(go.Scatter(
                            x=[x0, x1, None], y=[y0, y1, None],
                            line=dict(width=G_country[edge[0]][edge[1]]['weight'], color='#888'),
                            hoverinfo='none',
                            mode='lines'
                        ))
                    
                    node_x = []
                    node_y = []
                    node_text = []
                    node_size = []
                    
                    for node in G_country.nodes():
                        x, y = pos[node]
                        node_x.append(x)
                        node_y.append(y)
                        node_text.append(f"{node}<br>Papers: {G_country.nodes[node]['papers']}<br>Partners: {G_country.degree(node)}")
                        node_size.append(G_country.nodes[node]['papers'] * 3)
                    
                    node_trace = go.Scatter(
                        x=node_x, y=node_y,
                        mode='markers+text',
                        text=list(G_country.nodes()),
                        textposition="top center",
                        hovertext=node_text,
                        hoverinfo='text',
                        marker=dict(
                            size=node_size,
                            color=st.session_state['plot_palette']['categorical'][0],
                            line=dict(color='darkblue', width=2)
                        )
                    )
                    
                    fig_network = go.Figure(data=edge_trace + [node_trace],
                                           layout=go.Layout(
                                               title='Country Collaboration Network',
                                               showlegend=False,
                                               hovermode='closest',
                                               xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                                               yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                                               height=500
                                           ))
                    
                    fig_network = apply_scientific_style(fig_network)
                    st.plotly_chart(fig_network, use_container_width=True)
    
    with tab5:
        st.markdown("### Citation Analysis")
        
        col_cit1, col_cit2 = st.columns(2)
        
        with col_cit1:
            # Citation distribution
            citations = filtered_df['citations_oa'].dropna()
            if not citations.empty:
                bins = [0, 1, 5, 10, 20, 50, 100, float('inf')]
                labels = ['0', '1-4', '5-9', '10-19', '20-49', '50-99', '100+']
                citation_bins = pd.cut(citations, bins=bins, labels=labels, right=False)
                dist = citation_bins.value_counts().sort_index()
                
                fig_dist = go.Figure()
                fig_dist.add_trace(go.Bar(
                    x=dist.index,
                    y=dist.values,
                    marker_color=st.session_state['plot_palette']['categorical'][0],
                    marker_line_color='black',
                    marker_line_width=1
                ))
                fig_dist.update_layout(
                    title='Citation Distribution',
                    xaxis_title='Citation Range',
                    yaxis_title='Number of Papers'
                )
                fig_dist = apply_scientific_style(fig_dist)
                st.plotly_chart(fig_dist, use_container_width=True)
        
        with col_cit2:
            # Most cited papers
            top_cited = filtered_df.nlargest(10, 'citations_oa')[['title', 'citations_oa', 'late_year', 'journal']]
            if not top_cited.empty:
                fig_top = go.Figure()
                fig_top.add_trace(go.Bar(
                    y=[t[:50] + '...' if len(t) > 50 else t for t in top_cited['title']],
                    x=top_cited['citations_oa'],
                    orientation='h',
                    marker_color=st.session_state['plot_palette']['categorical'][1],
                    marker_line_color='black',
                    marker_line_width=1
                ))
                fig_top.update_layout(
                    title='Top 10 Most Cited Papers',
                    xaxis_title='Citations',
                    yaxis_title='Paper Title',
                    height=400
                )
                fig_top = apply_scientific_style(fig_top)
                st.plotly_chart(fig_top, use_container_width=True)
        
        # Citations over time
        if 'late_year' in filtered_df.columns and 'citations_oa' in filtered_df.columns:
            citations_by_year = filtered_df.groupby('late_year')['citations_oa'].agg(['sum', 'mean', 'count']).reset_index()
            
            fig_cit_year = go.Figure()
            fig_cit_year.add_trace(go.Scatter(
                x=citations_by_year['late_year'],
                y=citations_by_year['sum'],
                mode='lines+markers',
                name='Total Citations',
                line=dict(color=st.session_state['plot_palette']['categorical'][0], width=2),
                marker=dict(size=8)
            ))
            fig_cit_year.add_trace(go.Scatter(
                x=citations_by_year['late_year'],
                y=citations_by_year['mean'],
                mode='lines+markers',
                name='Average Citations',
                line=dict(color=st.session_state['plot_palette']['categorical'][1], width=2),
                marker=dict(size=8),
                yaxis='y2'
            ))
            
            fig_cit_year.update_layout(
                title='Citations by Year',
                xaxis_title='Year',
                yaxis=dict(title='Total Citations'),
                yaxis2=dict(title='Average Citations', overlaying='y', side='right'),
                hovermode='x'
            )
            fig_cit_year = apply_scientific_style(fig_cit_year)
            st.plotly_chart(fig_cit_year, use_container_width=True)
    
    with tab6:
        st.markdown("### Database Analysis")
        
        col_db1, col_db2 = st.columns(2)
        
        with col_db1:
            # WoS Quartile distribution
            fig_wos_q = plot_quartile_distribution(filtered_df, 'WoS', st.session_state['plot_palette'])
            if fig_wos_q:
                st.plotly_chart(fig_wos_q, use_container_width=True)
            else:
                st.info("No WoS-indexed papers with quartile information")
        
        with col_db2:
            # Scopus Quartile distribution
            fig_scopus_q = plot_quartile_distribution(filtered_df, 'Scopus', st.session_state['plot_palette'])
            if fig_scopus_q:
                st.plotly_chart(fig_scopus_q, use_container_width=True)
            else:
                st.info("No Scopus-indexed papers with quartile information")
        
        # WoS vs Scopus comparison table
        st.markdown("### Database Statistics")
        
        wos_papers = filtered_df[filtered_df['wos_indexed'] == True]
        scopus_papers = filtered_df[filtered_df['scopus_indexed'] == True]
        both_papers = filtered_df[(filtered_df['wos_indexed'] == True) & (filtered_df['scopus_indexed'] == True)]
        
        db_stats = pd.DataFrame({
            'Metric': ['Number of Papers', 'Percentage', 'Average Citations', 'Total Citations'],
            'WoS': [
                len(wos_papers),
                f"{len(wos_papers)/len(filtered_df)*100:.1f}%" if len(filtered_df) > 0 else '0%',
                f"{wos_papers['citations_oa'].mean():.1f}" if len(wos_papers) > 0 else 'N/A',
                f"{wos_papers['citations_oa'].sum():,}" if len(wos_papers) > 0 else '0'
            ],
            'Scopus': [
                len(scopus_papers),
                f"{len(scopus_papers)/len(filtered_df)*100:.1f}%" if len(filtered_df) > 0 else '0%',
                f"{scopus_papers['citations_oa'].mean():.1f}" if len(scopus_papers) > 0 else 'N/A',
                f"{scopus_papers['citations_oa'].sum():,}" if len(scopus_papers) > 0 else '0'
            ],
            'Both': [
                len(both_papers),
                f"{len(both_papers)/len(filtered_df)*100:.1f}%" if len(filtered_df) > 0 else '0%',
                f"{both_papers['citations_oa'].mean():.1f}" if len(both_papers) > 0 else 'N/A',
                f"{both_papers['citations_oa'].sum():,}" if len(both_papers) > 0 else '0'
            ]
        })
        
        st.dataframe(db_stats, use_container_width=True)
    
    # Data preview and export
    st.markdown("### 📋 Data Preview")
    st.dataframe(filtered_df.head(10), use_container_width=True)
    
    st.markdown("### 📥 Export Data")
    
    col_exp1, col_exp2, col_exp3 = st.columns(3)
    
    with col_exp1:
        # CSV export
        csv_data = filtered_df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📊 Download CSV",
            data=csv_data,
            file_name=f"analysis_{st.session_state.selected_ror}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
            use_container_width=True
        )
    
    with col_exp2:
        # Excel export
        if st.button("📈 Generate Excel Report", use_container_width=True):
            with st.spinner("Generating Excel report..."):
                filename = export_to_excel(
                    filtered_df,
                    filtered_df.to_dict('records'),
                    st.session_state.errors_list,
                    st.session_state.selected_ror,
                    st.session_state.orig_years_list,
                    st.session_state.exp_years
                )
                
                if os.path.exists(filename):
                    with open(filename, 'rb') as f:
                        excel_data = f.read()
                    
                    st.download_button(
                        label="📥 Download Excel File",
                        data=excel_data,
                        file_name=filename,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True
                    )
    
    with col_exp3:
        # JSON export
        json_data = json.dumps({
            'metadata': {
                'analysis_date': datetime.now().isoformat(),
                'ror_id': st.session_state.selected_ror,
                'organization_name': st.session_state.selected_org_name,
                'original_period': st.session_state.orig_years_list,
                'search_period': st.session_state.exp_years,
                'total_papers': len(filtered_df),
                'wos_indexed': int(filtered_df['wos_indexed'].sum()) if 'wos_indexed' in filtered_df.columns else 0,
                'scopus_indexed': int(filtered_df['scopus_indexed'].sum()) if 'scopus_indexed' in filtered_df.columns else 0
            },
            'papers': filtered_df.to_dict('records')
        }, indent=2, default=str).encode('utf-8')
        
        st.download_button(
            label="📋 Download JSON",
            data=json_data,
            file_name=f"analysis_{st.session_state.selected_ror}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
            mime="application/json",
            use_container_width=True
        )











