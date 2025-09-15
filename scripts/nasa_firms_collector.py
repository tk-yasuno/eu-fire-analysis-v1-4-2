#!/usr/bin/env python3
"""
NASA FIRMS API Real Data Collector
Real wildfire data collection using NASA Fire Information for Resource Management System
"""

import csv
import json
import logging
import os
import requests
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
import pandas as pd
from io import StringIO

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class FIRMSConfig:
    """NASA FIRMS API Configuration"""
    map_key: str = None
    base_url: str = "https://firms.modaps.eosdis.nasa.gov/api"
    request_delay: float = 2.0  # Respect API rate limits
    max_retries: int = 3
    timeout: int = 30
    default_days: int = 7
    min_confidence: str = "nominal"  # low, nominal, high
    max_records_per_request: int = 1000
    
    def __post_init__(self):
        # Load from environment if not provided
        if not self.map_key:
            self.map_key = os.getenv('NASA_FIRMS_MAP_KEY')
        
        if not self.map_key:
            raise ValueError("NASA FIRMS MAP_KEY is required. Get it from: https://firms.modaps.eosdis.nasa.gov/api/map_key/")

class NASAFIRMSCollector:
    """NASA FIRMS Real Fire Data Collector"""
    
    # VIIRS data sources with 375m high resolution
    VIIRS_SOURCES = [
        "VIIRS_SNPP_NRT",    # Suomi NPP Near Real-Time
        "VIIRS_NOAA20_NRT",  # NOAA-20 Near Real-Time  
        "VIIRS_NOAA21_NRT"   # NOAA-21 Near Real-Time
    ]
    
    # Geographic regions for data collection
    REGIONS = {
        "north_america": "-170,24,-50,72",      # USA, Canada, Alaska
        "europe": "-25,35,45,72",               # Europe
        "asia_pacific": "70,-50,180,70",        # Asia Pacific
        "australia": "110,-45,155,-10",         # Australia
        "south_america": "-85,-57,-32,14",      # South America
        "africa": "-20,-35,55,40",              # Africa
        "world": "-180,-90,180,90"              # Global
    }
    
    def __init__(self, config: FIRMSConfig):
        self.config = config
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'DisasterSentiment/1.0 (Academic Research)',
            'Accept': 'text/csv'
        })
        
        # Validate MAP_KEY
        self._validate_map_key()
        
        logger.info(f"Initialized NASA FIRMS collector with MAP_KEY: {self.config.map_key[:8]}...")
    
    def _validate_map_key(self) -> bool:
        """Validate MAP_KEY by checking status"""
        try:
            url = f"{self.config.base_url}/map_key/check_status/{self.config.map_key}"
            response = self.session.get(url, timeout=self.config.timeout)
            
            if response.status_code == 200:
                data = response.text
                logger.info(f"MAP_KEY validation successful: {data}")
                return True
            else:
                logger.error(f"MAP_KEY validation failed: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"Error validating MAP_KEY: {e}")
            return False
    
    def collect_fire_data(self, 
                         region: str = "world",
                         days: int = None,
                         date: str = None,
                         sources: List[str] = None) -> List[Dict[str, Any]]:
        """
        Collect real fire data from NASA FIRMS API
        
        Args:
            region: Geographic region or custom coordinates (west,south,east,north)
            days: Number of days to query (1-10)
            date: Specific date YYYY-MM-DD (optional)
            sources: List of VIIRS sources to query
            
        Returns:
            List of fire detection records
        """
        if days is None:
            days = self.config.default_days
        if sources is None:
            sources = self.VIIRS_SOURCES
            
        # Get region coordinates
        if region in self.REGIONS:
            area_coords = self.REGIONS[region]
        else:
            area_coords = region  # Assume custom coordinates
        
        all_fire_data = []
        
        for source in sources:
            logger.info(f"Collecting {source} data for {region} ({days} days)")
            
            try:
                fire_records = self._query_fire_data(source, area_coords, days, date)
                if fire_records:
                    all_fire_data.extend(fire_records)
                    logger.info(f"Collected {len(fire_records)} records from {source}")
                else:
                    logger.warning(f"No data returned from {source}")
                    
                # Rate limiting
                time.sleep(self.config.request_delay)
                
            except Exception as e:
                logger.error(f"Error collecting data from {source}: {e}")
                continue
        
        # Remove duplicates and apply filters
        filtered_data = self._process_fire_data(all_fire_data)
        
        logger.info(f"Total collected: {len(all_fire_data)} records")
        logger.info(f"After filtering: {len(filtered_data)} records")
        
        return filtered_data
    
    def _query_fire_data(self, 
                        source: str, 
                        area_coords: str, 
                        days: int,
                        date: str = None) -> List[Dict[str, Any]]:
        """Query fire data from specific VIIRS source"""
        
        # Build API URL
        if date:
            url = f"{self.config.base_url}/area/csv/{self.config.map_key}/{source}/{area_coords}/{days}/{date}"
        else:
            url = f"{self.config.base_url}/area/csv/{self.config.map_key}/{source}/{area_coords}/{days}"
        
        logger.debug(f"Querying: {url}")
        
        for attempt in range(self.config.max_retries):
            try:
                response = self.session.get(url, timeout=self.config.timeout)
                
                if response.status_code == 200:
                    # Parse CSV data
                    csv_data = response.text
                    if csv_data.strip():
                        return self._parse_csv_data(csv_data, source)
                    else:
                        logger.warning(f"Empty response from {source}")
                        return []
                        
                elif response.status_code == 429:
                    # Rate limited
                    wait_time = 2 ** attempt
                    logger.warning(f"Rate limited, waiting {wait_time}s before retry")
                    time.sleep(wait_time)
                    continue
                    
                else:
                    logger.error(f"API request failed: {response.status_code} - {response.text}")
                    return []
                    
            except Exception as e:
                logger.error(f"Request attempt {attempt + 1} failed: {e}")
                if attempt < self.config.max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                else:
                    raise e
        
        return []
    
    def _parse_csv_data(self, csv_data: str, source: str) -> List[Dict[str, Any]]:
        """Parse CSV fire data into structured records"""
        
        records = []
        csv_reader = csv.DictReader(StringIO(csv_data))
        
        for row in csv_reader:
            try:
                # Validate required fields
                if not all(key in row for key in ['latitude', 'longitude', 'confidence']):
                    continue
                
                # Convert and validate data types
                record = {
                    'latitude': float(row['latitude']),
                    'longitude': float(row['longitude']),
                    'brightness_ti4': float(row.get('bright_ti4', 0)),
                    'scan': float(row.get('scan', 0)),
                    'track': float(row.get('track', 0)),
                    'acq_date': row.get('acq_date', ''),
                    'acq_time': row.get('acq_time', ''),
                    'satellite': row.get('satellite', ''),
                    'confidence': row.get('confidence', '').lower(),
                    'version': row.get('version', ''),
                    'brightness_ti5': float(row.get('bright_ti5', 0)),
                    'frp': float(row.get('frp', 0)) if row.get('frp') else 0,  # Fire Radiative Power
                    'daynight': row.get('daynight', ''),
                    'source': source,
                    'collection_time': datetime.now().isoformat()
                }
                
                records.append(record)
                
            except (ValueError, KeyError) as e:
                logger.warning(f"Error parsing CSV row: {e}")
                continue
        
        return records
    
    def _process_fire_data(self, fire_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Filter and process fire data"""
        
        filtered_records = []
        
        for record in fire_records:
            # Apply confidence filter
            if self.config.min_confidence == "high" and record['confidence'] not in ['high']:
                continue
            elif self.config.min_confidence == "nominal" and record['confidence'] in ['low']:
                continue
            
            # Validate coordinates
            lat = record['latitude']
            lon = record['longitude']
            if not (-90 <= lat <= 90) or not (-180 <= lon <= 180):
                continue
            
            # Validate brightness temperature (basic range check)
            if record['brightness_ti4'] < 270 or record['brightness_ti4'] > 400:  # Kelvin
                continue
            
            # Add derived fields
            record['fire_intensity'] = self._calculate_fire_intensity(record)
            record['location_description'] = self._get_location_description(lat, lon)
            
            filtered_records.append(record)
        
        # Remove duplicates (same location, date, time)
        unique_records = self._remove_duplicates(filtered_records)
        
        return unique_records
    
    def _calculate_fire_intensity(self, record: Dict[str, Any]) -> str:
        """Calculate fire intensity based on FRP and brightness temperature"""
        frp = record.get('frp', 0)
        brightness = record.get('brightness_ti4', 0)
        
        if frp > 100 or brightness > 350:
            return "high"
        elif frp > 20 or brightness > 330:
            return "moderate"
        else:
            return "low"
    
    def _get_location_description(self, lat: float, lon: float) -> str:
        """Generate location description from coordinates"""
        # Simple geographic region classification
        if lat > 49:
            region = "Northern"
        elif lat > 23.5:
            region = "Temperate"
        elif lat > -23.5:
            region = "Tropical"
        else:
            region = "Southern"
        
        if lon < -100:
            continent = "North America"
        elif lon < -30:
            continent = "South America"
        elif lon < 40:
            continent = "Europe/Africa"
        elif lon < 140:
            continent = "Asia"
        else:
            continent = "Asia-Pacific"
        
        return f"{region} {continent}"
    
    def _remove_duplicates(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Remove duplicate fire detections"""
        seen = set()
        unique_records = []
        
        for record in records:
            # Create unique key based on location, date, and time
            key = (
                round(record['latitude'], 4),
                round(record['longitude'], 4),
                record['acq_date'],
                record['acq_time'][:4]  # Hour and minute only
            )
            
            if key not in seen:
                seen.add(key)
                unique_records.append(record)
        
        return unique_records
    
    def save_fire_data(self, fire_data: List[Dict[str, Any]], output_file: str):
        """Save fire data to CSV file"""
        
        if not fire_data:
            logger.warning("No fire data to save")
            return
        
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Write CSV with all fields
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            if fire_data:
                writer = csv.DictWriter(f, fieldnames=fire_data[0].keys())
                writer.writeheader()
                writer.writerows(fire_data)
        
        logger.info(f"Saved {len(fire_data)} fire records to {output_path}")
    
    def get_fire_summary(self, fire_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Generate summary statistics for fire data"""
        
        if not fire_data:
            return {"error": "No fire data available"}
        
        df = pd.DataFrame(fire_data)
        
        summary = {
            "total_detections": len(fire_data),
            "date_range": {
                "earliest": df['acq_date'].min(),
                "latest": df['acq_date'].max()
            },
            "confidence_distribution": df['confidence'].value_counts().to_dict(),
            "satellite_distribution": df['satellite'].value_counts().to_dict(),
            "source_distribution": df['source'].value_counts().to_dict(),
            "intensity_distribution": df['fire_intensity'].value_counts().to_dict(),
            "geographic_bounds": {
                "min_latitude": df['latitude'].min(),
                "max_latitude": df['latitude'].max(),
                "min_longitude": df['longitude'].min(),
                "max_longitude": df['longitude'].max()
            },
            "fire_radiative_power": {
                "mean": df['frp'].mean(),
                "max": df['frp'].max(),
                "total": df['frp'].sum()
            }
        }
        
        return summary

# Example usage and testing
if __name__ == "__main__":
    # Configuration
    config = FIRMSConfig(
        map_key="your_map_key_here",  # Replace with actual MAP_KEY
        request_delay=2.0,
        default_days=3
    )
    
    try:
        # Initialize collector
        collector = NASAFIRMSCollector(config)
        
        # Collect fire data for North America (past 3 days)
        fire_data = collector.collect_fire_data(
            region="north_america",
            days=3
        )
        
        # Save data
        collector.save_fire_data(fire_data, "output/nasa_firms_fires.csv")
        
        # Generate summary
        summary = collector.get_fire_summary(fire_data)
        print(json.dumps(summary, indent=2, default=str))
        
    except Exception as e:
        logger.error(f"Error in FIRMS data collection: {e}")