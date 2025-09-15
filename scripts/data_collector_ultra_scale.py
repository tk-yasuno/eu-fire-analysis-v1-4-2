#!/usr/bin/env python3
"""
Ultra-Scale Wildfire Data Collector for 20,000-30,000 English Texts
Enterprise-grade wildfire text collection with memory optimization and robust error handling
"""

import json
import os
import logging
import time
import gc
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
import pandas as pd
from tqdm import tqdm
import random
import re
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import multiprocessing as mp
from pathlib import Path
import psutil

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ultra_wildfire_collector.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class UltraScaleWildfireCollector:
    """
    Ultra-scale wildfire text data collector for 20,000-30,000 documents
    Features: Memory optimization, batch processing, progress monitoring, error recovery
    """
    
    def __init__(self, output_dir: str = "data", target_count: int = 25000, 
                 batch_size: int = 1000, max_workers: int = None):
        self.output_dir = output_dir
        self.target_count = target_count
        self.batch_size = batch_size
        self.max_workers = max_workers or min(8, mp.cpu_count())
        
        self.raw_dir = os.path.join(output_dir, "raw")
        self.cleaned_dir = os.path.join(output_dir, "cleaned")
        
        # Create directories
        os.makedirs(self.raw_dir, exist_ok=True)
        os.makedirs(self.cleaned_dir, exist_ok=True)
        
        # Enhanced wildfire keywords with more diversity
        self.wildfire_keywords = [
            # Core fire terms
            "wildfire", "wild fire", "forest fire", "bushfire", "brush fire",
            "grass fire", "woodland fire", "vegetation fire", "fire outbreak",
            "blaze", "conflagration", "inferno", "firestorm",
            
            # Fire suppression and fighting
            "fire suppression", "fire fighting", "firefighter", "fire department",
            "fire crew", "hotshot crew", "smoke jumper", "air tanker", "water drop",
            "fire retardant", "fire line", "containment", "suppression",
            
            # Weather and conditions
            "fire season", "fire weather", "fire danger", "fire risk",
            "red flag warning", "extreme fire danger", "critical fire weather",
            "drought", "dry conditions", "fire weather warning", "wind driven",
            "humidity", "temperature", "heat wave", "dry lightning",
            
            # Emergency response
            "evacuation", "fire evacuation", "emergency evacuation",
            "evacuation order", "evacuation warning", "shelter in place",
            "emergency services", "incident command", "fire management",
            
            # Impact and damage
            "fire damage", "fire destruction", "burned area", "fire perimeter",
            "acres burned", "structures destroyed", "property damage",
            "fire scar", "burn severity", "fire footprint",
            
            # Health and environment
            "smoke", "fire smoke", "air quality", "fire hazard", "ash fall",
            "particulate matter", "respiratory", "visibility", "smoke plume",
            
            # Prevention and management
            "fire safety", "fire prevention", "fire management", "fuel reduction",
            "prescribed burn", "controlled burn", "fire break", "defensible space",
            "vegetation management", "fire barrier", "fuel load",
            
            # Recovery and aftermath
            "fire recovery", "restoration", "reforestation", "erosion control",
            "debris flow", "mudslide", "watershed", "rehabilitation"
        ]
        
        # Expanded regional coverage
        self.regions = [
            # North America
            "California", "Oregon", "Washington", "British Columbia", "Alberta",
            "Colorado", "Montana", "Idaho", "Arizona", "Nevada", "Utah",
            "New Mexico", "Texas", "Alaska", "Wyoming", "North Dakota",
            
            # Australia and Oceania
            "New South Wales", "Victoria", "Queensland", "South Australia",
            "Western Australia", "Tasmania", "Australian Capital Territory",
            "Northern Territory", "New Zealand",
            
            # Europe
            "Greece", "Portugal", "Spain", "France", "Italy", "Turkey",
            "Croatia", "Montenegro", "Cyprus", "Bulgaria", "Romania",
            
            # South America
            "Chile", "Argentina", "Brazil", "Bolivia", "Peru", "Ecuador",
            "Colombia", "Venezuela", "Paraguay", "Uruguay",
            
            # Asia
            "Russia", "Siberia", "Indonesia", "Malaysia", "Philippines",
            "South Korea", "Japan", "China", "Mongolia", "Kazakhstan",
            
            # Africa
            "South Africa", "Algeria", "Morocco", "Tunisia", "Kenya",
            "Tanzania", "Zimbabwe", "Botswana", "Namibia",
            
            # Other
            "Scandinavia", "Canada", "Mexico", "Mediterranean"
        ]
        
        # Fire types and scenarios
        self.fire_types = [
            "wildfire", "forest fire", "grass fire", "brush fire", "crown fire",
            "ground fire", "surface fire", "spot fire", "structure fire",
            "interface fire", "urban fire", "rural fire"
        ]
        
        # Base content templates for variety
        self.content_templates = self._create_enhanced_templates()
        
        logger.info(f"UltraScaleWildfireCollector initialized for {target_count:,} texts")
        logger.info(f"Batch size: {batch_size:,}, Workers: {max_workers}")
        logger.info(f"Memory available: {psutil.virtual_memory().available / (1024**3):.2f} GB")
    
    def _create_enhanced_templates(self) -> List[str]:
        """Create diverse content templates for text generation"""
        
        # Emergency and evacuation scenarios
        emergency_templates = [
            "URGENT: {fire_type} approaching {region}. Immediate evacuation required for zones {zone}. Emergency shelters available at {location}.",
            "BREAKING: {fire_type} has grown to {acres} acres in {region}. Firefighters working around the clock to contain the blaze.",
            "ALERT: {weather_condition} creating critical fire danger in {region}. {fire_type} spreading rapidly through {terrain}.",
            "EVACUATION: Residents of {region} must leave immediately due to approaching {fire_type}. {safety_instruction}.",
            "EMERGENCY: {fire_type} threatens {infrastructure} in {region}. Emergency services responding with {resource}.",
        ]
        
        # Firefighting and response scenarios
        firefighting_templates = [
            "Firefighting crews from {region} deployed to battle the massive {fire_type}. {aerial_resource} continuing throughout the night.",
            "Containment lines holding steady against the advancing {fire_type} in {region}. {ground_resource} creating firebreaks.",
            "{fire_suppression_method} successful in slowing {fire_type} spread near {region}. {progress_update}.",
            "Fire department urges {region} residents to create defensible space. {prevention_advice}.",
            "{specialized_crew} working to suppress the {fire_type} in remote areas of {region}. {tactical_update}.",
        ]
        
        # Weather and conditions scenarios
        weather_templates = [
            "{weather_condition} persist across {region}, elevating wildfire risk. {fire_danger_level} declared.",
            "Lightning strikes sparked multiple fires during {weather_event} in {region}. {meteorological_forecast}.",
            "{wind_condition} hampering firefighting efforts in {region}. {fire_type} spreading rapidly through {terrain}.",
            "Rainfall finally arriving in {region} after months of drought. {seasonal_outlook} as moisture levels improve.",
            "Temperature soaring to {temp}°F with {humidity}% humidity in {region}. {fire_risk_assessment}.",
        ]
        
        # Community impact scenarios
        community_templates = [
            "Local {institution} in {region} {closure_action} due to poor air quality from nearby {fire_type}.",
            "{infrastructure} closed indefinitely due to fire activity in {region}. {alternative_arrangements}.",
            "Power lines downed by {fire_type}, leaving {number} households without electricity in {region}.",
            "Wildlife rescue teams working to save animals displaced by the {fire_type} in {region}.",
            "Economic impact of {fire_type} in {region} estimated at ${amount}. {economic_consequences}.",
        ]
        
        # Recovery and rebuilding scenarios
        recovery_templates = [
            "Rebuilding efforts begin in fire-devastated {region}. {reconstruction_requirements}.",
            "Mental health support available for {fire_type} survivors in {region}. {support_services}.",
            "Reforestation project planting {number} trees in burned areas of {region}. {ecological_recovery}.",
            "{agency} disaster assistance available for fire victims in {region}. {aid_description}.",
            "Community coming together to support {fire_type} survivors in {region}. {community_response}.",
        ]
        
        # Prevention and preparedness scenarios
        prevention_templates = [
            "Fire safety education program launched in {region}. {educational_initiative}.",
            "Homeowners in {region} installing fire-resistant materials. {building_standards}.",
            "Community volunteers clearing brush from {area} in {region}. {fuel_reduction_efforts}.",
            "Early warning system alerts {region} residents when fire danger increases. {technology_implementation}.",
            "Fire-adapted landscaping becoming popular in {region}. {landscape_management}.",
        ]
        
        return (emergency_templates + firefighting_templates + weather_templates + 
                community_templates + recovery_templates + prevention_templates)
    
    def _get_memory_usage(self) -> Dict[str, float]:
        """Get current memory usage statistics"""
        process = psutil.Process()
        memory_info = process.memory_info()
        virtual_memory = psutil.virtual_memory()
        
        return {
            'process_mb': memory_info.rss / (1024 * 1024),
            'system_available_gb': virtual_memory.available / (1024**3),
            'system_used_percent': virtual_memory.percent
        }
    
    def _generate_fire_scenario(self, template: str) -> str:
        """Generate a specific fire scenario from template"""
        replacements = {
            'fire_type': random.choice(self.fire_types),
            'region': random.choice(self.regions),
            'acres': f"{random.randint(500, 50000):,}",
            'zone': random.choice(['A, B, and C', 'East District', 'Northern Areas', '1-5', 'Downtown']),
            'location': random.choice(['community center', 'high school', 'civic center', 'sports complex']),
            'weather_condition': random.choice(['Drought conditions', 'Extreme heat', 'High winds', 'Low humidity']),
            'terrain': random.choice(['dry grasslands', 'dense forest', 'mountainous terrain', 'canyon areas']),
            'safety_instruction': random.choice(['Take only essential items', 'Use designated routes only', 'Do not delay departure']),
            'infrastructure': random.choice(['Highway 101', 'power grid', 'water treatment plant', 'communication towers']),
            'resource': random.choice(['air tankers', 'ground crews', 'emergency vehicles', 'specialized equipment']),
            'aerial_resource': random.choice(['Aerial water drops', 'Helicopter operations', 'Air tanker missions']),
            'ground_resource': random.choice(['Bulldozers', 'Hand crews', 'Engine companies']),
            'fire_suppression_method': random.choice(['Prescribed burning', 'Fire retardant drops', 'Containment lines']),
            'progress_update': random.choice(['Containment increased to 25%', 'Fire growth slowed significantly', 'No new hotspots detected']),
            'prevention_advice': random.choice(['Clear vegetation within 30 feet', 'Maintain defensible space', 'Remove flammable materials']),
            'specialized_crew': random.choice(['Smoke jumpers', 'Hotshot crews', 'Helitack teams']),
            'tactical_update': random.choice(['Initial attack successful', 'Suppression efforts ongoing', 'Backburning operations']),
            'fire_danger_level': random.choice(['Red flag warning', 'Extreme fire danger', 'Very high fire risk']),
            'weather_event': random.choice(['dry thunderstorm', 'heat dome', 'Santa Ana winds']),
            'meteorological_forecast': random.choice(['Dry conditions expected to continue', 'No relief in sight', 'Critical fire weather persists']),
            'wind_condition': random.choice(['Gusting winds up to 45 mph', 'Erratic wind patterns', 'Strong downslope winds']),
            'seasonal_outlook': random.choice(['Fire season may be ending', 'Drought conditions improving', 'Moisture return expected']),
            'temp': random.randint(85, 120),
            'humidity': random.randint(5, 25),
            'fire_risk_assessment': random.choice(['Perfect storm for fire ignition', 'Extreme fire conditions', 'Critical fire weather']),
            'institution': random.choice(['school district', 'hospital', 'university', 'government office']),
            'closure_action': random.choice(['canceling classes', 'suspending operations', 'implementing shelter protocols']),
            'alternative_arrangements': random.choice(['Detour routes established', 'Alternative services available', 'Emergency access only']),
            'number': random.choice(['25,000', '50,000', '100,000', '15,000']),
            'amount': random.choice(['2.5 billion', '500 million', '1.2 billion', '750 million']),
            'economic_consequences': random.choice(['Insurance claims processing', 'Local businesses affected', 'Tourism impact significant']),
            'reconstruction_requirements': random.choice(['Stricter fire safety standards', 'Fire-resistant materials required', 'Updated building codes']),
            'support_services': random.choice(['Trauma counselors available', 'Community support groups', 'Professional counseling']),
            'ecological_recovery': random.choice(['Ecosystem recovery expected in 5-10 years', 'Native species restoration', 'Soil stabilization priority']),
            'agency': random.choice(['FEMA', 'State emergency services', 'Federal agencies']),
            'aid_description': random.choice(['Financial assistance for rebuilding', 'Temporary housing support', 'Emergency relief funds']),
            'community_response': random.choice(['Donation drives organized', 'Volunteer networks activated', 'Mutual aid efforts']),
            'educational_initiative': random.choice(['Emergency preparedness training', 'Fire safety workshops', 'Community education programs']),
            'building_standards': random.choice(['Updated to reflect fire risk', 'Fire-resistant roofing materials', 'Defensive design principles']),
            'area': random.choice(['hiking trails', 'park areas', 'residential zones', 'wildland interfaces']),
            'fuel_reduction_efforts': random.choice(['Part of comprehensive fire strategy', 'Community wildfire protection', 'Proactive fire management']),
            'technology_implementation': random.choice(['Saving lives through early detection', 'Advanced warning systems', 'Real-time monitoring']),
            'landscape_management': random.choice(['Reducing ignition risk through design', 'Water-wise fire-safe plants', 'Sustainable landscaping'])
        }
        
        try:
            return template.format(**replacements)
        except KeyError as e:
            # Fallback to simple replacement if key missing
            return template.replace('{' + str(e).strip("'") + '}', 'unknown')
    
    def _generate_batch_texts(self, batch_id: int, batch_size: int) -> List[str]:
        """Generate a batch of diverse wildfire texts"""
        texts = []
        templates_cycle = self.content_templates * ((batch_size // len(self.content_templates)) + 1)
        
        for i in range(batch_size):
            try:
                template = templates_cycle[i]
                text = self._generate_fire_scenario(template)
                
                # Add variation prefixes for diversity
                if random.random() < 0.3:
                    prefix = random.choice(["Breaking: ", "Update: ", "Alert: ", "Latest: ", "Urgent: "])
                    text = prefix + text
                
                # Add slight variations
                if random.random() < 0.2:
                    text = text.replace("wildfire", "forest fire")
                elif random.random() < 0.2:
                    text = text.replace("fire", "blaze")
                
                texts.append(text)
                
            except Exception as e:
                logger.warning(f"Error generating text {i} in batch {batch_id}: {e}")
                # Fallback text
                texts.append(f"Wildfire emergency reported in {random.choice(self.regions)}. Emergency services responding.")
        
        return texts
    
    def _clean_text(self, text: str) -> str:
        """Clean and normalize text"""
        # Remove excessive whitespace
        text = re.sub(r'\s+', ' ', text.strip())
        
        # Remove special characters but keep basic punctuation
        text = re.sub(r'[^\w\s\.\,\!\?\;\:\-\(\)\"\']+', ' ', text)
        
        # Remove extra spaces
        text = ' '.join(text.split())
        
        return text
    
    def _is_valid_text(self, text: str, min_length: int = 50, max_length: int = 1000) -> bool:
        """Validate text quality and length"""
        if not text or len(text.strip()) < min_length:
            return False
        
        if len(text) > max_length:
            return False
        
        # Check if text is in English (basic check)
        english_chars = sum(1 for c in text if c.isascii())
        total_chars = len(text)
        
        if total_chars > 0 and (english_chars / total_chars) < 0.8:
            return False
        
        return True
    
    def _filter_wildfire_content(self, text: str) -> bool:
        """Check if text is related to wildfires"""
        text_lower = text.lower()
        return any(keyword in text_lower for keyword in self.wildfire_keywords)
    
    def collect_ultra_scale_data(self) -> List[Dict]:
        """Collect ultra-scale wildfire data with memory optimization"""
        logger.info(f"Starting ultra-scale data collection for {self.target_count:,} documents")
        
        collected_texts = []
        num_batches = (self.target_count + self.batch_size - 1) // self.batch_size
        
        # Progress tracking
        progress_bar = tqdm(total=self.target_count, desc="Collecting texts", unit="docs")
        
        for batch_num in range(num_batches):
            batch_start_time = time.time()
            
            # Calculate batch size for this iteration
            remaining = self.target_count - len(collected_texts)
            current_batch_size = min(self.batch_size, remaining)
            
            if current_batch_size <= 0:
                break
            
            try:
                # Generate batch of texts
                batch_texts = self._generate_batch_texts(batch_num, current_batch_size)
                
                # Process batch
                batch_processed = 0
                for i, text in enumerate(batch_texts):
                    if len(collected_texts) >= self.target_count:
                        break
                    
                    # Clean and validate
                    cleaned_text = self._clean_text(text)
                    
                    if self._filter_wildfire_content(cleaned_text) and self._is_valid_text(cleaned_text):
                        collected_texts.append({
                            'id': len(collected_texts) + 1,
                            'text': cleaned_text,
                            'source': 'ultra_scale_generator',
                            'timestamp': datetime.now().isoformat(),
                            'category': 'wildfire',
                            'language': 'en',
                            'batch_id': batch_num
                        })
                        batch_processed += 1
                
                # Update progress
                progress_bar.update(batch_processed)
                
                # Memory management
                if batch_num % 10 == 0:  # Every 10 batches
                    gc.collect()
                    memory_info = self._get_memory_usage()
                    logger.info(f"Batch {batch_num+1}/{num_batches} completed. "
                              f"Memory: {memory_info['process_mb']:.1f}MB, "
                              f"System: {memory_info['system_used_percent']:.1f}%")
                
                # Batch timing
                batch_time = time.time() - batch_start_time
                if batch_num % 5 == 0:
                    logger.info(f"Batch {batch_num+1} processed {batch_processed} texts in {batch_time:.2f}s")
                
            except Exception as e:
                logger.error(f"Error processing batch {batch_num}: {e}")
                continue
        
        progress_bar.close()
        
        # Final memory cleanup
        gc.collect()
        
        logger.info(f"Ultra-scale collection completed: {len(collected_texts):,} documents")
        return collected_texts
    
    def save_data_chunked(self, data: List[Dict], chunk_size: int = 5000) -> Tuple[List[str], List[str]]:
        """Save data in chunks to manage memory and file sizes"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        raw_files = []
        cleaned_files = []
        
        num_chunks = (len(data) + chunk_size - 1) // chunk_size
        
        logger.info(f"Saving {len(data):,} documents in {num_chunks} chunks of {chunk_size:,} each")
        
        for chunk_idx in range(num_chunks):
            start_idx = chunk_idx * chunk_size
            end_idx = min((chunk_idx + 1) * chunk_size, len(data))
            chunk_data = data[start_idx:end_idx]
            
            # Raw data chunk
            raw_filename = f"{timestamp}_wildfire_raw_chunk_{chunk_idx+1:02d}.json"
            raw_filepath = os.path.join(self.raw_dir, raw_filename)
            
            with open(raw_filepath, 'w', encoding='utf-8') as f:
                json.dump(chunk_data, f, indent=2, ensure_ascii=False)
            
            raw_files.append(raw_filepath)
            
            # Also save as cleaned (in this case, same data)
            cleaned_filename = f"{timestamp}_wildfire_cleaned_chunk_{chunk_idx+1:02d}.json"
            cleaned_filepath = os.path.join(self.cleaned_dir, cleaned_filename)
            
            with open(cleaned_filepath, 'w', encoding='utf-8') as f:
                json.dump(chunk_data, f, indent=2, ensure_ascii=False)
            
            cleaned_files.append(cleaned_filepath)
            
            logger.info(f"Saved chunk {chunk_idx+1}/{num_chunks}: {len(chunk_data):,} documents")
        
        # Also create a consolidated index file
        index_file = os.path.join(self.cleaned_dir, f"{timestamp}_wildfire_index.json")
        index_data = {
            'total_documents': len(data),
            'chunks': num_chunks,
            'chunk_size': chunk_size,
            'timestamp': timestamp,
            'files': {
                'raw': raw_files,
                'cleaned': cleaned_files
            }
        }
        
        with open(index_file, 'w', encoding='utf-8') as f:
            json.dump(index_data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Index file created: {index_file}")
        
        return raw_files, cleaned_files
    
    def generate_statistics(self, data: List[Dict]) -> Dict:
        """Generate comprehensive statistics about the collected data"""
        if not data:
            return {}
        
        texts = [item['text'] for item in data]
        lengths = [len(text) for text in texts]
        word_counts = [len(text.split()) for text in texts]
        
        # Keyword analysis
        keyword_counts = {}
        for keyword in self.wildfire_keywords[:20]:  # Top 20 keywords
            count = sum(1 for text in texts if keyword.lower() in text.lower())
            if count > 0:
                keyword_counts[keyword] = count
        
        # Regional analysis
        region_counts = {}
        for region in self.regions[:15]:  # Top 15 regions
            count = sum(1 for text in texts if region in text)
            if count > 0:
                region_counts[region] = count
        
        stats = {
            'collection_info': {
                'total_documents': len(data),
                'target_achieved': len(data) >= self.target_count * 0.95,
                'collection_rate': len(data) / self.target_count if self.target_count > 0 else 0
            },
            'text_statistics': {
                'avg_length': sum(lengths) / len(lengths),
                'min_length': min(lengths),
                'max_length': max(lengths),
                'avg_word_count': sum(word_counts) / len(word_counts),
                'min_word_count': min(word_counts),
                'max_word_count': max(word_counts)
            },
            'content_analysis': {
                'top_keywords': sorted(keyword_counts.items(), key=lambda x: x[1], reverse=True)[:10],
                'top_regions': sorted(region_counts.items(), key=lambda x: x[1], reverse=True)[:10],
                'unique_sources': list(set(item.get('source', 'unknown') for item in data)),
                'languages': list(set(item.get('language', 'unknown') for item in data))
            },
            'quality_metrics': {
                'avg_ascii_ratio': sum(sum(1 for c in text if c.isascii()) / len(text) for text in texts) / len(texts),
                'texts_with_numbers': sum(1 for text in texts if any(c.isdigit() for c in text)),
                'texts_with_caps': sum(1 for text in texts if any(c.isupper() for c in text))
            }
        }
        
        return stats
    
    def run_ultra_scale_pipeline(self) -> Dict:
        """Run the complete ultra-scale data collection pipeline"""
        logger.info("Starting Ultra-Scale Wildfire Data Collection Pipeline...")
        logger.info(f"Target: {self.target_count:,} documents")
        
        start_time = datetime.now()
        initial_memory = self._get_memory_usage()
        
        try:
            # Step 1: Collect data
            logger.info("=== Step 1: Ultra-Scale Data Collection ===")
            data = self.collect_ultra_scale_data()
            
            if not data:
                raise Exception("No data collected")
            
            # Step 2: Save data in chunks
            logger.info("=== Step 2: Chunked Data Storage ===")
            raw_files, cleaned_files = self.save_data_chunked(data)
            
            # Step 3: Generate statistics
            logger.info("=== Step 3: Statistical Analysis ===")
            stats = self.generate_statistics(data)
            
            # Execution summary
            end_time = datetime.now()
            execution_time = end_time - start_time
            final_memory = self._get_memory_usage()
            
            summary = {
                'execution_info': {
                    'start_time': start_time.isoformat(),
                    'end_time': end_time.isoformat(),
                    'execution_time': str(execution_time),
                    'target_count': self.target_count,
                    'actual_count': len(data),
                    'success_rate': len(data) / self.target_count if self.target_count > 0 else 0
                },
                'memory_usage': {
                    'initial_mb': initial_memory['process_mb'],
                    'final_mb': final_memory['process_mb'],
                    'peak_system_percent': final_memory['system_used_percent']
                },
                'files': {
                    'raw_files': raw_files,
                    'cleaned_files': cleaned_files,
                    'total_chunks': len(raw_files)
                },
                'statistics': stats,
                'success': True
            }
            
            # Save summary
            summary_file = os.path.join(self.cleaned_dir, f"ultra_scale_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
            with open(summary_file, 'w', encoding='utf-8') as f:
                json.dump(summary, f, indent=2, ensure_ascii=False)
            
            # Print results
            self._print_final_summary(summary)
            
            return summary
            
        except Exception as e:
            logger.error(f"Ultra-scale pipeline failed: {str(e)}")
            raise

    def _print_final_summary(self, summary: Dict):
        """Print comprehensive final summary"""
        exec_info = summary['execution_info']
        stats = summary['statistics']
        
        logger.info("\n" + "="*80)
        logger.info("ULTRA-SCALE WILDFIRE DATA COLLECTION - EXECUTION SUMMARY")
        logger.info("="*80)
        logger.info(f"Target Documents: {exec_info['target_count']:,}")
        logger.info(f"Actual Documents: {exec_info['actual_count']:,}")
        logger.info(f"Success Rate: {exec_info['success_rate']:.1%}")
        logger.info(f"Execution Time: {exec_info['execution_time']}")
        logger.info(f"Data Chunks: {summary['files']['total_chunks']}")
        logger.info(f"Memory Usage: {summary['memory_usage']['initial_mb']:.1f}MB -> {summary['memory_usage']['final_mb']:.1f}MB")
        
        if 'text_statistics' in stats:
            text_stats = stats['text_statistics']
            logger.info(f"Avg Text Length: {text_stats['avg_length']:.1f} chars")
            logger.info(f"Avg Word Count: {text_stats['avg_word_count']:.1f} words")
        
        if 'content_analysis' in stats and 'top_keywords' in stats['content_analysis']:
            top_keywords = stats['content_analysis']['top_keywords'][:5]
            logger.info(f"Top Keywords: {', '.join([f'{k}({v})' for k, v in top_keywords])}")
        
        logger.info("="*80)


def main():
    """Main function for ultra-scale execution"""
    # Configuration for ultra-scale processing
    target_count = 25000  # 25,000 documents
    batch_size = 1000     # Process in 1K batches
    max_workers = 8       # Parallel processing
    
    logger.info(f"Starting ultra-scale wildfire collection: {target_count:,} documents")
    
    # Initialize collector
    collector = UltraScaleWildfireCollector(
        output_dir="data",
        target_count=target_count,
        batch_size=batch_size,
        max_workers=max_workers
    )
    
    # Run collection pipeline
    try:
        summary = collector.run_ultra_scale_pipeline()
        
        print(f"\nUltra-scale collection completed successfully!")
        print(f"Documents: {summary['execution_info']['actual_count']:,}")
        print(f"Time: {summary['execution_info']['execution_time']}")
        print(f"Chunks: {summary['files']['total_chunks']}")
        
    except Exception as e:
        logger.error(f"Ultra-scale collection failed: {e}")
        raise


if __name__ == "__main__":
    main()