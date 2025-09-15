# EU Fire Analysis v1.4.2

**Europe-focused Forest Fire Detection & Analysis System using NASA FIRMS Data**

## 🌍 Overview

EU Fire Analysis v1.4.2 is a dedicated system for detecting and analyzing forest fires across Europe using NASA FIRMS (Fire Information for Resource Management System) data. By integrating cutting-edge AI techniques with geospatial analysis, it enables detailed pattern recognition of fire activity.

## 🎯 Key Features

### Geographic Coverage
- **Scope**: Entire European region (34°N–72°N, 25°W–50°E)
- **Regional Breakdown**: Over 12 detailed subregions
  - Nordic: Scandinavia, North Atlantic
  - Western Europe: British Isles, Continental West, Central West
  - Southern Europe: Iberian Peninsula, Western Mediterranean, Balkans
  - Eastern Europe: Central, Black Sea, Western Russia

### Technical Specifications
- **AI Embedding**: Sentence Transformers (all-MiniLM-L6-v2)
- **Clustering**: FAISS k-means (optimized for large-scale data)
- **Visualization**: t-SNE, regional heatmaps, temporal analysis
- **GPU Acceleration**: CUDA-enabled (if available)

## 🚀 Quick Start

### 1. Environment Setup
```bash
# Install dependencies
pip install -r requirements.txt

# Recommended for GPU environments
pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu118
```

### 2. Configuration File
Adjust parameters in `config_europe_firms.json`:
```json
{
  "region": "Europe",
  "coordinates": {
    "south": 34.0, "north": 72.0,
    "west": -25.0, "east": 50.0
  },
  "max_samples": 15000,
  "days_back": 10
}
```

### 3. Run Analysis
```bash
python europe_firms_pipeline_v2.py
```

## 📊 Analysis Results

### Performance Metrics (Latest Run)
- **Processing Time**: 76.45 seconds (13,334 samples)
- **Quality Score**: 0.648
- **Number of Clusters**: 15
- **Coverage**: Full EU region

### Output Files
1. **📊 Data Files**
   - `nasa_firms_data.csv` – Raw FIRMS data
   - `europe_fires_clustered.csv` – Clustered results
   - `final_europe_results.json` – Summary of analysis

2. **🖼️ Visualization Files**
   - `tsne_plot.png` – Cluster distribution
   - `cluster_regional_analysis.png` – Regional breakdown
   - `cluster_geographic_distribution.png` – Geographic spread
   - `cluster_intensity_analysis.png` – Fire intensity
   - `cluster_temporal_patterns.png` – Temporal trends

3. **📝 Report**
   - `comprehensive_fire_analysis_report.md` – Full analytical report

## 🔧 Core Components

### `europe_firms_pipeline_v2.py`
Main pipeline: data collection → embedding → clustering → visualization

### `cluster_feature_analyzer.py`
Regional analysis engine:
- Detailed classification of 12+ European regions
- Geographic, temporal, and intensity pattern analysis
- Multidimensional visualizations

### `scripts/`
- `data_collector.py` – NASA FIRMS API integration
- `embedding_generator.py` – Embedding generation
- `clustering.py` – FAISS k-means implementation
- `visualization.py` – Visualization engine

## 🌟 Advanced Features

### Detailed Regional Classification
Expanded from 2 basic zones to 12+ fine-grained regions:

**Nordic Region**
- Nordic (Scandinavia): Norway, Sweden, Finland
- Nordic (North Atlantic): Iceland, Faroe Islands

**Western Europe**
- British Isles: UK, Ireland
- Western Europe (Continental): France, Belgium, Netherlands
- Central Western Europe: Germany, Switzerland

**Southern Europe**
- Iberian Peninsula: Spain, Portugal
- Mediterranean West: Southern France, Italy
- Balkans: Balkan Peninsula
- Southeast Mediterranean: Greece, Southern Balkans

**Eastern Europe**
- Central Europe: Poland, Czech Republic
- Eastern Europe (Central): Ukraine, Belarus
- Eastern Europe (Russia): Western Russia
- Eastern Europe (Black Sea): Black Sea coast

### Adaptive Clustering
- Automatically selects optimal method based on data size
- HDBSCAN (small-scale) → FAISS k-means (large-scale)
- Real-time quality evaluation

## 📈 Use Cases

### 1. Fire Monitoring & Early Warning
Real-time surveillance of fire activity across Europe

### 2. Regional Fire Pattern Analysis
Detailed insights into seasonal trends, intensity, and frequency

### 3. Research & Policy Support
Supports evidence-based disaster prevention and environmental policy

### 4. International Collaboration
Facilitates fire data sharing and coordinated response among EU nations

## 🔧 Configuration Options

### Custom Coordinate Range
Focus on specific regions:
```json
{
  "coordinates": {
    "south": 50.0, "north": 60.0,  // Nordic only
    "west": 0.0, "east": 20.0
  }
}
```

### Time Range Adjustment
```json
{
  "days_back": 30,  // Last 30 days
  "max_samples": 50000  // Sample limit
}
```

## 📊 Sample Results

### Latest Analysis (Run on September 15, 2025)
- **Total Fires Detected**: 13,334
- **High-Confidence Detections**: 13,334 (≥50% confidence)
- **Regional Distribution**:
  - Mediterranean: 43.6% (5,816)
  - Eastern Europe: 62.8% (8,382)
  - Western Europe: 15.3% (2,040)
  - Nordic: 1.5% (197)
- **Processing Time**: 76.45 seconds
- **Quality Score**: 0.648

### Geographic Characteristics
- **Latitude Range**: 34.0°N – 66.3°N
- **Longitude Range**: -22.3°W – 50.0°E
- **Density**: High concentration along Mediterranean coast and Balkans

## 🛠️ Technical Requirements

### Minimum Requirements
- Python 3.8+
- RAM: 8GB+
- Storage: 10GB+

### Recommended Environment
- Python 3.9+
- RAM: 16GB+
- GPU: CUDA-enabled (NVIDIA GTX 1660 or higher)
- Storage: SSD 20GB+

### Dependencies
```
torch>=1.9.0
transformers>=4.20.0
sentence-transformers>=2.2.0
faiss-cpu>=1.7.0  # or faiss-gpu
numpy>=1.21.0
pandas>=1.3.0
matplotlib>=3.5.0
seaborn>=0.11.0
scikit-learn>=1.0.0
```

## 📄 License

MIT License – See `LICENSE` file for details

## 🤝 Contributing

1. Fork the repository  
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)  
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)  
4. Push to the branch (`git push origin feature/AmazingFeature`)  
5. Open a Pull Request  

## 📞 Support

- Issues: GitHub Issues  
- https://www.linkedin.com/in/yasunotkt/

## 🔄 Version History

### v1.4.2 (Current)
- ✅ Detailed regional classification (12+ zones)
- ✅ FAISS k-means optimization
- ✅ GPU acceleration support
- ✅ Comprehensive visualization system
- ✅ Real-time quality evaluation

### Previous Versions
- v1.4.1: Basic EU support
- v1.4.0: South America base system

---

**EU Fire Analysis v1.4.2** – Europe-specific forest fire analysis system  
Powered by NASA FIRMS Data & Advanced AI Technology
