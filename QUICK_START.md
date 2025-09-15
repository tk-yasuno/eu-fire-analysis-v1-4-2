# EU Fire Analysis Quick Start Guide

## 🚀 Quick Setup & Execution

### Step 1: Clone & Setup
```bash
git clone https://github.com/tk-yasuno/eu-fire-analysis-v1-4-2.git
cd eu-fire-analysis-v1-4-2
pip install -r requirements.txt
```

### Step 2: Run Analysis
```bash
python europe_firms_pipeline_v2.py
```

### Step 3: View Results
- **Main Results**: `data_firms_europe_[timestamp]/`
- **Key Visualizations**:
  - `tsne_plot.png` - Cluster overview
  - `cluster_regional_analysis.png` - Detailed regional analysis
  - `comprehensive_fire_analysis_report.md` - Full report

## 📊 Expected Output (Sample Run)
```
🎉 EUROPE FOREST FIRE ANALYSIS RESULTS
======================================================================
✅ Status: SUCCESS
🎯 Selected Method: FAISS k-means
📊 Quality Score: 0.648
🔢 Clusters Found: 15
📦 Total Fire Detections: 13,334
⏱️ Processing Time: 76.45s
🌍 Region Coverage: Europe (34°N-72°N, 25°W-50°E)
```

## 🗺️ Regional Coverage
- **Nordic**: Finland, Sweden, Norway, Iceland
- **Western Europe**: UK, Ireland, France, Germany, Netherlands
- **Southern Europe**: Spain, Portugal, Italy, Greece, Balkans
- **Eastern Europe**: Poland, Czech Republic, Ukraine, Russia (western)

## ⚙️ Configuration
Edit `config_europe_firms.json` to customize:
- Geographic bounds
- Sample size limits
- Analysis parameters

## 🔧 Troubleshooting
- **GPU not detected**: System will use CPU automatically
- **API timeout**: Check internet connection
- **Memory error**: Reduce `max_samples` in config

For detailed documentation, see `README.md`