# EU Fire Analysis v1.4.2

**Europe-focused Forest Fire Detection & Analysis System using NASA FIRMS Data**

## 🌍 Overview

EU Fire Analysis v1.4.2は、NASA FIRMS（Fire Information for Resource Management System）データを使用してヨーロッパ全域の森林火災を検出・分析する専用システムです。最新のAI技術と地理情報システムを組み合わせて、詳細な火災パターン分析を実現します。

## 🎯 Key Features

### 地理的カバレッジ
- **対象範囲**: ヨーロッパ全域（34°N-72°N, 25°W-50°E）
- **詳細地域分析**: 12以上の具体的地域区分
  - 北欧（Nordic）: スカンジナビア、北大西洋
  - 西欧（Western Europe）: ブリテン島、大陸部、中央西欧
  - 南欧（Southern Europe）: イベリア半島、地中海西部、バルカン半島
  - 東欧（Eastern Europe）: 中央、黒海沿岸、ロシア西部

### 技術仕様
- **AI埋め込み**: Sentence Transformers (all-MiniLM-L6-v2)
- **クラスタリング**: FAISS k-means（大規模データ最適化）
- **可視化**: t-SNE、地域別ヒートマップ、時系列分析
- **GPU加速**: CUDA対応（利用可能時）

## 🚀 Quick Start

### 1. 環境セットアップ
```bash
# 依存関係インストール
pip install -r requirements.txt

# GPU環境（推奨）
pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu118
```

### 2. 設定ファイル確認
`config_europe_firms.json`でパラメータを調整：
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

### 3. 分析実行
```bash
python europe_firms_pipeline_v2.py
```

## 📊 Analysis Results

### パフォーマンス指標（最新実行結果）
- **処理時間**: 76.45秒（13,334サンプル）
- **品質スコア**: 0.648
- **クラスター数**: 15個
- **地域カバレッジ**: 全EU域

### 生成される分析ファイル
1. **📊 データファイル**
   - `nasa_firms_data.csv` - 生データ
   - `europe_fires_clustered.csv` - クラスタ分析済み
   - `final_europe_results.json` - 分析結果サマリ

2. **🖼️ 可視化ファイル**
   - `tsne_plot.png` - クラスタ分布
   - `cluster_regional_analysis.png` - 詳細地域分析
   - `cluster_geographic_distribution.png` - 地理的分布
   - `cluster_intensity_analysis.png` - 火災強度分析
   - `cluster_temporal_patterns.png` - 時間パターン

3. **📝 分析レポート**
   - `comprehensive_fire_analysis_report.md` - 包括的レポート

## 🔧 Core Components

### `europe_firms_pipeline_v2.py`
メインパイプライン：データ収集→埋め込み生成→クラスタリング→可視化

### `cluster_feature_analyzer.py`
詳細地域分析システム：
- 12+ ヨーロッパ地域の詳細分類
- 地理的・時間的・強度パターン分析
- 多次元可視化生成

### `scripts/`
- `data_collector.py` - NASA FIRMS API接続
- `embedding_generator.py` - AI埋め込み生成
- `clustering.py` - FAISS k-means実装
- `visualization.py` - 可視化エンジン

## 🌟 Advanced Features

### 詳細地域分類システム
従来の2つの簡易区分から12以上の詳細地域区分に拡張：

**北欧（Nordic Region）**
- Nordic (Scandinavia): ノルウェー、スウェーデン、フィンランド
- Nordic (North Atlantic): アイスランド、フェロー諸島

**西欧（Western Europe）**
- British Isles: イギリス・アイルランド
- Western Europe (Continental): フランス・ベルギー・オランダ
- Central Western Europe: ドイツ・スイス

**南欧（Southern Europe）**
- Iberian Peninsula: スペイン・ポルトガル
- Mediterranean West: イタリア・フランス南部
- Balkans: バルカン半島
- Southeast Mediterranean: ギリシャ・南バルカン

**東欧（Eastern Europe）**
- Central Europe: ポーランド・チェコ
- Eastern Europe (Central): ウクライナ・ベラルーシ
- Eastern Europe (Russia): ロシア西部
- Eastern Europe (Black Sea): 黒海沿岸

### 適応的クラスタリング
- データサイズに応じた最適手法自動選択
- HDBSCAN（小規模）→ FAISS k-means（大規模）
- リアルタイム品質評価

## 📈 Use Cases

### 1. 火災監視・早期警戒
リアルタイムでヨーロッパ全域の火災活動を監視

### 2. 地域別火災パターン分析
各地域の火災特性（季節性、強度、頻度）を詳細分析

### 3. 研究・政策立案支援
科学的根拠に基づく防災政策・環境保護戦略の策定支援

### 4. 国際協力・情報共有
EU諸国間での火災情報共有と協調対応

## 🔧 Configuration Options

### 座標範囲カスタマイズ
特定地域にフォーカス：
```json
{
  "coordinates": {
    "south": 50.0, "north": 60.0,  // 北欧のみ
    "west": 0.0, "east": 20.0
  }
}
```

### 時間範囲調整
```json
{
  "days_back": 30,  // 過去30日間のデータ
  "max_samples": 50000  // サンプル数上限
}
```

## 📊 Sample Results

### 最新分析結果（2025年9月15日実行）
- **総火災検出数**: 13,334件
- **高信頼度検出**: 13,334件（≥50%信頼度）
- **地域分布**: 
  - 地中海地域: 43.6% (5,816件)
  - 東欧: 62.8% (8,382件)
  - 西欧: 15.3% (2,040件)
  - 北欧: 1.5% (197件)
- **処理時間**: 76.45秒
- **品質スコア**: 0.648

### 地理的特徴
- **緯度範囲**: 34.0°N - 66.3°N
- **経度範囲**: -22.3°W - 50.0°E
- **密度**: 地中海沿岸とバルカン半島で高密度

## 🛠️ Technical Requirements

### 最小要件
- Python 3.8+
- RAM: 8GB以上
- ストレージ: 10GB以上

### 推奨環境
- Python 3.9+
- RAM: 16GB以上
- GPU: CUDA対応（NVIDIA GTX 1660以上）
- ストレージ: SSD 20GB以上

### 依存関係
```
torch>=1.9.0
transformers>=4.20.0
sentence-transformers>=2.2.0
faiss-cpu>=1.7.0  # または faiss-gpu
numpy>=1.21.0
pandas>=1.3.0
matplotlib>=3.5.0
seaborn>=0.11.0
scikit-learn>=1.0.0
```

## 📄 License

MIT License - 詳細は`LICENSE`ファイルを参照

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
- ✅ 詳細地域分析システム（12+ 地域区分）
- ✅ FAISS k-means最適化
- ✅ GPU加速対応
- ✅ 包括的可視化システム
- ✅ リアルタイム品質評価

### Previous Versions
- v1.4.1: 基本EU対応
- v1.4.0: 南米版ベースシステム

---

**EU Fire Analysis v1.4.2** - ヨーロッパ専用森林火災分析システム  
Powered by NASA FIRMS Data & Advanced AI Technology
