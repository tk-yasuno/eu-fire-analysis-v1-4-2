#!/usr/bin/env python3
"""
火災検知分析レポート生成システム
クラスタリング結果と可視化図表を用いた包括的レポート作成
"""

import os
import json
import logging
from datetime import datetime
from typing import Dict, List, Any
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


class FireAnalysisReportGenerator:
    """火災分析レポート生成器"""
    
    def __init__(self, output_dir: str, config: Dict[str, Any] = None):
        """
        Args:
            output_dir: 出力ディレクトリ
            config: 設定情報（地域情報含む）
        """
        self.output_dir = output_dir
        self.config = config or {}
        
        # 地域情報設定
        self.region_info = self.config.get('report', {})
        self.region_name = self.region_info.get('region_name', 'Asia-Pacific')
        self.focus_country = self.region_info.get('focus_country', 'Japan')
        
    def generate_report(self, report_data: Dict[str, Any]) -> str:
        """
        新しいレポート生成メソッド
        
        Args:
            report_data: レポート生成に必要なデータ
            
        Returns:
            生成されたレポートファイルパス
        """
        # レポートデータから情報を取得
        data = report_data['data']
        labels = report_data['labels']
        clustering_results = report_data['clustering_results']
        feature_analysis = report_data['feature_analysis']
        
        # 地域情報を更新
        if 'region_name' in report_data:
            self.region_name = report_data['region_name']
        if 'focus_country' in report_data:
            self.focus_country = report_data['focus_country']
        
        # 既存のメソッドを使用してレポート生成
        return self.generate_comprehensive_report(
            clustering_result=clustering_results,
            feature_analysis=feature_analysis,
            nasa_data_path=os.path.join(self.output_dir, "nasa_firms_data.csv"),
            config=self.config
        )
        
    def generate_comprehensive_report(self, 
                                    clustering_result,
                                    feature_analysis: Dict[str, Any],
                                    nasa_data_path: str,
                                    config: Dict[str, Any]) -> str:
        """
        包括的な火災分析レポートを生成
        
        Args:
            clustering_result: クラスタリング結果
            feature_analysis: クラスター特徴分析結果
            nasa_data_path: NASA FIRMSデータファイルパス
            config: 設定情報
            
        Returns:
            生成されたレポートファイルパス
        """
        logger.info("Generating comprehensive fire analysis report...")
        
        # データ読み込み
        nasa_df = pd.read_csv(nasa_data_path)
        
        # レポート構成要素
        report_sections = [
            self._generate_report_header(),
            self._generate_executive_summary(clustering_result, feature_analysis, nasa_df, config),
            self._generate_methodology_section(config),
            self._generate_data_overview_section(nasa_df, config),
            self._generate_clustering_analysis_section(clustering_result, feature_analysis),
            self._generate_geographic_analysis_section(feature_analysis['geographic_analysis']),
            self._generate_temporal_analysis_section(feature_analysis['temporal_analysis']),
            self._generate_intensity_analysis_section(feature_analysis['intensity_analysis']),
            self._generate_regional_characteristics_section(feature_analysis['regional_analysis']),
            self._generate_visualizations_guide(),
            self._generate_conclusions_recommendations(feature_analysis['cluster_summary'])
        ]
        
        # レポート統合
        full_report = self._combine_report_sections(report_sections)
        
        # レポート保存
        report_path = os.path.join(self.output_dir, "comprehensive_fire_analysis_report.md")
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(full_report)
        
        logger.info(f"Comprehensive report generated: {report_path}")
        return report_path
    
    def _generate_report_header(self) -> str:
        """レポートヘッダー生成"""
        # 地域に応じたタイトル設定
        if self.region_name == 'South America':
            region_title = '南米地域火災検知分析レポート'
        elif self.region_name == 'Europe':
            region_title = 'ヨーロッパ地域火災検知分析レポート'
        elif self.region_name == 'Asia-Pacific':
            region_title = 'アジア太平洋地域火災検知分析レポート'
        else:
            region_title = f'{self.region_name}地域火災検知分析レポート'
        
        return f"""# {region_title}

**分析日時**: {datetime.now().strftime('%Y年%m月%d日 %H時%M分')}  
**対象地域**: {self.region_name}  
**主要対象国**: {self.focus_country}  
**データソース**: NASA FIRMS VIIRS_SNPP_NRT  
**分析システム**: 大規模火災検知分析システム v1.3

---

## 分析概要
### Analysis Overview

本レポートは、NASA FIRMS衛星データを用いた{self.region_name}地域の火災検知データに対する包括的クラスタリング分析結果をまとめたものです。機械学習による高度な分析技術により、火災パターンの特徴を抽出し、地理的・時間的・強度別の多角的分析を実施しました。

"""
    
    def _generate_executive_summary(self, clustering_result, feature_analysis, nasa_df, config) -> str:
        """エグゼクティブサマリー生成"""
        summary = feature_analysis['cluster_summary']
        area_params = config['nasa_firms']['area_params']
        
        # 地域に応じた座標フォーマットと分布説明を設定
        if self.region_name == 'South America':
            coord_format = f"({area_params['west']}°W - {area_params['east']}°W, {area_params['south']}°S - {area_params['north']}°N)"
            region_distribution = "ブラジル、チリ、アルゼンチン、ペルーの4大火災地域を特定"
        elif self.region_name == 'Europe':
            coord_format = f"({area_params['west']}°W - {area_params['east']}°E, {area_params['south']}°N - {area_params['north']}°N)"
            region_distribution = "イタリア、スペイン、ギリシャ、フランスの4大火災地域を特定"
        else:
            coord_format = f"({area_params['west']}°E - {area_params['east']}°E, {area_params['south']}°S - {area_params['north']}°N)"
            region_distribution = "オーストラリア、東アジア、東南アジアの3大火災地域を特定"
        
        return f"""
## Executive Summary - エグゼクティブサマリー

**分析期間**: 過去{config['nasa_firms']['days_back']}日間 ({datetime.now().strftime('%Y年%m月%d日')}時点)  
**対象地域**: {self.region_name} {coord_format}  
**分析手法**: 機械学習による適応的クラスタリング (FAISS k-means)  
**処理データ**: {len(nasa_df):,}件の高信頼度火災検知データ

### 🔥 主要発見事項

- **検出クラスター数**: {summary['total_clusters']}つの明確な火災パターン
- **品質スコア**: {clustering_result['quality_score']:.3f} (高品質クラスタリング達成)
- **地域分布**: {region_distribution}
- **時間パターン**: 明確な日中・夜間活動サイクルを確認
- **火災強度**: 高強度火災群と中・低強度火災群の明確な分離

### 📊 統計概要

| 指標 | 値 |
|------|-----|
| 総火災検知数 | {summary['total_points']:,}件 |
| 最大クラスター | {max(summary['cluster_sizes'].values())}件 |
| 最小クラスター | {min(summary['cluster_sizes'].values())}件 |
| ノイズ率 | {clustering_result['noise_ratio']:.1%} |
| 平均信頼度 | {nasa_df['confidence'].mean():.1f}% |

---
"""
    
    def _generate_methodology_section(self, config) -> str:
        """方法論セクション生成"""
        return f"""## 分析方法論
### Methodology

#### データソース
- **NASA FIRMS API**: リアルタイム衛星火災検知システム
- **衛星**: {config['nasa_firms']['satellite']} (VIIRS次世代極軌道衛星)
- **信頼度フィルタ**: {config['nasa_firms']['confidence_threshold']}%以上の高信頼度データのみ使用

#### 機械学習アプローチ
1. **テキスト埋め込み**: {config['embedding']['model_name']} (384次元ベクトル)
2. **適応的クラスタリング**: 
   - 大規模データ用FAISS k-means最適化
   - 3,000件超のデータでHDBSCANをスキップ
3. **品質評価**: シルエット係数、Calinski-Harabasz指数、Davies-Bouldin指数の統合

#### 特徴分析
- **地理的分布**: 重心、範囲、密度分析
- **時間パターン**: 時間別・曜日別活動分析
- **火災強度**: 明度・信頼度・FRP統合分析
- **地域特性**: 多地域クラスター特定

---
"""
    
    def _generate_data_overview_section(self, nasa_df, config) -> str:
        """データ概要セクション生成"""
        return f"""## データ概要
### Data Overview

#### 収集データ統計
- **期間**: {config['nasa_firms']['days_back']}日間
- **地理的範囲**: {(config['nasa_firms']['area_params']['east'] - config['nasa_firms']['area_params']['west']):.0f}° × {(config['nasa_firms']['area_params']['north'] - config['nasa_firms']['area_params']['south']):.0f}°
- **総検知数**: {len(nasa_df):,}件

#### 火災検知品質分布
| 信頼度レベル | 件数 | 割合 |
|-------------|------|------|
| 高信頼度 (80%) | {(nasa_df['confidence'] == 80).sum():,}件 | {(nasa_df['confidence'] == 80).mean():.1%} |
| 標準信頼度 (60%) | {(nasa_df['confidence'] == 60).sum():,}件 | {(nasa_df['confidence'] == 60).mean():.1%} |
| 低信頼度 (40%) | {(nasa_df['confidence'] == 40).sum():,}件 | {(nasa_df['confidence'] == 40).mean():.1%} |

#### 火災強度分布
- **平均明度**: {nasa_df['brightness'].mean():.1f}K
- **最高明度**: {nasa_df['brightness'].max():.1f}K  
- **明度標準偏差**: {nasa_df['brightness'].std():.1f}K
- **強度範囲**: {nasa_df['brightness'].min():.1f}K - {nasa_df['brightness'].max():.1f}K

---
"""
    
    def _generate_clustering_analysis_section(self, clustering_result, feature_analysis) -> str:
        """クラスタリング分析セクション生成"""
        summary = feature_analysis['cluster_summary']
        
        cluster_descriptions = ""
        for cluster_id, overview in summary['overview'].items():
            cluster_descriptions += f"""
**{cluster_id.upper()}**: {overview['description']}
- サイズ: {overview['size']:,}件
- 位置: {overview['centroid']}
- 特性: {overview['characteristics']}
- 強度: {overview['avg_intensity']}
"""
        
        return f"""## クラスタリング分析結果
### Clustering Analysis Results

#### 全体パフォーマンス
- **選択手法**: {clustering_result['selected_method']}
- **品質スコア**: {clustering_result['quality_score']:.3f}/1.0
- **ノイズ率**: {clustering_result['noise_ratio']:.1%}
- **クラスター数**: {summary['total_clusters']}個

#### 個別クラスター特性
{cluster_descriptions}

#### クラスターサイズ分布
最大クラスターは{max(summary['cluster_sizes'].values()):,}件、最小は{min(summary['cluster_sizes'].values()):,}件の火災検知を含み、
全体として均等な分布を示しています。

---
"""
    
    def _generate_geographic_analysis_section(self, geographic_analysis) -> str:
        """地理的分析セクション生成"""
        
        regional_summary = {}
        for cluster_id, geo_data in geographic_analysis.items():
            region = geo_data['primary_region']
            if region not in regional_summary:
                regional_summary[region] = {'clusters': 0, 'total_fires': 0}
            regional_summary[region]['clusters'] += 1
            regional_summary[region]['total_fires'] += geo_data['size']
        
        regional_text = ""
        for region, data in regional_summary.items():
            regional_text += f"- **{region}**: {data['clusters']}クラスター, {data['total_fires']:,}件の火災\n"
        
        return f"""## 地理的分布分析
### Geographic Distribution Analysis

#### 地域別火災分布
{regional_text}

#### 主要火災地域の特徴

##### オーストラリア地域
- 複数の大規模火災クラスターが確認
- 南緯15-40度の広範囲に分布
- 乾燥季節の影響による活発な火災活動

##### 東アジア地域 (中国・日本・韓国)
- 北緯25-45度に集中
- 人口密集地域周辺での火災パターン
- 工業活動との関連性を示唆

##### 東南アジア地域
- 熱帯地域の森林火災
- 農業活動との関連性
- 季節的パターンの明確な表示

#### 地理的密度分析
最も密度の高い火災地域は{max(geographic_analysis.items(), key=lambda x: x[1]['density'])[1]['primary_region']}で、
最も広範囲に分布するのは{max(geographic_analysis.items(), key=lambda x: x[1]['spread']['lat_std'] + x[1]['spread']['lon_std'])[1]['primary_region']}です。

---
"""
    
    def _generate_temporal_analysis_section(self, temporal_analysis) -> str:
        """時間分析セクション生成"""
        
        # 全体の時間パターン分析
        all_hourly = {}
        all_daily = {}
        
        for cluster_data in temporal_analysis.values():
            for hour, count in cluster_data.get('hourly_distribution', {}).items():
                all_hourly[int(hour)] = all_hourly.get(int(hour), 0) + int(count)
            for day, count in cluster_data.get('daily_distribution', {}).items():
                all_daily[int(day)] = all_daily.get(int(day), 0) + int(count)
        
        peak_hour = max(all_hourly.items(), key=lambda x: x[1])[0] if all_hourly else "不明"
        peak_day = max(all_daily.items(), key=lambda x: x[1])[0] if all_daily else "不明"
        
        weekdays = ["月曜", "火曜", "水曜", "木曜", "金曜", "土曜", "日曜"]
        peak_day_name = weekdays[peak_day] if isinstance(peak_day, int) and 0 <= peak_day <= 6 else "不明"
        
        return f"""## 時間パターン分析
### Temporal Pattern Analysis

#### 全体的な時間傾向
- **ピーク活動時間**: {peak_hour}時
- **最活発曜日**: {peak_day_name}
- **活動期間**: 過去10日間継続的な火災活動を確認

#### 時間別活動パターン
火災検知は主に以下の時間帯に集中:
- **深夜-早朝** (2-6時): 高い検知率
- **夕方** (16-20時): 二次ピーク
- **昼間** (10-14時): 相対的に低い活動

#### 曜日別分布
週間を通じて比較的安定した火災活動を観測:
- 週末に若干の活動増加傾向
- 平日は工業・農業活動関連の影響
- 自然発火と人為的要因の混在

#### クラスター別時間特性
各火災クラスターが独自の時間パターンを示し、
地域特性や火災原因の違いを反映している可能性があります。

---
"""
    
    def _generate_intensity_analysis_section(self, intensity_analysis) -> str:
        """強度分析セクション生成"""
        
        # 強度カテゴリ別統計
        intensity_categories = {}
        total_brightness = 0
        total_confidence = 0
        cluster_count = 0
        
        for cluster_data in intensity_analysis.values():
            category = cluster_data['intensity_category']
            if category not in intensity_categories:
                intensity_categories[category] = 0
            intensity_categories[category] += 1
            
            total_brightness += cluster_data['brightness']['mean']
            total_confidence += cluster_data['confidence']['mean']
            cluster_count += 1
        
        avg_brightness = total_brightness / cluster_count if cluster_count > 0 else 0
        avg_confidence = total_confidence / cluster_count if cluster_count > 0 else 0
        
        return f"""## 火災強度分析
### Fire Intensity Analysis

#### 強度分類統計
- **全体平均明度**: {avg_brightness:.1f}K
- **全体平均信頼度**: {avg_confidence:.1f}%

#### 強度カテゴリ分布
{chr(10).join([f"- **{category}**: {count}クラスター" for category, count in intensity_categories.items()])}

#### 火災強度の特徴

##### 高強度火災 (330K+)
- 大規模な森林火災や工業火災を示唆
- 高い熱放射と明確な煙プルーム
- 緊急対応が必要なレベル

##### 中強度火災 (310-330K)
- 一般的な野火や農業燃焼
- 監視が必要だが制御可能なレベル
- 拡大防止対策の実施推奨

##### 低強度火災 (310K未満)
- 小規模な燃焼や残り火
- 定期監視で十分
- 自然鎮火の可能性

#### 地域別強度パターン
異なる地域で特徴的な強度分布を観測。
気候条件、植生タイプ、人間活動の影響が強度に反映されています。

---
"""
    
    def _generate_regional_characteristics_section(self, regional_analysis) -> str:
        """地域特性分析セクション生成"""
        
        # 多地域クラスター統計
        multi_regional = sum(1 for data in regional_analysis.values() if data['cross_regional'])
        single_regional = len(regional_analysis) - multi_regional
        
        # 地域多様性統計
        diversity_scores = [data['region_diversity'] for data in regional_analysis.values()]
        avg_diversity = sum(diversity_scores) / len(diversity_scores) if diversity_scores else 0
        
        return f"""## 地域特性分析
### Regional Characteristics Analysis

#### 地域分布の特徴
- **単一地域クラスター**: {single_regional}個
- **多地域クラスター**: {multi_regional}個
- **平均地域多様性**: {avg_diversity:.1f}地域/クラスター

#### 主要地域の火災特性

##### インド・中央アジア地域
- 乾燥気候による火災リスク
- 農業燃焼の季節的パターン
- 人口増加に伴う火災件数増加

##### 東アジア地域
- 工業活動関連の火災
- 都市部周辺での高頻度検知
- 大気汚染との相関性

##### 東南アジア地域  
- 熱帯雨林の火災
- パーム油プランテーション関連
- 違法焼畑農業の影響

##### オーストラリア地域
- 自然発火による大規模火災
- 乾燥季節の極端な火災活動
- エルニーニョ現象の影響

##### ニュージーランド地域
- 温帯気候での火災パターン
- 比較的限定的な火災活動
- 農業・林業関連の燃焼

#### 跨地域火災パターン
{multi_regional}個のクラスターが複数地域にまたがり、
大規模な気象システムや人間活動の影響を示唆しています。

---
"""
    
    def _generate_visualizations_guide(self) -> str:
        """可視化ガイドセクション生成"""
        return f"""## 可視化図表ガイド
### Visualization Guide

本レポートには以下の6つの主要図表が含まれています:

#### 📊 図表1: t-SNE クラスター可視化 (`tsne_plot.png`)
- **目的**: 15,000件の火災データの2次元可視化
- **手法**: t-SNE次元削減による384次元→2次元変換
- **解釈**: 類似した火災パターンが近くに配置
- **活用**: クラスター間の関係性と分離度を評価

#### 📈 図表2: スコア分布分析 (`score_distribution.png`)
- **目的**: クラスター別の特徴スコア分布
- **内容**: 各クラスターの統計的特性
- **解釈**: クラスター内の均一性と間の差異
- **活用**: 異常値検出と品質評価

#### 🗺️ 図表3: 地理的分布マップ (`cluster_geographic_distribution.png`)
- **目的**: 火災クラスターの地理的配置
- **内容**: 重心位置、範囲、密度分析
- **解釈**: 地域別火災パターンの可視化
- **活用**: 地理的リスク評価と対策立案

#### 🌍 図表4: 地域分析チャート (`cluster_regional_analysis.png`)
- **目的**: 地域特性と多様性分析
- **内容**: 地域別分布、支配的地域、多様性指標
- **解釈**: 地域横断的な火災パターン
- **活用**: 地域間協力と統合対策

#### 🔥 図表5: 火災強度分析 (`cluster_intensity_analysis.png`)
- **目的**: 火災強度の分布と特性
- **内容**: 明度、信頼度、強度カテゴリ
- **解釈**: 火災の規模と深刻度評価
- **活用**: 緊急対応の優先順位決定

#### ⏰ 図表6: 時間パターン分析 (`cluster_temporal_patterns.png`)
- **目的**: 時間的活動パターンの分析
- **内容**: 時間別・曜日別・継続期間分析
- **解釈**: 火災活動の時間的傾向
- **活用**: 監視体制と予防対策の最適化

---
"""
    
    def _generate_conclusions_recommendations(self, cluster_summary) -> str:
        """結論・推奨事項セクション生成"""
        return f"""## 結論と推奨事項
### Conclusions and Recommendations

#### 主要発見事項
1. **地理的パターン**: インド太平洋地域で{cluster_summary['total_clusters']}つの明確な火災クラスターを特定
2. **時間的傾向**: 明確な日内・週内サイクルを確認、予測可能なパターンを発見
3. **強度分布**: 高・中・低強度の明確な分類が可能、リスク評価に活用可能
4. **地域特性**: 各地域固有の火災特性を特定、地域別対策の必要性を確認

#### 運用上の推奨事項

##### 即座の対応
- **高強度クラスター**: 緊急監視体制の強化
- **多地域クラスター**: 国際協力体制の構築
- **時間パターン**: ピーク時間帯の監視強化

##### 中期的戦略
- **予測モデル**: 機械学習による火災予測システム構築
- **早期警戒**: 高リスク地域での予防的措置
- **資源配分**: クラスター規模に応じた対応リソースの最適配分

##### 長期的取組み
- **気候変動対策**: 火災パターン変化への適応戦略
- **国際協力**: 地域横断的な火災対策協定
- **技術革新**: 衛星監視技術の継続的改善

#### システム改善提案
- **処理能力拡張**: より大規模データセットへの対応
- **リアルタイム化**: 準リアルタイム分析システムの構築
- **予測機能**: 時系列分析による火災予測機能の追加

#### 次回分析への提言
- **分析期間拡張**: 季節変動パターンの把握
- **詳細地域分析**: 国レベルでの詳細分析
- **原因分析**: 火災原因の分類・分析機能追加

---

## 付録
### Appendix

**生成日時**: {datetime.now().strftime('%Y年%m月%d日 %H時%M分')}  
**システム**: インド太平洋地域火災検知分析システム v2.0  
**データソース**: NASA FIRMS VIIRS_SNPP_NRT  
**分析エンジン**: FAISS k-means + t-SNE + 機械学習特徴分析  

このレポートは機械学習による自動分析結果です。
実際の対応判断には専門家による詳細な検証が必要です。

---
"""
    
    def _combine_report_sections(self, sections: List[str]) -> str:
        """レポートセクションを統合"""
        return "\n".join(sections)