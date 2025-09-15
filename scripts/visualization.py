"""
可視化・出力モジュール
t-SNE次元削減、プロット生成、CSV出力機能
"""

import os
import json
import numpy as np
import pandas as pd
import torch
from typing import List, Dict, Tuple, Optional
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.manifold import TSNE
from sklearn.decomposition import PCA
import logging

# ログ設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 日本語フォント設定(Matplotlib用 - Windows対応)
plt.rcParams['font.family'] = ['DejaVu Sans', 'Yu Gothic', 'Meiryo', 'MS Gothic']


class VisualizationManager:
    """可視化・出力マネージャークラス"""
    
    def __init__(self, output_dir: str = "outputs", figsize: Tuple[int, int] = (12, 8)):
        """
        Args:
            output_dir: 出力ディレクトリ
            figsize: 図のサイズ
        """
        self.output_dir = output_dir
        self.figsize = figsize
        os.makedirs(output_dir, exist_ok=True)
        
        # Seabornスタイル設定
        sns.set_style("whitegrid")
        sns.set_palette("husl")
        
        logger.info(f"VisualizationManager initialized with output_dir: {output_dir}")
    
    def reduce_dimensions_tsne(self, features: np.ndarray, perplexity: int = 30, 
                              random_state: int = 42) -> np.ndarray:
        """
        t-SNEで次元削減
        
        Args:
            features: 特徴量行列
            perplexity: t-SNEのperplexityパラメータ
            random_state: 乱数シード
            
        Returns:
            2次元座標
        """
        logger.info(f"Running t-SNE with perplexity={perplexity}")
        
        try:
            # サンプル数がperplexityより少ない場合の調整
            n_samples = features.shape[0]
            if n_samples <= perplexity * 3:
                perplexity = max(5, n_samples // 3)
                logger.warning(f"Adjusted perplexity to {perplexity} due to small sample size")
            
            # t-SNE実行
            tsne = TSNE(
                n_components=2, 
                perplexity=perplexity, 
                random_state=random_state,
                max_iter=1000,
                verbose=1
            )
            
            coords = tsne.fit_transform(features)
            logger.info(f"t-SNE completed. Output shape: {coords.shape}")
            
            return coords
            
        except Exception as e:
            logger.error(f"t-SNE failed: {e}")
            logger.info("Falling back to PCA")
            return self.reduce_dimensions_pca(features)
    
    def reduce_dimensions_pca(self, features: np.ndarray) -> np.ndarray:
        """
        PCAで次元削減(フォールバック)
        
        Args:
            features: 特徴量行列
            
        Returns:
            2次元座標
        """
        logger.info("Running PCA dimensionality reduction")
        
        pca = PCA(n_components=2, random_state=42)
        coords = pca.fit_transform(features)
        
        logger.info(f"PCA completed. Explained variance ratio: {pca.explained_variance_ratio_}")
        return coords
    
    def create_cluster_plot(self, coords: np.ndarray, labels: np.ndarray, 
                           scores: np.ndarray, title: str = "NASA FIRMS Disaster Sentiment Clustering") -> str:
        """
        クラスタープロットを作成(図1: t-SNEクラスター可視化)
        
        Args:
            coords: 2次元座標
            labels: クラスターラベル
            scores: スコア配列
            title: プロットタイトル
            
        Returns:
            保存されたファイルパス
        """
        plt.figure(figsize=self.figsize)
        
        # 散布図作成
        scatter = plt.scatter(
            coords[:, 0], coords[:, 1], 
            c=labels, 
            s=np.abs(scores) * 100 + 20,  # スコアに応じてサイズを変更
            alpha=0.6, 
            cmap='Set1'
        )
        
        # カラーバー追加
        plt.colorbar(scatter, label='Cluster')
        
        # ラベル・タイトル
        plt.xlabel('t-SNE Dimension 1')
        plt.ylabel('t-SNE Dimension 2')
        plt.title(title)
        plt.grid(True, alpha=0.3)
        
        # クラスター中心を計算・表示
        unique_labels = np.unique(labels)
        for label in unique_labels:
            mask = labels == label
            center_x = np.mean(coords[mask, 0])
            center_y = np.mean(coords[mask, 1])
            plt.annotate(f'C{label}', (center_x, center_y), 
                        fontsize=12, fontweight='bold',
                        bbox=dict(boxstyle="round,pad=0.3", facecolor='white', alpha=0.8))
        
        # 保存
        plot_path = os.path.join(self.output_dir, "tsne_plot.png")
        plt.tight_layout()
        plt.savefig(plot_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Cluster plot saved to {plot_path}")
        return plot_path
    
    def create_score_distribution_plot(self, scores: np.ndarray, labels: np.ndarray) -> str:
        """
        スコア分布プロットを作成(図2: センチメント分布)
        
        Args:
            scores: スコア配列
            labels: クラスターラベル
            
        Returns:
            保存されたファイルパス
        """
        plt.figure(figsize=(12, 8))
        
        # クラスターごとのスコア分布
        unique_labels = np.unique(labels)
        colors = plt.cm.Set1(np.linspace(0, 1, len(unique_labels)))
        
        for i, label in enumerate(unique_labels):
            mask = labels == label
            cluster_scores = scores[mask]
            plt.hist(cluster_scores, alpha=0.6, label=f'Cluster {label}', 
                    bins=30, color=colors[i], density=True)
        
        plt.xlabel('Sentiment Score')
        plt.ylabel('Density')
        plt.title('Sentiment Score Distribution by Cluster\n(NASA FIRMS Fire Detection Data)')
        plt.legend()
        plt.grid(True, alpha=0.3)
        
        # 統計情報追加
        overall_mean = np.mean(scores)
        plt.axvline(overall_mean, color='red', linestyle='--', alpha=0.7, 
                   label=f'Overall Mean: {overall_mean:.3f}')
        plt.legend()
        
        # 保存
        dist_path = os.path.join(self.output_dir, "score_distribution.png")
        plt.tight_layout()
        plt.savefig(dist_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Score distribution plot saved to {dist_path}")
        return dist_path
    
    def create_cluster_summary_plot(self, analysis: Dict) -> str:
        """
        クラスターサマリープロットを作成(図3: 4分割サマリー図)
        
        Args:
            analysis: クラスター分析結果
            
        Returns:
            保存されたファイルパス
        """
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 12))
        
        clusters = list(analysis['cluster_sizes'].keys())
        sizes = list(analysis['cluster_sizes'].values())
        means = [analysis['cluster_stats'][c]['score_mean'] for c in clusters]
        stds = [analysis['cluster_stats'][c]['score_std'] for c in clusters]
        
        # 1. クラスターサイズ
        bars1 = ax1.bar(clusters, sizes, alpha=0.7, color='skyblue')
        ax1.set_title('Cluster Sizes\n(NASA FIRMS Fire Detections)', fontsize=14, fontweight='bold')
        ax1.set_xlabel('Cluster ID')
        ax1.set_ylabel('Number of Fire Detections')
        ax1.grid(True, alpha=0.3)
        
        # 数値ラベル追加
        for bar, size in zip(bars1, sizes):
            height = bar.get_height()
            ax1.text(bar.get_x() + bar.get_width()/2., height + max(sizes)*0.01,
                    f'{size:,}', ha='center', va='bottom', fontsize=10)
        
        # 2. 平均センチメントスコア
        bars2 = ax2.bar(clusters, means, alpha=0.7, color='orange')
        ax2.set_title('Average Sentiment Scores', fontsize=14, fontweight='bold')
        ax2.set_xlabel('Cluster ID')
        ax2.set_ylabel('Average Sentiment Score')
        ax2.grid(True, alpha=0.3)
        ax2.axhline(y=0, color='red', linestyle='--', alpha=0.5)
        
        # 数値ラベル追加
        for bar, mean in zip(bars2, means):
            height = bar.get_height()
            ax2.text(bar.get_x() + bar.get_width()/2., height + 0.01,
                    f'{mean:.3f}', ha='center', va='bottom', fontsize=10)
        
        # 3. センチメント標準偏差
        bars3 = ax3.bar(clusters, stds, alpha=0.7, color='green')
        ax3.set_title('Sentiment Score Variability', fontsize=14, fontweight='bold')
        ax3.set_xlabel('Cluster ID')
        ax3.set_ylabel('Standard Deviation')
        ax3.grid(True, alpha=0.3)
        
        # 4. サイズ vs 平均スコア散布図
        scatter = ax4.scatter(sizes, means, s=150, alpha=0.7, c=clusters, cmap='Set1')
        for i, cluster in enumerate(clusters):
            ax4.annotate(f'C{cluster}', (sizes[i], means[i]), 
                        xytext=(5, 5), textcoords='offset points', fontsize=12, fontweight='bold')
        ax4.set_title('Cluster Size vs Average Sentiment', fontsize=14, fontweight='bold')
        ax4.set_xlabel('Cluster Size (Fire Detections)')
        ax4.set_ylabel('Average Sentiment Score')
        ax4.grid(True, alpha=0.3)
        ax4.axhline(y=0, color='red', linestyle='--', alpha=0.5)
        
        plt.suptitle('NASA FIRMS Disaster Sentiment Analysis - Cluster Summary', 
                    fontsize=16, fontweight='bold', y=0.98)
        plt.tight_layout()
        
        # 保存
        summary_path = os.path.join(self.output_dir, "3_cluster_summary.png")
        plt.savefig(summary_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Cluster summary plot saved to {summary_path}")
        return summary_path
    
    def create_geographic_intensity_plot(self, analysis: Dict) -> str:
        """
        地理的火災強度プロットを作成(図4: 地域別火災パターン)
        
        Args:
            analysis: クラスター分析結果
            
        Returns:
            保存されたファイルパス
        """
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
        
        clusters = list(analysis['cluster_sizes'].keys())
        sizes = list(analysis['cluster_sizes'].values())
        means = [analysis['cluster_stats'][c]['score_mean'] for c in clusters]
        
        # 1. 火災強度分布(円グラフ)
        colors = plt.cm.Set1(np.linspace(0, 1, len(clusters)))
        wedges, texts, autotexts = ax1.pie(sizes, labels=[f'Cluster {c}' for c in clusters], 
                                          autopct='%1.1f%%', colors=colors, startangle=90)
        ax1.set_title('Fire Detection Distribution\nby Sentiment Cluster', 
                     fontsize=14, fontweight='bold')
        
        # 2. センチメント強度マップ(棒グラフ + ヒートマップ風)
        bars = ax2.bar(range(len(clusters)), means, color=colors, alpha=0.8)
        ax2.set_title('Sentiment Intensity by Cluster\n(Disaster Response Analysis)', 
                     fontsize=14, fontweight='bold')
        ax2.set_xlabel('Cluster ID')
        ax2.set_ylabel('Average Sentiment Score')
        ax2.set_xticks(range(len(clusters)))
        ax2.set_xticklabels([f'C{c}' for c in clusters])
        ax2.grid(True, alpha=0.3)
        ax2.axhline(y=0, color='red', linestyle='--', alpha=0.7, linewidth=2)
        
        # センチメントレベル注釈
        for i, (bar, mean, size) in enumerate(zip(bars, means, sizes)):
            height = bar.get_height()
            sentiment_label = "Positive" if mean > 0.1 else "Negative" if mean < -0.1 else "Neutral"
            ax2.text(bar.get_x() + bar.get_width()/2., height + 0.02,
                    f'{sentiment_label}\n({size:,} fires)', ha='center', va='bottom', 
                    fontsize=10, fontweight='bold')
        
        plt.suptitle('NASA FIRMS Geographic Fire Intensity & Sentiment Analysis', 
                    fontsize=16, fontweight='bold', y=1.02)
        plt.tight_layout()
        
        # 保存
        geo_path = os.path.join(self.output_dir, "4_geographic_intensity.png")
        plt.savefig(geo_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Geographic intensity plot saved to {geo_path}")
        return geo_path
    
    def run_visualization_pipeline(self, features: np.ndarray, labels: np.ndarray,
                                  scores: np.ndarray, texts: List[str], ids: List[str],
                                  analysis: Dict) -> Dict[str, str]:
        """
        v0-7スタイル可視化パイプライン全体を実行(4つの主要図表)
        
        Args:
            features: 特徴量行列
            labels: クラスターラベル
            scores: スコア配列
            texts: テキストリスト
            ids: ドキュメントID
            analysis: クラスター分析結果
            
        Returns:
            生成されたファイルパスの辞書
        """
        logger.info("Starting NASA FIRMS visualization pipeline (4 main plots)...")
        
        # 1. 次元削減
        logger.info("Step 1: Running t-SNE dimensionality reduction...")
        coords = self.reduce_dimensions_tsne(features)
        
        # 2. 4つの主要プロット生成
        plot_files = {}
        
        logger.info("Step 2: Creating Plot 1 - t-SNE Cluster Visualization...")
        plot_files['plot1_tsne_clusters'] = self.create_cluster_plot(coords, labels, scores)
        
        logger.info("Step 3: Creating Plot 2 - Sentiment Distribution...")
        plot_files['plot2_sentiment_distribution'] = self.create_score_distribution_plot(scores, labels)
        
        logger.info("Step 4: Creating Plot 3 - Cluster Summary (4-panel)...")
        plot_files['plot3_cluster_summary'] = self.create_cluster_summary_plot(analysis)
        
        logger.info("Step 5: Creating Plot 4 - Geographic Intensity Analysis...")
        plot_files['plot4_geographic_intensity'] = self.create_geographic_intensity_plot(analysis)
        
        # 3. 追加データ保存
        logger.info("Step 6: Saving additional data files...")
        
        # CSV出力
        samples_path, full_path = self.save_csv_outputs(coords, labels, scores, texts, ids)
        plot_files['cluster_samples_csv'] = samples_path
        plot_files['full_results_csv'] = full_path
        
        # 座標データ保存
        coords_path = os.path.join(self.output_dir, "tsne_coordinates.npy")
        np.save(coords_path, coords)
        plot_files['tsne_coordinates'] = coords_path
        
        logger.info("=== NASA FIRMS Visualization Pipeline Complete ===")
        logger.info("Generated 4 main visualization plots:")
        for i, (key, path) in enumerate(plot_files.items(), 1):
            if key.startswith('plot'):
                logger.info(f"  {i}. {key}: {path}")
        
        return plot_files

    def sample_representative_documents(self, labels: np.ndarray, scores: np.ndarray,
                                      texts: List[str], ids: List[str], 
                                      n_per_cluster: int = 10) -> pd.DataFrame:
        """
        各クラスターから代表的なドキュメントをサンプリング
        
        Args:
            labels: クラスターラベル
            scores: スコア配列
            texts: テキストリスト
            ids: ドキュメントID
            n_per_cluster: クラスターあたりのサンプル数
            
        Returns:
            サンプルされたデータのDataFrame
        """
        sampled_data = []
        
        unique_labels = np.unique(labels)
        for label in unique_labels:
            mask = labels == label
            cluster_indices = np.where(mask)[0]
            cluster_scores = scores[mask]
            
            # スコア順でソート(降順)
            sorted_indices = np.argsort(cluster_scores)[::-1]
            
            # 上位n_per_cluster個を選択
            selected_indices = sorted_indices[:n_per_cluster]
            
            for idx in selected_indices:
                original_idx = cluster_indices[idx]
                sampled_data.append({
                    'id': ids[original_idx],
                    'cluster': int(label),
                    'text': texts[original_idx],
                    'score': float(scores[original_idx])
                })
        
        df = pd.DataFrame(sampled_data)
        logger.info(f"Sampled {len(df)} representative documents")
        
        return df
    
    def create_full_dataset_with_coordinates(self, coords: np.ndarray, labels: np.ndarray,
                                           scores: np.ndarray, texts: List[str], 
                                           ids: List[str]) -> pd.DataFrame:
        """
        座標情報を含む完全なデータセットを作成
        
        Args:
            coords: 2次元座標
            labels: クラスターラベル
            scores: スコア配列
            texts: テキストリスト
            ids: ドキュメントID
            
        Returns:
            座標情報を含むDataFrame
        """
        # この関数の実装は後で追加
        pass
    
    def create_cluster_plot(self, coords: np.ndarray, labels: np.ndarray, 
                           scores: np.ndarray, title: str = "Disaster Text Clustering") -> str:
        """
        クラスタープロットを作成
        
        Args:
            coords: 2次元座標
            labels: クラスターラベル
            scores: スコア配列
            title: プロットタイトル
            
        Returns:
            保存されたファイルパス
        """
        plt.figure(figsize=self.figsize)
        
        # 散布図作成
        scatter = plt.scatter(
            coords[:, 0], coords[:, 1], 
            c=labels, 
            s=scores * 100,  # スコアに応じてサイズを変更
            alpha=0.6, 
            cmap='Set1'
        )
        
        # カラーバー追加
        plt.colorbar(scatter, label='Cluster')
        
        # ラベル・タイトル
        plt.xlabel('Dimension 1')
        plt.ylabel('Dimension 2')
        plt.title(title)
        plt.grid(True, alpha=0.3)
        
        # クラスター中心を計算・表示
        unique_labels = np.unique(labels)
        for label in unique_labels:
            mask = labels == label
            center_x = np.mean(coords[mask, 0])
            center_y = np.mean(coords[mask, 1])
            plt.annotate(f'C{label}', (center_x, center_y), 
                        fontsize=12, fontweight='bold',
                        bbox=dict(boxstyle="round,pad=0.3", facecolor='white', alpha=0.8))
        
        # 保存
        plot_path = os.path.join(self.output_dir, "tsne_plot.png")
        plt.tight_layout()
        plt.savefig(plot_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Cluster plot saved to {plot_path}")
        return plot_path
    
    def create_score_distribution_plot(self, scores: np.ndarray, labels: np.ndarray) -> str:
        """
        スコア分布プロットを作成
        
        Args:
            scores: スコア配列
            labels: クラスターラベル
            
        Returns:
            保存されたファイルパス
        """
        plt.figure(figsize=(10, 6))
        
        # クラスターごとのスコア分布
        unique_labels = np.unique(labels)
        for label in unique_labels:
            mask = labels == label
            cluster_scores = scores[mask]
            plt.hist(cluster_scores, alpha=0.6, label=f'Cluster {label}', bins=20)
        
        plt.xlabel('Sentiment Score')
        plt.ylabel('Frequency')
        plt.title('Score Distribution by Cluster')
        plt.legend()
        plt.grid(True, alpha=0.3)
        
        # 保存
        dist_path = os.path.join(self.output_dir, "score_distribution.png")
        plt.tight_layout()
        plt.savefig(dist_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Score distribution plot saved to {dist_path}")
        return dist_path
    
    def create_cluster_summary_plot(self, analysis: Dict) -> str:
        """
        クラスターサマリープロットを作成
        
        Args:
            analysis: クラスター分析結果
            
        Returns:
            保存されたファイルパス
        """
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 10))
        
        clusters = list(analysis['cluster_sizes'].keys())
        sizes = list(analysis['cluster_sizes'].values())
        means = [analysis['cluster_stats'][c]['score_mean'] for c in clusters]
        stds = [analysis['cluster_stats'][c]['score_std'] for c in clusters]
        
        # 1. クラスターサイズ
        ax1.bar(clusters, sizes, alpha=0.7)
        ax1.set_title('Cluster Sizes')
        ax1.set_xlabel('Cluster')
        ax1.set_ylabel('Number of Documents')
        
        # 2. 平均スコア
        ax2.bar(clusters, means, alpha=0.7, color='orange')
        ax2.set_title('Average Scores by Cluster')
        ax2.set_xlabel('Cluster')
        ax2.set_ylabel('Average Score')
        
        # 3. スコア標準偏差
        ax3.bar(clusters, stds, alpha=0.7, color='green')
        ax3.set_title('Score Standard Deviation')
        ax3.set_xlabel('Cluster')
        ax3.set_ylabel('Standard Deviation')
        
        # 4. サイズvs平均スコア散布図
        ax4.scatter(sizes, means, s=100, alpha=0.7)
        for i, cluster in enumerate(clusters):
            ax4.annotate(f'C{cluster}', (sizes[i], means[i]), 
                        xytext=(5, 5), textcoords='offset points')
        ax4.set_title('Cluster Size vs Average Score')
        ax4.set_xlabel('Cluster Size')
        ax4.set_ylabel('Average Score')
        
        plt.tight_layout()
        
        # 保存
        summary_path = os.path.join(self.output_dir, "cluster_summary.png")
        plt.savefig(summary_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Cluster summary plot saved to {summary_path}")
        return summary_path
    
    def sample_representative_documents(self, labels: np.ndarray, scores: np.ndarray,
                                      texts: List[str], ids: List[str], 
                                      n_per_cluster: int = 10) -> pd.DataFrame:
        """
        各クラスターから代表的なドキュメントをサンプリング
        
        Args:
            labels: クラスターラベル
            scores: スコア配列
            texts: テキストリスト
            ids: ドキュメントID
            n_per_cluster: クラスターあたりのサンプル数
            
        Returns:
            サンプルされたデータのDataFrame
        """
        sampled_data = []
        
        unique_labels = np.unique(labels)
        for label in unique_labels:
            mask = labels == label
            cluster_indices = np.where(mask)[0]
            cluster_scores = scores[mask]
            
            # スコア順でソート(降順)
            sorted_indices = np.argsort(cluster_scores)[::-1]
            
            # 上位n_per_cluster個を選択
            selected_indices = sorted_indices[:n_per_cluster]
            
            for idx in selected_indices:
                original_idx = cluster_indices[idx]
                sampled_data.append({
                    'id': ids[original_idx],
                    'cluster': int(label),
                    'text': texts[original_idx],
                    'score': float(scores[original_idx])
                })
        
        df = pd.DataFrame(sampled_data)
        logger.info(f"Sampled {len(df)} representative documents")
        
        return df
    
    def create_full_dataset_with_coordinates(self, coords: np.ndarray, labels: np.ndarray,
                                           scores: np.ndarray, texts: List[str], 
                                           ids: List[str]) -> pd.DataFrame:
        """
        座標情報を含む完全なデータセットを作成
        
        Args:
            coords: 2次元座標
            labels: クラスターラベル
            scores: スコア配列
            texts: テキストリスト
            ids: ドキュメントID
            
        Returns:
            完全なDataFrame
        """
        df = pd.DataFrame({
            'id': ids,
            'text': texts,
            'score': scores,
            'cluster': labels,
            'x': coords[:, 0],
            'y': coords[:, 1]
        })
        
        return df
    
    def save_csv_outputs(self, coords: np.ndarray, labels: np.ndarray, scores: np.ndarray,
                        texts: List[str], ids: List[str]) -> Tuple[str, str]:
        """
        CSV形式で結果を保存
        
        Args:
            coords: 2次元座標
            labels: クラスターラベル
            scores: スコア配列
            texts: テキストリスト
            ids: ドキュメントID
            
        Returns:
            (代表サンプルCSV, 完全データCSV)
        """
        # 代表サンプル
        samples_df = self.sample_representative_documents(labels, scores, texts, ids)
        samples_path = os.path.join(self.output_dir, "cluster_samples.csv")
        samples_df.to_csv(samples_path, index=False, encoding='utf-8')
        
        # 完全データセット
        full_df = self.create_full_dataset_with_coordinates(coords, labels, scores, texts, ids)
        full_path = os.path.join(self.output_dir, "full_results.csv")
        full_df.to_csv(full_path, index=False, encoding='utf-8')
        
        logger.info(f"CSV outputs saved:")
        logger.info(f"  Cluster samples: {samples_path}")
        logger.info(f"  Full results: {full_path}")
        
        return samples_path, full_path
    
    def run_visualization_pipeline(self, features: np.ndarray, labels: np.ndarray,
                                  scores: np.ndarray, texts: List[str], ids: List[str],
                                  analysis: Dict) -> Dict[str, str]:
        """
        可視化パイプライン全体を実行
        
        Args:
            features: 特徴量行列
            labels: クラスターラベル
            scores: スコア配列
            texts: テキストリスト
            ids: ドキュメントID
            analysis: クラスター分析結果
            
        Returns:
            生成されたファイルパスの辞書
        """
        logger.info("Starting visualization pipeline...")
        
        # 1. 次元削減
        coords = self.reduce_dimensions_tsne(features)
        
        # 2. プロット生成
        plot_files = {}
        
        plot_files['cluster_plot'] = self.create_cluster_plot(coords, labels, scores)
        plot_files['score_distribution'] = self.create_score_distribution_plot(scores, labels)
        plot_files['cluster_summary'] = self.create_cluster_summary_plot(analysis)
        
        # 3. CSV出力
        samples_path, full_path = self.save_csv_outputs(coords, labels, scores, texts, ids)
        plot_files['cluster_samples_csv'] = samples_path
        plot_files['full_results_csv'] = full_path
        
        # 4. 座標データ保存
        coords_path = os.path.join(self.output_dir, "coordinates.npy")
        np.save(coords_path, coords)
        plot_files['coordinates'] = coords_path
        
        logger.info("Visualization pipeline completed")
        
        return plot_files


def main():
    """メイン実行関数(テスト用)"""
    # テストデータ作成
    n_samples = 100
    feature_dim = 50
    
    # ダミーデータ
    features = np.random.randn(n_samples, feature_dim)
    labels = np.random.randint(0, 3, n_samples)
    scores = np.random.rand(n_samples)
    texts = [f"これはテストドキュメント{i}です。災害関連のテキストサンプル。" for i in range(n_samples)]
    ids = [f"doc_{i:03d}" for i in range(n_samples)]
    
    # ダミー分析結果
    analysis = {
        'total_clusters': 3,
        'cluster_sizes': {0: 35, 1: 30, 2: 35},
        'cluster_stats': {
            0: {'score_mean': 0.6, 'score_std': 0.2, 'score_min': 0.1, 'score_max': 0.9},
            1: {'score_mean': 0.4, 'score_std': 0.15, 'score_min': 0.1, 'score_max': 0.8},
            2: {'score_mean': 0.7, 'score_std': 0.18, 'score_min': 0.2, 'score_max': 0.95}
        },
        'sample_texts': {i: [] for i in range(3)}
    }
    
    # 可視化実行
    viz_manager = VisualizationManager()
    
    try:
        output_files = viz_manager.run_visualization_pipeline(
            features, labels, scores, texts, ids, analysis
        )
        
        print("✓ Visualization completed successfully!")
        print("Generated files:")
        for key, filepath in output_files.items():
            print(f"  {key}: {filepath}")
        
    except Exception as e:
        print(f"✗ Visualization failed: {e}")
        logger.error(f"Error details: {e}", exc_info=True)


if __name__ == "__main__":
    main()