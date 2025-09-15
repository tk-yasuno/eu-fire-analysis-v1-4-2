#!/usr/bin/env python3
"""
動的クラスタリング選択システム
HDBSCAN vs FAISS k-means の品質評価による自動選択
"""

import os
import sys
import json
import logging
import numpy as np
import matplotlib.pyplot as plt
from sklearn.metrics import silhouette_score, calinski_harabasz_score, davies_bouldin_score
from sklearn.preprocessing import StandardScaler
import time
from typing import Dict, List, Tuple, Any, Optional
from dataclasses import dataclass
from enum import Enum

# 既存モジュール
scripts_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'scripts')
sys.path.append(scripts_dir)
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from hdbscan_clustering import HDBSCANClustering

# ログ設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ClusteringMethod(Enum):
    """クラスタリング手法"""
    HDBSCAN = "hdbscan"
    KMEANS = "kmeans"
    HYBRID = "hybrid"


@dataclass
class ClusteringQualityMetrics:
    """クラスタリング品質メトリクス"""
    method: str
    n_clusters: int
    n_noise: int
    silhouette_score: float
    calinski_harabasz_score: float
    davies_bouldin_score: float
    processing_time: float
    noise_ratio: float
    cluster_sizes: List[int]
    quality_score: float  # 総合品質スコア


@dataclass
class ClusteringResult:
    """クラスタリング結果"""
    method: str
    labels: np.ndarray
    centroids: Optional[np.ndarray]
    metrics: ClusteringQualityMetrics
    probabilities: Optional[np.ndarray] = None
    hierarchy: Optional[Any] = None


class AdaptiveClusteringSelector:
    """適応型クラスタリング選択器"""
    
    def __init__(self, 
                 output_dir: str = "data_adaptive_clustering",
                 min_cluster_quality: float = 0.3,
                 max_noise_ratio: float = 0.8):
        """
        Args:
            output_dir: 出力ディレクトリ
            min_cluster_quality: 最小品質スコア閾値
            max_noise_ratio: 最大ノイズ比率閾値
        """
        self.output_dir = output_dir
        self.min_cluster_quality = min_cluster_quality
        self.max_noise_ratio = max_noise_ratio
        os.makedirs(output_dir, exist_ok=True)
        
        # 重み設定（品質スコア計算用）
        self.weights = {
            "silhouette": 0.3,
            "calinski_harabasz": 0.2,
            "davies_bouldin": 0.2,  # 低いほど良い（逆転）
            "noise_penalty": 0.2,
            "cluster_balance": 0.1
        }
        
    def run_hdbscan_clustering(self, 
                              embeddings: np.ndarray,
                              min_cluster_size: int = 50,
                              min_samples: int = 10) -> ClusteringResult:
        """HDBSCAN実行"""
        logger.info(f"Running HDBSCAN clustering (min_cluster_size={min_cluster_size})")
        
        start_time = time.time()
        
        # HDBSCAN実行
        hdbscan_clusterer = HDBSCANClustering(
            min_cluster_size=min_cluster_size,
            min_samples=min_samples
        )
        
        hdbscan_clusterer.fit(embeddings)
        labels = hdbscan_clusterer.labels_
        
        processing_time = time.time() - start_time
        
        # メトリクス計算
        metrics = self._calculate_clustering_metrics(
            embeddings, labels, "HDBSCAN", processing_time
        )
        
        return ClusteringResult(
            method="HDBSCAN",
            labels=labels,
            centroids=None,
            metrics=metrics,
            probabilities=hdbscan_clusterer.probabilities_,
            hierarchy=hdbscan_clusterer.condensed_tree_
        )
    
    def run_kmeans_clustering(self, 
                             embeddings: np.ndarray,
                             n_clusters: int = 8) -> ClusteringResult:
        """FAISS k-means実行"""
        logger.info(f"Running FAISS k-means clustering (k={n_clusters})")
        
        start_time = time.time()
        
        # FAISS k-means実行
        sys.path.append('scripts')
        from clustering import FastClustering
        fast_clustering = FastClustering(output_dir=self.output_dir)
        labels, centroids = fast_clustering.kmeans_clustering(
            features=embeddings,
            k=n_clusters,
            verbose=False
        )
        
        processing_time = time.time() - start_time
        
        # メトリクス計算
        metrics = self._calculate_clustering_metrics(
            embeddings, labels, "FAISS k-means", processing_time
        )
        
        return ClusteringResult(
            method="FAISS k-means",
            labels=labels,
            centroids=centroids,
            metrics=metrics
        )
    
    def _calculate_clustering_metrics(self, 
                                    embeddings: np.ndarray,
                                    labels: np.ndarray,
                                    method: str,
                                    processing_time: float) -> ClusteringQualityMetrics:
        """クラスタリング品質メトリクス計算"""
        
        unique_labels = set(labels)
        n_clusters = len(unique_labels) - (1 if -1 in unique_labels else 0)
        n_noise = np.sum(labels == -1) if -1 in unique_labels else 0
        noise_ratio = n_noise / len(labels)
        
        # クラスターサイズ
        cluster_sizes = []
        for label in unique_labels:
            if label != -1:
                cluster_sizes.append(int(np.sum(labels == label)))
        
        # 品質メトリクス計算
        silhouette = 0.0
        calinski_harabasz = 0.0
        davies_bouldin = float('inf')
        
        try:
            if n_clusters > 1:
                # ノイズポイントを除外して計算
                if -1 in unique_labels:
                    non_noise_mask = labels != -1
                    if np.sum(non_noise_mask) > 1:
                        clean_embeddings = embeddings[non_noise_mask]
                        clean_labels = labels[non_noise_mask]
                        
                        if len(set(clean_labels)) > 1:
                            silhouette = silhouette_score(clean_embeddings, clean_labels)
                            calinski_harabasz = calinski_harabasz_score(clean_embeddings, clean_labels)
                            davies_bouldin = davies_bouldin_score(clean_embeddings, clean_labels)
                else:
                    silhouette = silhouette_score(embeddings, labels)
                    calinski_harabasz = calinski_harabasz_score(embeddings, labels)
                    davies_bouldin = davies_bouldin_score(embeddings, labels)
                    
        except Exception as e:
            logger.warning(f"Could not calculate some metrics for {method}: {e}")
        
        # 総合品質スコア計算
        quality_score = self._calculate_quality_score(
            silhouette, calinski_harabasz, davies_bouldin, 
            noise_ratio, cluster_sizes, n_clusters
        )
        
        return ClusteringQualityMetrics(
            method=method,
            n_clusters=n_clusters,
            n_noise=n_noise,
            silhouette_score=silhouette,
            calinski_harabasz_score=calinski_harabasz,
            davies_bouldin_score=davies_bouldin,
            processing_time=processing_time,
            noise_ratio=noise_ratio,
            cluster_sizes=cluster_sizes,
            quality_score=quality_score
        )
    
    def _calculate_quality_score(self, 
                               silhouette: float,
                               calinski_harabasz: float,
                               davies_bouldin: float,
                               noise_ratio: float,
                               cluster_sizes: List[int],
                               n_clusters: int) -> float:
        """総合品質スコア計算"""
        
        # シルエット係数（-1 to 1 → 0 to 1）
        silhouette_norm = (silhouette + 1) / 2
        
        # Calinski-Harabasz指数（正規化）
        calinski_norm = min(calinski_harabasz / 1000, 1.0) if calinski_harabasz > 0 else 0
        
        # Davies-Bouldin指数（低いほど良い → 逆転して正規化）
        davies_norm = max(0, 1 - davies_bouldin / 10) if davies_bouldin != float('inf') else 0
        
        # ノイズペナルティ（ノイズ比率が高いほどペナルティ）
        noise_penalty = max(0, 1 - noise_ratio * 2)
        
        # クラスターバランス（サイズの均等性）
        cluster_balance = 0
        if len(cluster_sizes) > 1:
            mean_size = np.mean(cluster_sizes)
            std_size = np.std(cluster_sizes)
            cluster_balance = max(0, 1 - std_size / mean_size) if mean_size > 0 else 0
        elif len(cluster_sizes) == 1:
            cluster_balance = 0.5  # 単一クラスターは中程度のスコア
        
        # 重み付き総合スコア
        quality_score = (
            self.weights["silhouette"] * silhouette_norm +
            self.weights["calinski_harabasz"] * calinski_norm +
            self.weights["davies_bouldin"] * davies_norm +
            self.weights["noise_penalty"] * noise_penalty +
            self.weights["cluster_balance"] * cluster_balance
        )
        
        return quality_score
    
    def select_best_clustering(self, 
                             embeddings: np.ndarray,
                             hdbscan_params: Dict = None,
                             kmeans_params: Dict = None) -> Tuple[ClusteringResult, Dict[str, Any]]:
        """最適クラスタリング手法選択"""
        
        logger.info("=== Adaptive Clustering Selection ===")
        
        # デフォルトパラメータ
        if hdbscan_params is None:
            hdbscan_params = {"min_cluster_size": max(10, len(embeddings) // 20)}
        if kmeans_params is None:
            kmeans_params = {"n_clusters": min(8, max(2, len(embeddings) // 10))}
        
        # 大量データの場合はFAISS k-meansを直接選択
        if len(embeddings) > 3000:
            logger.info(f"Large dataset detected ({len(embeddings)} samples). Skipping HDBSCAN, using FAISS k-means.")
            try:
                kmeans_result = self.run_kmeans_clustering(embeddings, **kmeans_params)
                logger.info(f"k-means: {kmeans_result.metrics.n_clusters} clusters, "
                           f"quality={kmeans_result.metrics.quality_score:.3f}")
                selection_reason = f"k-means selected: large dataset optimization ({len(embeddings)} samples)"
                return kmeans_result, {"selection_reason": selection_reason}
            except Exception as e:
                logger.error(f"k-means failed: {e}")
                raise RuntimeError(f"Clustering failed for large dataset: {e}")
        
        results = {}
        
        # HDBSCAN実行（小規模データのみ）
        try:
            hdbscan_result = self.run_hdbscan_clustering(embeddings, **hdbscan_params)
            results["hdbscan"] = hdbscan_result
            logger.info(f"HDBSCAN: {hdbscan_result.metrics.n_clusters} clusters, "
                       f"quality={hdbscan_result.metrics.quality_score:.3f}")
        except Exception as e:
            logger.error(f"HDBSCAN failed: {e}")
            hdbscan_result = None
        
        # k-means実行
        try:
            kmeans_result = self.run_kmeans_clustering(embeddings, **kmeans_params)
            results["kmeans"] = kmeans_result
            logger.info(f"k-means: {kmeans_result.metrics.n_clusters} clusters, "
                       f"quality={kmeans_result.metrics.quality_score:.3f}")
        except Exception as e:
            logger.error(f"k-means failed: {e}")
            kmeans_result = None
        
        # 最適手法選択
        best_result = None
        selection_reason = ""
        
        if hdbscan_result and kmeans_result:
            # 両方成功 → 品質比較
            hdbscan_quality = hdbscan_result.metrics.quality_score
            kmeans_quality = kmeans_result.metrics.quality_score
            
            # 品質閾値チェック
            hdbscan_acceptable = (hdbscan_quality >= self.min_cluster_quality and 
                                hdbscan_result.metrics.noise_ratio <= self.max_noise_ratio)
            kmeans_acceptable = kmeans_quality >= self.min_cluster_quality
            
            if hdbscan_acceptable and kmeans_acceptable:
                # 両方受容可能 → 高品質選択
                if hdbscan_quality > kmeans_quality:
                    best_result = hdbscan_result
                    selection_reason = f"HDBSCAN selected: higher quality ({hdbscan_quality:.3f} vs {kmeans_quality:.3f})"
                else:
                    best_result = kmeans_result
                    selection_reason = f"k-means selected: higher quality ({kmeans_quality:.3f} vs {hdbscan_quality:.3f})"
            elif hdbscan_acceptable:
                best_result = hdbscan_result
                selection_reason = f"HDBSCAN selected: only acceptable method (quality={hdbscan_quality:.3f})"
            elif kmeans_acceptable:
                best_result = kmeans_result
                selection_reason = f"k-means selected: only acceptable method (quality={kmeans_quality:.3f})"
            else:
                # 両方品質不足 → より良い方を選択
                if hdbscan_quality > kmeans_quality:
                    best_result = hdbscan_result
                    selection_reason = f"HDBSCAN selected: best of poor options (quality={hdbscan_quality:.3f})"
                else:
                    best_result = kmeans_result
                    selection_reason = f"k-means selected: best of poor options (quality={kmeans_quality:.3f})"
                    
        elif hdbscan_result:
            best_result = hdbscan_result
            selection_reason = f"HDBSCAN selected: only successful method (quality={hdbscan_result.metrics.quality_score:.3f})"
        elif kmeans_result:
            best_result = kmeans_result
            selection_reason = f"k-means selected: only successful method (quality={kmeans_result.metrics.quality_score:.3f})"
        else:
            raise RuntimeError("Both clustering methods failed")
        
        # 選択結果詳細
        selection_details = {
            "selected_method": best_result.method,
            "selection_reason": selection_reason,
            "comparison_results": {
                method: {
                    "quality_score": result.metrics.quality_score,
                    "n_clusters": result.metrics.n_clusters,
                    "noise_ratio": result.metrics.noise_ratio,
                    "silhouette_score": result.metrics.silhouette_score,
                    "processing_time": result.metrics.processing_time
                } for method, result in results.items()
            },
            "quality_threshold": self.min_cluster_quality,
            "noise_threshold": self.max_noise_ratio,
            "weights": self.weights
        }
        
        logger.info(f"Selection: {selection_reason}")
        
        return best_result, selection_details
    
    def create_comparison_visualization(self, 
                                      embeddings: np.ndarray,
                                      results: Dict[str, ClusteringResult],
                                      selection_details: Dict[str, Any]) -> str:
        """比較可視化作成"""
        
        import umap
        
        # UMAP次元削減
        reducer = umap.UMAP(n_components=2, random_state=42, n_neighbors=min(15, len(embeddings)//4))
        umap_coords = reducer.fit_transform(embeddings)
        
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        
        # サブプロット配置
        plot_positions = [(0, 0), (0, 1)]
        comparison_pos = (1, 0)
        metrics_pos = (1, 1)
        
        # 各手法の結果プロット
        for i, (method, result) in enumerate(results.items()):
            if i >= len(plot_positions):
                break
                
            ax = axes[plot_positions[i]]
            labels = result.labels
            
            # メインクラスター
            unique_labels = set(labels)
            for label in unique_labels:
                if label == -1:
                    continue
                mask = labels == label
                ax.scatter(umap_coords[mask, 0], umap_coords[mask, 1],
                          alpha=0.7, s=50, label=f'Cluster {label}')
            
            # ノイズポイント
            if -1 in unique_labels:
                noise_mask = labels == -1
                ax.scatter(umap_coords[noise_mask, 0], umap_coords[noise_mask, 1],
                          c='red', alpha=0.8, s=30, marker='x', label='Noise')
            
            ax.set_title(f'{result.method}\nQuality: {result.metrics.quality_score:.3f}', 
                        fontweight='bold', fontsize=12)
            ax.set_xlabel('UMAP Dimension 1')
            ax.set_ylabel('UMAP Dimension 2')
            ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=8)
            ax.grid(True, alpha=0.3)
        
        # 品質比較バープロット
        ax_comp = axes[comparison_pos]
        methods = list(results.keys())
        quality_scores = [results[method].metrics.quality_score for method in methods]
        cluster_counts = [results[method].metrics.n_clusters for method in methods]
        
        x = np.arange(len(methods))
        width = 0.35
        
        bars1 = ax_comp.bar(x - width/2, quality_scores, width, label='Quality Score', alpha=0.8)
        bars2 = ax_comp.bar(x + width/2, [c/10 for c in cluster_counts], width, label='Clusters/10', alpha=0.8)
        
        ax_comp.set_xlabel('Method')
        ax_comp.set_ylabel('Score')
        ax_comp.set_title('Quality Comparison')
        ax_comp.set_xticks(x)
        ax_comp.set_xticklabels(methods)
        ax_comp.legend()
        ax_comp.grid(True, alpha=0.3)
        
        # 選択された手法を強調
        selected_idx = methods.index(selection_details["selected_method"])
        bars1[selected_idx].set_color('gold')
        bars1[selected_idx].set_edgecolor('red')
        bars1[selected_idx].set_linewidth(2)
        
        # メトリクス詳細テーブル
        ax_metrics = axes[metrics_pos]
        ax_metrics.axis('tight')
        ax_metrics.axis('off')
        
        # テーブルデータ準備
        table_data = []
        headers = ['Metric', 'HDBSCAN', 'k-means']
        
        if 'hdbscan' in results and 'kmeans' in results:
            metrics_to_show = [
                ('Quality Score', 'quality_score'),
                ('Clusters', 'n_clusters'), 
                ('Noise Points', 'n_noise'),
                ('Silhouette', 'silhouette_score'),
                ('Processing (s)', 'processing_time')
            ]
            
            for name, attr in metrics_to_show:
                hdbscan_val = getattr(results['hdbscan'].metrics, attr)
                kmeans_val = getattr(results['kmeans'].metrics, attr)
                
                if isinstance(hdbscan_val, float):
                    hdbscan_str = f"{hdbscan_val:.3f}"
                    kmeans_str = f"{kmeans_val:.3f}"
                else:
                    hdbscan_str = str(hdbscan_val)
                    kmeans_str = str(kmeans_val)
                
                table_data.append([name, hdbscan_str, kmeans_str])
        
        table = ax_metrics.table(cellText=table_data, colLabels=headers,
                               cellLoc='center', loc='center')
        table.auto_set_font_size(False)
        table.set_fontsize(10)
        table.scale(1, 2)
        
        # 選択理由を表示
        reason_text = f"Selected: {selection_details['selected_method']}\nReason: {selection_details['selection_reason']}"
        ax_metrics.text(0.5, 0.1, reason_text, transform=ax_metrics.transAxes,
                       ha='center', va='bottom', fontsize=10, 
                       bbox=dict(boxstyle="round,pad=0.3", facecolor='lightblue', alpha=0.7))
        
        plt.suptitle('Adaptive Clustering Selection Results', fontsize=16, fontweight='bold')
        plt.tight_layout()
        
        # 保存
        plot_file = os.path.join(self.output_dir, "adaptive_clustering_comparison.png")
        plt.savefig(plot_file, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Comparison visualization saved: {plot_file}")
        return plot_file
    
    def save_selection_results(self, 
                             best_result: ClusteringResult,
                             selection_details: Dict[str, Any]) -> str:
        """選択結果保存"""
        
        # 保存用データ準備
        results_data = {
            "selected_method": best_result.method,
            "selection_timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "selection_details": selection_details,
            "selected_result": {
                "method": best_result.method,
                "labels": best_result.labels.tolist(),
                "metrics": {
                    "quality_score": best_result.metrics.quality_score,
                    "n_clusters": best_result.metrics.n_clusters,
                    "n_noise": best_result.metrics.n_noise,
                    "silhouette_score": best_result.metrics.silhouette_score,
                    "calinski_harabasz_score": best_result.metrics.calinski_harabasz_score,
                    "davies_bouldin_score": best_result.metrics.davies_bouldin_score,
                    "processing_time": best_result.metrics.processing_time,
                    "noise_ratio": best_result.metrics.noise_ratio,
                    "cluster_sizes": best_result.metrics.cluster_sizes
                }
            }
        }
        
        # centroids保存（存在する場合）
        if best_result.centroids is not None:
            centroids_file = os.path.join(self.output_dir, "selected_centroids.npy")
            np.save(centroids_file, best_result.centroids)
            results_data["selected_result"]["centroids_file"] = centroids_file
        
        # 確率保存（HDBSCANの場合）
        if best_result.probabilities is not None:
            probabilities_file = os.path.join(self.output_dir, "selected_probabilities.npy")
            np.save(probabilities_file, best_result.probabilities)
            results_data["selected_result"]["probabilities_file"] = probabilities_file
        
        # JSON保存
        results_file = os.path.join(self.output_dir, "adaptive_clustering_selection.json")
        with open(results_file, 'w', encoding='utf-8') as f:
            json.dump(results_data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Selection results saved: {results_file}")
        return results_file


def main():
    """テスト実行"""
    # ダミーデータ
    np.random.seed(42)
    n_samples = 500
    n_features = 128
    
    # マルチクラスターデータ生成
    embeddings = np.vstack([
        np.random.normal(0, 1, (n_samples//3, n_features)),
        np.random.normal(3, 1, (n_samples//3, n_features)),
        np.random.normal(-2, 0.5, (n_samples//3 + n_samples%3, n_features))
    ])
    
    # 適応型選択システム
    selector = AdaptiveClusteringSelector()
    
    # 最適手法選択
    best_result, selection_details = selector.select_best_clustering(embeddings)
    
    print(f"\n=== Adaptive Clustering Selection Results ===")
    print(f"Selected Method: {best_result.method}")
    print(f"Quality Score: {best_result.metrics.quality_score:.3f}")
    print(f"Clusters: {best_result.metrics.n_clusters}")
    print(f"Noise Points: {best_result.metrics.n_noise}")
    print(f"Selection Reason: {selection_details['selection_reason']}")


if __name__ == "__main__":
    main()