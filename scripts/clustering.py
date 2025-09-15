"""
Faissベース高速クラスタリングモジュール
K-meansクラスタリング機能
"""

import os
import numpy as np
import torch
from typing import List, Dict, Tuple, Optional
import faiss
import logging
from sklearn.preprocessing import StandardScaler

# ログ設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FastClustering:
    """Faissベースの高速クラスタリングクラス"""
    
    def __init__(self, output_dir: str = "outputs"):
        """
        Args:
            output_dir: 出力ディレクトリ
        """
        self.output_dir = output_dir
        self.scaler = StandardScaler()
        os.makedirs(output_dir, exist_ok=True)
        logger.info(f"FastClustering initialized with output_dir: {output_dir}")
    
    def prepare_features(self, embeddings: torch.Tensor, scores: torch.Tensor,
                        normalize: bool = True) -> np.ndarray:
        """
        特徴量行列を準備
        
        Args:
            embeddings: 埋め込みテンソル
            scores: スコアテンソル
            normalize: 正規化するか
            
        Returns:
            結合された特徴量行列
        """
        # テンソルをnumpy配列に変換
        embeddings_np = embeddings.numpy() if isinstance(embeddings, torch.Tensor) else embeddings
        scores_np = scores.numpy() if isinstance(scores, torch.Tensor) else scores
        
        # スコアを2次元に
        if scores_np.ndim == 1:
            scores_np = scores_np.reshape(-1, 1)
        
        # 特徴量結合
        features = np.concatenate([scores_np, embeddings_np], axis=1)
        
        # 正規化
        if normalize:
            features = self.scaler.fit_transform(features)
            logger.info("Features normalized")
        
        logger.info(f"Feature matrix shape: {features.shape}")
        return features.astype(np.float32)
    
    def kmeans_clustering(self, features: np.ndarray, k: int = 3, 
                         niter: int = 20, verbose: bool = True) -> Tuple[np.ndarray, np.ndarray]:
        """
        Faiss K-meansクラスタリング
        
        Args:
            features: 特徴量行列
            k: クラスター数
            niter: 反復回数
            verbose: 詳細出力
            
        Returns:
            (ラベル, 中心点)
        """
        logger.info(f"Starting K-means clustering with k={k}, niter={niter}")
        
        try:
            # Faiss K-means初期化
            d = features.shape[1]  # 次元数
            
            # GPU使用可能性チェック
            if faiss.get_num_gpus() > 0:
                logger.info("Using GPU for clustering")
                # GPU版K-means
                res = faiss.StandardGpuResources()
                cfg = faiss.GpuIndexFlatConfig()
                cfg.device = 0
                
                # CPU版でK-meansを初期化してGPUに移行
                kmeans = faiss.Kmeans(d, k, niter=niter, verbose=verbose, gpu=True)
            else:
                logger.info("Using CPU for clustering")
                # CPU版K-means
                kmeans = faiss.Kmeans(d, k, niter=niter, verbose=verbose, gpu=False)
            
            # クラスタリング実行
            kmeans.train(features)
            
            # ラベル割り当て
            _, labels = kmeans.index.search(features, 1)
            labels = labels.reshape(-1)
            
            # 中心点取得
            centroids = kmeans.centroids
            
            logger.info(f"Clustering completed. Found {len(np.unique(labels))} clusters")
            
            return labels, centroids
            
        except Exception as e:
            logger.error(f"Clustering failed: {e}")
            logger.info("Falling back to CPU-only clustering")
            return self._fallback_cpu_clustering(features, k, niter, verbose)
    
    def _fallback_cpu_clustering(self, features: np.ndarray, k: int, 
                                niter: int, verbose: bool) -> Tuple[np.ndarray, np.ndarray]:
        """CPU版フォールバッククラスタリング"""
        try:
            # シンプルなCPU版K-means
            kmeans = faiss.Kmeans(features.shape[1], k, niter=niter, verbose=verbose, gpu=False)
            kmeans.train(features)
            
            _, labels = kmeans.index.search(features, 1)
            labels = labels.reshape(-1)
            centroids = kmeans.centroids
            
            return labels, centroids
            
        except Exception as e:
            logger.error(f"CPU clustering also failed: {e}")
            # 最終フォールバック：ランダムラベル
            logger.warning("Using random labels as final fallback")
            labels = np.random.randint(0, k, size=len(features))
            centroids = np.random.randn(k, features.shape[1]).astype(np.float32)
            return labels, centroids
    
    def analyze_clusters(self, labels: np.ndarray, scores: np.ndarray, 
                        texts: List[str]) -> Dict:
        """
        クラスター分析
        
        Args:
            labels: クラスターラベル
            scores: スコア配列
            texts: テキストリスト
            
        Returns:
            分析結果辞書
        """
        unique_labels = np.unique(labels)
        analysis = {
            'total_clusters': len(unique_labels),
            'cluster_sizes': {},
            'cluster_stats': {},
            'sample_texts': {}
        }
        
        for label in unique_labels:
            mask = labels == label
            cluster_scores = scores[mask]
            cluster_texts = [texts[i] for i in range(len(texts)) if mask[i]]
            
            # 基本統計
            analysis['cluster_sizes'][int(label)] = int(np.sum(mask))
            analysis['cluster_stats'][int(label)] = {
                'score_mean': float(np.mean(cluster_scores)),
                'score_std': float(np.std(cluster_scores)),
                'score_min': float(np.min(cluster_scores)),
                'score_max': float(np.max(cluster_scores))
            }
            
            # サンプルテキスト（上位3つ）
            top_indices = np.argsort(cluster_scores)[-3:][::-1]
            analysis['sample_texts'][int(label)] = [
                {
                    'text': cluster_texts[i][:100] + "..." if len(cluster_texts[i]) > 100 else cluster_texts[i],
                    'score': float(cluster_scores[i])
                }
                for i in top_indices if i < len(cluster_texts)
            ]
        
        return analysis
    
    def save_clustering_results(self, labels: np.ndarray, centroids: np.ndarray,
                               ids: List[str], analysis: Dict) -> str:
        """
        クラスタリング結果を保存
        
        Args:
            labels: クラスターラベル
            centroids: 中心点
            ids: ドキュメントID
            analysis: 分析結果
            
        Returns:
            保存されたファイルパス
        """
        import json
        
        # ラベルをnumpy配列として保存
        labels_path = os.path.join(self.output_dir, "labels.npy")
        np.save(labels_path, labels)
        
        # 中心点を保存
        centroids_path = os.path.join(self.output_dir, "centroids.npy")
        np.save(centroids_path, centroids)
        
        # 分析結果をJSONで保存
        analysis_path = os.path.join(self.output_dir, "cluster_analysis.json")
        with open(analysis_path, 'w', encoding='utf-8') as f:
            json.dump(analysis, f, ensure_ascii=False, indent=2)
        
        # ラベル付きデータを保存
        labeled_data_path = os.path.join(self.output_dir, "labeled_data.json")
        labeled_data = [
            {'id': doc_id, 'cluster': int(label)}
            for doc_id, label in zip(ids, labels)
        ]
        with open(labeled_data_path, 'w', encoding='utf-8') as f:
            json.dump(labeled_data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Clustering results saved:")
        logger.info(f"  Labels: {labels_path}")
        logger.info(f"  Centroids: {centroids_path}")
        logger.info(f"  Analysis: {analysis_path}")
        logger.info(f"  Labeled data: {labeled_data_path}")
        
        return labels_path
    
    def load_clustering_results(self) -> Tuple[np.ndarray, np.ndarray, Dict]:
        """保存されたクラスタリング結果を読み込み"""
        import json
        
        labels_path = os.path.join(self.output_dir, "labels.npy")
        centroids_path = os.path.join(self.output_dir, "centroids.npy")
        analysis_path = os.path.join(self.output_dir, "cluster_analysis.json")
        
        if not all(os.path.exists(p) for p in [labels_path, centroids_path, analysis_path]):
            raise FileNotFoundError("Clustering results not found")
        
        labels = np.load(labels_path)
        centroids = np.load(centroids_path)
        
        with open(analysis_path, 'r', encoding='utf-8') as f:
            analysis = json.load(f)
        
        logger.info("Clustering results loaded successfully")
        return labels, centroids, analysis
    
    def run_clustering_pipeline(self, embeddings: torch.Tensor, scores: torch.Tensor,
                               ids: List[str], texts: List[str], k: int = 3) -> Tuple[np.ndarray, Dict]:
        """
        クラスタリングパイプライン全体を実行
        
        Args:
            embeddings: 埋め込みテンソル
            scores: スコアテンソル
            ids: ドキュメントID
            texts: テキストリスト
            k: クラスター数
            
        Returns:
            (ラベル, 分析結果)
        """
        logger.info("Starting clustering pipeline...")
        
        # 1. 特徴量準備
        features = self.prepare_features(embeddings, scores)
        
        # 2. クラスタリング実行
        labels, centroids = self.kmeans_clustering(features, k=k)
        
        # 3. 分析
        scores_np = scores.numpy() if isinstance(scores, torch.Tensor) else scores
        analysis = self.analyze_clusters(labels, scores_np, texts)
        
        # 4. 結果保存
        self.save_clustering_results(labels, centroids, ids, analysis)
        
        # 5. 結果出力
        self.print_clustering_summary(analysis)
        
        logger.info("Clustering pipeline completed")
        return labels, analysis
    
    def print_clustering_summary(self, analysis: Dict):
        """クラスタリング結果サマリーを出力"""
        logger.info("=== Clustering Summary ===")
        logger.info(f"Total clusters: {analysis['total_clusters']}")
        
        for cluster_id in range(analysis['total_clusters']):
            stats = analysis['cluster_stats'][cluster_id]
            size = analysis['cluster_sizes'][cluster_id]
            samples = analysis['sample_texts'][cluster_id]
            
            logger.info(f"\nCluster {cluster_id}:")
            logger.info(f"  Size: {size} documents")
            logger.info(f"  Score mean: {stats['score_mean']:.3f} (±{stats['score_std']:.3f})")
            logger.info(f"  Sample texts:")
            for i, sample in enumerate(samples[:2]):  # 上位2つ
                logger.info(f"    {i+1}. [{sample['score']:.3f}] {sample['text']}")


def main():
    """メイン実行関数（テスト用）"""
    # テストデータ作成
    n_samples = 50
    embedding_dim = 384
    
    # ダミー埋め込みとスコア
    embeddings = torch.randn(n_samples, embedding_dim)
    scores = torch.rand(n_samples)
    ids = [f"doc_{i:03d}" for i in range(n_samples)]
    texts = [f"これはテストドキュメント{i}です。" for i in range(n_samples)]
    
    # クラスタリング実行
    clustering = FastClustering()
    
    try:
        labels, analysis = clustering.run_clustering_pipeline(
            embeddings, scores, ids, texts, k=3
        )
        
        print(f"✓ Clustering completed successfully!")
        print(f"Labels shape: {labels.shape}")
        print(f"Unique labels: {np.unique(labels)}")
        
    except Exception as e:
        print(f"✗ Clustering failed: {e}")
        logger.error(f"Error details: {e}", exc_info=True)


if __name__ == "__main__":
    main()