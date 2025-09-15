"""
LMDBベースキャッシュマネージャー
埋め込みの高速保存・読み込み機能
"""

import os
import pickle
import lmdb
import torch
import numpy as np
from typing import List, Dict, Optional, Tuple, Union
import logging

# ログ設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CacheManager:
    """LMDBベースの埋め込みキャッシュマネージャー"""
    
    def __init__(self, cache_dir: str = "cache", map_size: int = 1e9):
        """
        Args:
            cache_dir: キャッシュディレクトリ
            map_size: LMDBマップサイズ（バイト）
        """
        self.cache_dir = cache_dir
        self.map_size = int(map_size)  # 1GB
        self.embeddings_db_path = os.path.join(cache_dir, "embeddings.lmdb")
        self.scores_db_path = os.path.join(cache_dir, "scores.lmdb")
        self.metadata_db_path = os.path.join(cache_dir, "metadata.lmdb")
        
        # ディレクトリ作成
        os.makedirs(cache_dir, exist_ok=True)
        
        logger.info(f"CacheManager initialized with cache_dir: {cache_dir}")
    
    def _serialize_tensor(self, tensor: torch.Tensor) -> bytes:
        """テンソルをバイト列にシリアライズ"""
        return pickle.dumps(tensor.numpy())
    
    def _deserialize_tensor(self, data: bytes) -> torch.Tensor:
        """バイト列からテンソルをデシリアライズ"""
        return torch.from_numpy(pickle.loads(data))
    
    def _serialize_metadata(self, metadata: Dict) -> bytes:
        """メタデータをバイト列にシリアライズ"""
        return pickle.dumps(metadata)
    
    def _deserialize_metadata(self, data: bytes) -> Dict:
        """バイト列からメタデータをデシリアライズ"""
        return pickle.loads(data)
    
    def store_embeddings(self, embeddings: torch.Tensor, ids: List[str],
                        batch_size: int = 100) -> bool:
        """
        埋め込みをLMDBに保存
        
        Args:
            embeddings: 埋め込みテンソル
            ids: ドキュメントIDリスト
            batch_size: バッチサイズ
            
        Returns:
            成功可否
        """
        try:
            # 埋め込みデータベース
            env = lmdb.open(self.embeddings_db_path, map_size=self.map_size)
            
            with env.begin(write=True) as txn:
                for i, (embedding, doc_id) in enumerate(zip(embeddings, ids)):
                    key = doc_id.encode('utf-8')
                    value = self._serialize_tensor(embedding)
                    txn.put(key, value)
                    
                    if (i + 1) % batch_size == 0:
                        logger.info(f"Stored {i + 1}/{len(embeddings)} embeddings")
            
            env.close()
            
            # メタデータ保存
            metadata = {
                'total_count': len(embeddings),
                'embedding_dim': embeddings.shape[1],
                'ids': ids
            }
            self._store_metadata('embeddings', metadata)
            
            logger.info(f"Successfully stored {len(embeddings)} embeddings")
            return True
            
        except Exception as e:
            logger.error(f"Failed to store embeddings: {e}")
            return False
    
    def store_scores(self, scores: torch.Tensor, ids: List[str],
                    batch_size: int = 100) -> bool:
        """
        スコアをLMDBに保存
        
        Args:
            scores: スコアテンソル
            ids: ドキュメントIDリスト  
            batch_size: バッチサイズ
            
        Returns:
            成功可否
        """
        try:
            # スコアデータベース
            env = lmdb.open(self.scores_db_path, map_size=self.map_size)
            
            with env.begin(write=True) as txn:
                for i, (score, doc_id) in enumerate(zip(scores, ids)):
                    key = doc_id.encode('utf-8')
                    value = self._serialize_tensor(score.unsqueeze(0))  # 1次元に
                    txn.put(key, value)
                    
                    if (i + 1) % batch_size == 0:
                        logger.info(f"Stored {i + 1}/{len(scores)} scores")
            
            env.close()
            
            # メタデータ保存
            metadata = {
                'total_count': len(scores),
                'ids': ids,
                'score_stats': {
                    'min': float(scores.min()),
                    'max': float(scores.max()),
                    'mean': float(scores.mean()),
                    'std': float(scores.std())
                }
            }
            self._store_metadata('scores', metadata)
            
            logger.info(f"Successfully stored {len(scores)} scores")
            return True
            
        except Exception as e:
            logger.error(f"Failed to store scores: {e}")
            return False
    
    def _store_metadata(self, key: str, metadata: Dict) -> bool:
        """メタデータを保存"""
        try:
            env = lmdb.open(self.metadata_db_path, map_size=self.map_size // 10)
            
            with env.begin(write=True) as txn:
                key_bytes = key.encode('utf-8')
                value_bytes = self._serialize_metadata(metadata)
                txn.put(key_bytes, value_bytes)
            
            env.close()
            return True
            
        except Exception as e:
            logger.error(f"Failed to store metadata: {e}")
            return False
    
    def load_embedding(self, doc_id: str) -> Optional[torch.Tensor]:
        """特定のドキュメントの埋め込みを取得"""
        try:
            env = lmdb.open(self.embeddings_db_path, readonly=True)
            
            with env.begin() as txn:
                key = doc_id.encode('utf-8')
                value = txn.get(key)
                
                if value is None:
                    env.close()
                    return None
                
                embedding = self._deserialize_tensor(value)
            
            env.close()
            return embedding
            
        except Exception as e:
            logger.error(f"Failed to load embedding for {doc_id}: {e}")
            return None
    
    def load_score(self, doc_id: str) -> Optional[torch.Tensor]:
        """特定のドキュメントのスコアを取得"""
        try:
            env = lmdb.open(self.scores_db_path, readonly=True)
            
            with env.begin() as txn:
                key = doc_id.encode('utf-8')
                value = txn.get(key)
                
                if value is None:
                    env.close()
                    return None
                
                score = self._deserialize_tensor(value)
            
            env.close()
            return score.squeeze()  # 1次元から0次元へ
            
        except Exception as e:
            logger.error(f"Failed to load score for {doc_id}: {e}")
            return None
    
    def load_all_embeddings(self) -> Tuple[torch.Tensor, List[str]]:
        """全埋め込みを読み込み"""
        try:
            metadata = self._load_metadata('embeddings')
            if metadata is None:
                raise ValueError("Embeddings metadata not found")
            
            ids = metadata['ids']
            embedding_dim = metadata['embedding_dim']
            
            env = lmdb.open(self.embeddings_db_path, readonly=True)
            embeddings = torch.zeros(len(ids), embedding_dim)
            
            with env.begin() as txn:
                for i, doc_id in enumerate(ids):
                    key = doc_id.encode('utf-8')
                    value = txn.get(key)
                    
                    if value is not None:
                        embeddings[i] = self._deserialize_tensor(value)
            
            env.close()
            logger.info(f"Loaded {len(ids)} embeddings from cache")
            return embeddings, ids
            
        except Exception as e:
            logger.error(f"Failed to load all embeddings: {e}")
            raise
    
    def load_all_scores(self) -> Tuple[torch.Tensor, List[str]]:
        """全スコアを読み込み"""
        try:
            metadata = self._load_metadata('scores')
            if metadata is None:
                raise ValueError("Scores metadata not found")
            
            ids = metadata['ids']
            
            env = lmdb.open(self.scores_db_path, readonly=True)
            scores = torch.zeros(len(ids))
            
            with env.begin() as txn:
                for i, doc_id in enumerate(ids):
                    key = doc_id.encode('utf-8')
                    value = txn.get(key)
                    
                    if value is not None:
                        scores[i] = self._deserialize_tensor(value).squeeze()
            
            env.close()
            logger.info(f"Loaded {len(ids)} scores from cache")
            return scores, ids
            
        except Exception as e:
            logger.error(f"Failed to load all scores: {e}")
            raise
    
    def _load_metadata(self, key: str) -> Optional[Dict]:
        """メタデータを読み込み"""
        try:
            env = lmdb.open(self.metadata_db_path, readonly=True)
            
            with env.begin() as txn:
                key_bytes = key.encode('utf-8')
                value = txn.get(key_bytes)
                
                if value is None:
                    env.close()
                    return None
                
                metadata = self._deserialize_metadata(value)
            
            env.close()
            return metadata
            
        except Exception as e:
            logger.error(f"Failed to load metadata for {key}: {e}")
            return None
    
    def cache_exists(self, cache_type: str) -> bool:
        """キャッシュが存在するかチェック"""
        if cache_type == 'embeddings':
            return os.path.exists(self.embeddings_db_path)
        elif cache_type == 'scores':
            return os.path.exists(self.scores_db_path)
        else:
            return False
    
    def get_cache_info(self) -> Dict:
        """キャッシュ情報を取得"""
        info = {
            'embeddings_exists': self.cache_exists('embeddings'),
            'scores_exists': self.cache_exists('scores'),
            'cache_dir': self.cache_dir
        }
        
        # メタデータがあれば追加
        embeddings_meta = self._load_metadata('embeddings')
        scores_meta = self._load_metadata('scores')
        
        if embeddings_meta:
            info['embeddings_count'] = embeddings_meta['total_count']
            info['embedding_dim'] = embeddings_meta['embedding_dim']
        
        if scores_meta:
            info['scores_count'] = scores_meta['total_count']
            info['score_stats'] = scores_meta['score_stats']
        
        return info
    
    def clear_cache(self, cache_type: Optional[str] = None) -> bool:
        """キャッシュをクリア"""
        try:
            if cache_type is None or cache_type == 'embeddings':
                if os.path.exists(self.embeddings_db_path):
                    os.remove(self.embeddings_db_path)
                    logger.info("Embeddings cache cleared")
            
            if cache_type is None or cache_type == 'scores':
                if os.path.exists(self.scores_db_path):
                    os.remove(self.scores_db_path)
                    logger.info("Scores cache cleared")
            
            if cache_type is None:
                if os.path.exists(self.metadata_db_path):
                    os.remove(self.metadata_db_path)
                    logger.info("Metadata cache cleared")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to clear cache: {e}")
            return False


def main():
    """メイン実行関数（テスト用）"""
    # キャッシュマネージャー初期化
    cache_manager = CacheManager()
    
    # テストデータ作成
    test_embeddings = torch.randn(10, 384)  # 10個の384次元埋め込み
    test_scores = torch.randn(10)
    test_ids = [f"doc_{i:03d}" for i in range(10)]
    
    print("Testing CacheManager...")
    
    # 保存テスト
    print("1. Storing embeddings and scores...")
    success_emb = cache_manager.store_embeddings(test_embeddings, test_ids)
    success_scores = cache_manager.store_scores(test_scores, test_ids)
    
    if success_emb and success_scores:
        print("✓ Storage successful")
    else:
        print("✗ Storage failed")
        return
    
    # 読み込みテスト
    print("2. Loading individual embedding and score...")
    embedding = cache_manager.load_embedding("doc_005")
    score = cache_manager.load_score("doc_005")
    
    if embedding is not None and score is not None:
        print(f"✓ Loaded embedding shape: {embedding.shape}, score: {score.item():.3f}")
    else:
        print("✗ Individual loading failed")
    
    # 全データ読み込みテスト
    print("3. Loading all embeddings and scores...")
    try:
        all_embeddings, all_ids = cache_manager.load_all_embeddings()
        all_scores, _ = cache_manager.load_all_scores()
        print(f"✓ Loaded all data - embeddings: {all_embeddings.shape}, scores: {all_scores.shape}")
    except Exception as e:
        print(f"✗ Bulk loading failed: {e}")
    
    # キャッシュ情報テスト
    print("4. Cache information:")
    info = cache_manager.get_cache_info()
    for key, value in info.items():
        print(f"  {key}: {value}")
    
    print("CacheManager test completed!")


if __name__ == "__main__":
    main()