"""
埋め込み生成・スコアリングモジュール
バッチ処理による効率的な埋め込み生成とセンチメントスコア計算
"""

import json
import os
import torch
import numpy as np
from typing import List, Dict, Tuple, Optional
from torch.utils.data import DataLoader, Dataset
from sentence_transformers import SentenceTransformer
from tqdm import tqdm
import logging

# ログ設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TextDataset(Dataset):
    """テキストデータセットクラス"""
    
    def __init__(self, texts: List[str]):
        self.texts = texts
    
    def __len__(self):
        return len(self.texts)
    
    def __getitem__(self, idx):
        return self.texts[idx]


class EmbeddingGenerator:
    """埋め込み生成とスコアリングクラス"""
    
    def __init__(self, model: SentenceTransformer, batch_size: int = 16, 
                 output_dir: str = "outputs"):
        """
        Args:
            model: 事前ロードされたSentenceTransformerモデル
            batch_size: バッチサイズ
            output_dir: 出力ディレクトリ
        """
        self.model = model
        self.batch_size = batch_size
        self.output_dir = output_dir
        self.device = next(model.parameters()).device if hasattr(model, 'parameters') else 'cpu'
        
        # 出力ディレクトリ作成
        os.makedirs(output_dir, exist_ok=True)
        
        logger.info(f"EmbeddingGenerator initialized with device: {self.device}")
    
    def load_cleaned_data(self, filepath: str) -> List[Dict]:
        """清浄化されたデータを読み込み"""
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        logger.info(f"Loaded {len(data)} documents from {filepath}")
        return data
    
    def calculate_sentiment_scores(self, embeddings: torch.Tensor) -> torch.Tensor:
        """
        埋め込みからセンチメントスコアを計算
        この例では、L2ノルムを使用（実際の実装では他の方法も検討可能）
        
        Args:
            embeddings: 埋め込みテンソル
            
        Returns:
            スコアテンソル
        """
        # L2ノルムをスコアとして使用
        scores = torch.norm(embeddings, p=2, dim=1)
        
        # 正規化（0-1の範囲）
        scores = (scores - scores.min()) / (scores.max() - scores.min())
        
        return scores
    
    def alternative_sentiment_scores(self, embeddings: torch.Tensor, texts: List[str]) -> torch.Tensor:
        """
        代替のセンチメントスコア計算方法
        
        Args:
            embeddings: 埋め込みテンソル  
            texts: 元テキストリスト
            
        Returns:
            スコアテンソル
        """
        scores = torch.zeros(len(embeddings))
        
        # 感情表現に基づく簡易スコアリング
        positive_words = ['安全', '救助', '支援', '感謝', '復旧', 'ありがとう', '無事']
        negative_words = ['危険', '被害', '停電', '火災', '避難', '心配', '災害']
        urgent_words = ['緊急', '急いで', 'すぐに', '至急', '注意', '警告']
        
        for i, text in enumerate(texts):
            score = 0.5  # ベーススコア
            
            # ポジティブ要素
            score += 0.1 * sum(1 for word in positive_words if word in text)
            
            # ネガティブ要素  
            score -= 0.1 * sum(1 for word in negative_words if word in text)
            
            # 緊急度要素
            score += 0.2 * sum(1 for word in urgent_words if word in text)
            
            # 正規化
            scores[i] = max(0.0, min(1.0, score))
        
        return scores
    
    def generate_embeddings_batch(self, texts: List[str], use_alternative_scoring: bool = False) -> Tuple[torch.Tensor, torch.Tensor]:
        """
        バッチ処理で埋め込みとスコアを生成
        
        Args:
            texts: テキストリスト
            use_alternative_scoring: 代替スコアリング方法を使用するか
            
        Returns:
            (埋め込み, スコア) のタプル
        """
        # データローダー作成
        dataset = TextDataset(texts)
        dataloader = DataLoader(dataset, batch_size=self.batch_size, shuffle=False)
        
        all_embeddings = []
        all_scores = []
        
        # バッチ処理で埋め込み生成
        with torch.no_grad():
            for batch_texts in tqdm(dataloader, desc="Generating embeddings"):
                # 埋め込み生成
                batch_embeddings = self.model.encode(
                    batch_texts, 
                    convert_to_tensor=True,
                    device=self.device,
                    show_progress_bar=False
                )
                
                # スコア計算
                if use_alternative_scoring:
                    batch_scores = self.alternative_sentiment_scores(batch_embeddings, batch_texts)
                else:
                    batch_scores = self.calculate_sentiment_scores(batch_embeddings)
                
                # CPUに移動して保存
                all_embeddings.append(batch_embeddings.cpu())
                all_scores.append(batch_scores.cpu())
        
        # 結合
        embeddings = torch.cat(all_embeddings, dim=0)
        scores = torch.cat(all_scores, dim=0)
        
        logger.info(f"Generated embeddings shape: {embeddings.shape}")
        logger.info(f"Generated scores shape: {scores.shape}")
        
        return embeddings, scores
    
    def save_embeddings_and_scores(self, embeddings: torch.Tensor, scores: torch.Tensor,
                                   ids: List[str]) -> Tuple[str, str]:
        """
        埋め込みとスコアを保存
        
        Args:
            embeddings: 埋め込みテンソル
            scores: スコアテンソル  
            ids: ドキュメントIDリスト
            
        Returns:
            (埋め込みファイルパス, スコアファイルパス)
        """
        # ファイルパス
        embeddings_path = os.path.join(self.output_dir, "embeddings.pt")
        scores_path = os.path.join(self.output_dir, "scores.pt")
        
        # 保存
        torch.save({
            'embeddings': embeddings,
            'ids': ids,
            'shape': embeddings.shape
        }, embeddings_path)
        
        torch.save({
            'scores': scores,
            'ids': ids,
            'shape': scores.shape
        }, scores_path)
        
        logger.info(f"Embeddings saved to {embeddings_path}")
        logger.info(f"Scores saved to {scores_path}")
        
        return embeddings_path, scores_path
    
    def load_embeddings_and_scores(self) -> Tuple[torch.Tensor, torch.Tensor, List[str]]:
        """保存された埋め込みとスコアを読み込み"""
        embeddings_path = os.path.join(self.output_dir, "embeddings.pt")
        scores_path = os.path.join(self.output_dir, "scores.pt")
        
        if not (os.path.exists(embeddings_path) and os.path.exists(scores_path)):
            raise FileNotFoundError("Embeddings or scores file not found")
        
        # 読み込み
        embedding_data = torch.load(embeddings_path)
        score_data = torch.load(scores_path)
        
        embeddings = embedding_data['embeddings']
        scores = score_data['scores']
        ids = embedding_data['ids']
        
        logger.info(f"Loaded embeddings shape: {embeddings.shape}")
        logger.info(f"Loaded scores shape: {scores.shape}")
        
        return embeddings, scores, ids
    
    def process_data(self, data_filepath: str, use_alternative_scoring: bool = False) -> Tuple[str, str]:
        """
        データファイルを処理して埋め込みとスコアを生成
        
        Args:
            data_filepath: 清浄化されたデータファイルのパス
            use_alternative_scoring: 代替スコアリング方法を使用するか
            
        Returns:
            (埋め込みファイルパス, スコアファイルパス)
        """
        logger.info("Starting embedding generation process...")
        
        # データ読み込み
        data = self.load_cleaned_data(data_filepath)
        texts = [item['text'] for item in data]
        ids = [item['id'] for item in data]
        
        # 埋め込み生成
        embeddings, scores = self.generate_embeddings_batch(texts, use_alternative_scoring)
        
        # 保存
        embedding_path, score_path = self.save_embeddings_and_scores(embeddings, scores, ids)
        
        # 統計情報を出力
        self.print_statistics(embeddings, scores, texts)
        
        return embedding_path, score_path
    
    def print_statistics(self, embeddings: torch.Tensor, scores: torch.Tensor, texts: List[str]):
        """統計情報を出力"""
        logger.info("=== Generation Statistics ===")
        logger.info(f"Total documents: {len(texts)}")
        logger.info(f"Embedding dimension: {embeddings.shape[1]}")
        logger.info(f"Score range: {scores.min():.3f} - {scores.max():.3f}")
        logger.info(f"Score mean: {scores.mean():.3f}")
        logger.info(f"Score std: {scores.std():.3f}")
        
        # 上位・下位スコアのサンプル
        sorted_indices = torch.argsort(scores, descending=True)
        logger.info("Top scored texts:")
        for i in range(min(3, len(texts))):
            idx = sorted_indices[i].item()
            logger.info(f"  {scores[idx]:.3f}: {texts[idx][:100]}...")
        
        logger.info("Lowest scored texts:")
        for i in range(min(3, len(texts))):
            idx = sorted_indices[-(i+1)].item()
            logger.info(f"  {scores[idx]:.3f}: {texts[idx][:100]}...")


def main():
    """メイン実行関数（テスト用）"""
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(__file__)))
    
    from scripts.model_loader import ModelLoader
    from scripts.data_collector import DataCollector
    
    try:
        # データ準備
        collector = DataCollector()
        data_filepath = collector.run_collection_pipeline()
        
        # モデルロード
        model_loader = ModelLoader()
        model = model_loader.load_model()
        
        # 埋め込み生成器初期化
        generator = EmbeddingGenerator(model, batch_size=8)
        
        # 埋め込み生成
        embedding_path, score_path = generator.process_data(data_filepath, use_alternative_scoring=True)
        
        print(f"✓ Embeddings saved to: {embedding_path}")
        print(f"✓ Scores saved to: {score_path}")
        
        # メモリ使用量確認
        if torch.cuda.is_available():
            memory_allocated = torch.cuda.memory_allocated() / 1024**3
            print(f"GPU memory allocated: {memory_allocated:.2f} GB")
        
    except Exception as e:
        print(f"✗ Error in embedding generation: {e}")
        logger.error(f"Error details: {e}", exc_info=True)


if __name__ == "__main__":
    main()