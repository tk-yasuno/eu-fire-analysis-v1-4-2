"""
モデルローダーと量子化モジュール
all-MiniLM-L6-v2モデルの8-bit量子化ロード機能
"""

import os
import torch
from typing import Optional, Tuple
from transformers import AutoModel, AutoTokenizer, BitsAndBytesConfig
from sentence_transformers import SentenceTransformer
import logging

# ログ設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ModelLoader:
    """量子化されたモデルのローダークラス"""
    
    def __init__(self, model_name: str = "sentence-transformers/all-MiniLM-L6-v2", 
                 device: str = "auto", use_quantization: bool = True):
        """
        Args:
            model_name: 使用するモデル名
            device: デバイス指定 ('auto', 'cpu', 'cuda:0' など)
            use_quantization: 8-bit量子化を使用するか
        """
        self.model_name = model_name
        self.device = device
        self.use_quantization = use_quantization
        self.model = None
        self.tokenizer = None
        
    def setup_quantization_config(self) -> Optional[BitsAndBytesConfig]:
        """8-bit量子化設定を作成"""
        if not self.use_quantization:
            return None
            
        try:
            config = BitsAndBytesConfig(
                load_in_8bit=True,
                llm_int8_enable_fp32_cpu_offload=True,
                llm_int8_has_fp16_weight=False,
                llm_int8_threshold=6.0
            )
            logger.info("8-bit quantization config created")
            return config
        except Exception as e:
            logger.warning(f"Failed to create quantization config: {e}")
            logger.info("Falling back to standard loading")
            return None
    
    def check_gpu_memory(self) -> Tuple[bool, float]:
        """GPU メモリをチェック"""
        if not torch.cuda.is_available():
            logger.info("CUDA not available, using CPU")
            return False, 0.0
        
        try:
            gpu_memory = torch.cuda.get_device_properties(0).total_memory / 1024**3  # GB
            logger.info(f"GPU memory available: {gpu_memory:.2f} GB")
            return True, gpu_memory
        except Exception as e:
            logger.warning(f"Failed to check GPU memory: {e}")
            return False, 0.0
    
    def load_sentence_transformer(self) -> SentenceTransformer:
        """SentenceTransformerモデルを直接ロード（推奨）"""
        try:
            # GPU使用可能性チェック
            cuda_available, gpu_memory = self.check_gpu_memory()
            
            if cuda_available and gpu_memory >= 8.0:
                device = 'cuda'
            else:
                device = 'cpu'
                logger.info("Using CPU due to insufficient GPU memory or availability")
            
            # SentenceTransformerで直接ロード
            model = SentenceTransformer(self.model_name, device=device)
            
            # メモリ使用量をチェック
            if device == 'cuda':
                memory_allocated = torch.cuda.memory_allocated() / 1024**3
                logger.info(f"GPU memory allocated: {memory_allocated:.2f} GB")
                
                if memory_allocated > 8.0:
                    logger.warning("Memory usage exceeds 8GB, consider using CPU")
            
            logger.info(f"Model loaded successfully on {device}")
            return model
            
        except Exception as e:
            logger.error(f"Failed to load SentenceTransformer: {e}")
            logger.info("Falling back to manual loading...")
            return self.load_manual_model()
    
    def load_manual_model(self) -> SentenceTransformer:
        """手動でトークナイザーとモデルをロードして結合"""
        try:
            # 量子化設定
            quantization_config = self.setup_quantization_config()
            
            # トークナイザーをロード
            self.tokenizer = AutoTokenizer.from_pretrained(self.model_name)
            logger.info("Tokenizer loaded successfully")
            
            # モデルをロード
            if quantization_config is not None:
                self.model = AutoModel.from_pretrained(
                    self.model_name,
                    quantization_config=quantization_config,
                    device_map=self.device,
                    trust_remote_code=True
                )
                logger.info("Model loaded with 8-bit quantization")
            else:
                # 標準ロード
                device = 'cuda' if torch.cuda.is_available() and self.device == 'auto' else 'cpu'
                self.model = AutoModel.from_pretrained(self.model_name)
                self.model = self.model.to(device)
                logger.info(f"Model loaded without quantization on {device}")
            
            # 評価モードに設定
            self.model.eval()
            
            # SentenceTransformerオブジェクトを作成
            sentence_transformer = SentenceTransformer(modules=[self.model])
            
            return sentence_transformer
            
        except Exception as e:
            logger.error(f"Failed to load manual model: {e}")
            raise
    
    def load_model(self) -> SentenceTransformer:
        """
        モデルをロードするメイン関数
        
        Returns:
            ロードされたSentenceTransformerモデル
        """
        logger.info(f"Loading model: {self.model_name}")
        
        # まずSentenceTransformerで直接ロードを試行
        try:
            model = self.load_sentence_transformer()
            logger.info("Model loading completed successfully")
            return model
        except Exception as e:
            logger.error(f"All loading methods failed: {e}")
            raise
    
    def validate_model(self, model: SentenceTransformer) -> bool:
        """モデルの動作確認"""
        try:
            # テストエンコーディング
            test_texts = ["これはテストです", "災害情報をお知らせします"]
            embeddings = model.encode(test_texts)
            
            logger.info(f"Validation successful. Embedding shape: {embeddings.shape}")
            return True
            
        except Exception as e:
            logger.error(f"Model validation failed: {e}")
            return False
    
    def get_memory_usage(self) -> str:
        """現在のメモリ使用量を取得"""
        if torch.cuda.is_available():
            allocated = torch.cuda.memory_allocated() / 1024**3
            cached = torch.cuda.memory_reserved() / 1024**3
            return f"GPU Memory - Allocated: {allocated:.2f}GB, Cached: {cached:.2f}GB"
        else:
            return "CPU mode - GPU memory info not available"


def main():
    """メイン実行関数"""
    # モデルローダーを初期化
    loader = ModelLoader(use_quantization=True)
    
    try:
        # モデルをロード
        model = loader.load_model()
        
        # 動作確認
        if loader.validate_model(model):
            print("✓ Model loaded and validated successfully!")
            print(f"Memory usage: {loader.get_memory_usage()}")
            
            # サンプル埋め込み生成
            sample_texts = [
                "地震が発生しました。避難してください。",
                "台風の影響で電車が止まっています。",
                "火災が発生。消防車が到着しました。"
            ]
            
            embeddings = model.encode(sample_texts)
            print(f"Sample embeddings shape: {embeddings.shape}")
            print("Sample embedding norms:", [f"{emb.sum():.3f}" for emb in embeddings])
        else:
            print("✗ Model validation failed")
            
    except Exception as e:
        print(f"✗ Failed to load model: {e}")


if __name__ == "__main__":
    main()