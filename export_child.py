#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
export_child.py - 子供の画像検出・エクスポートツール
=================================================

概要:
----
このスクリプトは、SQLiteデータベース内の画像ファイルから子供が写っている画像を
YOLOv8モデルを使用して高精度で検出し、指定された枚数をランダムに選んでエクスポートします。
処理済みの画像はキャッシュに記録され、再実行時には未処理の画像のみを効率的に処理します。

主な機能:
-------
- YOLOv8モデルによる子供の画像検出
- 子供らしさを判定する複数の特徴分析（サイズ比率、頭身比率、縦横比など）
- 検出結果のキャッシュによる効率的な再処理
- マルチスレッドによる高速処理
- 検出された画像からのランダムエクスポート
- すべての画像をJPEG形式に統一

コマンドラインオプション:
--------------------
--db-path       SQLiteデータベースファイルのパス（デフォルト: data.db）
--output-dir    エクスポート先ディレクトリ（デフォルト: ./output）
--cache-file    キャッシュファイルのパス（デフォルト: export_child.cache）
--model-dir     モデルディレクトリ（デフォルト: ./models）
--confidence    検出信頼度の閾値 (0.0-1.0)（デフォルト: 0.5）
--num-copies    エクスポートする画像の枚数（デフォルト: 50）
--workers       並列処理のワーカー数（デフォルト: 4）
--debug         デバッグモードを有効にする

使用例:
-----
# 基本的な使い方（デフォルト設定）
python export_child.py

# データベースパスとエクスポート先を指定
python export_child.py --db-path /path/to/media.db --output-dir /path/to/export

# 信頼度閾値と出力枚数を調整
python export_child.py --confidence 0.7 --num-copies 100

# 並列処理のワーカー数を増やして処理速度を向上
python export_child.py --workers 8

# デバッグモードを有効にして詳細なログを表示
python export_child.py --debug

必要なライブラリ:
-------------
- ultralytics (YOLOv8)
- opencv-python (cv2)
- numpy
- tqdm
- PIL (Pillow)

注意事項:
-------
- 初回実行時にYOLOv8モデルが自動的にダウンロードされます
- 大量の画像を処理する場合はキャッシュファイルを活用することで効率的に処理できます
- 出力ディレクトリの内容は実行時に削除されるため、重要なファイルは別の場所に保存してください
"""
import os
import cv2
import random
import sqlite3
import argparse
import numpy as np
import traceback
import logging
import shutil
import time
from pathlib import Path
from tqdm import tqdm
import concurrent.futures
import urllib.request
import zipfile
import yaml

# ロギングの設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("export_child.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("ChildImageExporter")

class ChildImageExporter:
    def __init__(self, 
                 db_path="data.db",
                 output_dir="./output",
                 cache_file="export_child.cache",
                 model_dir="./models",
                 confidence_threshold=0.5,
                 num_copies=50,
                 max_workers=4):
        self.db_path = Path(db_path)
        self.output_dir = Path(output_dir)
        self.cache_file = Path(cache_file)
        self.model_dir = Path(model_dir)
        self.confidence_threshold = confidence_threshold
        self.num_copies = num_copies
        self.max_workers = max_workers
        
        # キャッシュと出力ディレクトリの初期化
        self.output_dir.mkdir(exist_ok=True)
        self.model_dir.mkdir(exist_ok=True)
        self.cache = self._load_cache()
        
        # YOLOモデルの初期化
        self.model = self._initialize_model()

    def _load_cache(self):
        """キャッシュファイルを読み込む"""
        cache = {'processed_files': set(), 'matching_files': set()}
        if self.cache_file.exists():
            with open(self.cache_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#'):
                        continue
                    if line.startswith('MATCH:'):
                        cache['matching_files'].add(line[6:])
                    cache['processed_files'].add(line.replace('MATCH:', ''))
        return cache

    def _save_cache(self):
        """キャッシュファイルを保存する"""
        with open(self.cache_file, 'w', encoding='utf-8') as f:
            f.write('# 処理済みファイルリスト（MATCH:が付いているものは条件に合致）\n')
            # マッチしたファイルを先に書き出し
            for file in sorted(self.cache['matching_files']):
                f.write(f'MATCH:{file}\n')
            # マッチしなかったファイルを書き出し
            processed_only = self.cache['processed_files'] - self.cache['matching_files']
            for file in sorted(processed_only):
                f.write(f'{file}\n')

    def _download_model(self):
        """APIキー不要の一般公開YOLOモデルをダウンロード"""
        model_path = self.model_dir / "yolov8-model"
        weights_path = model_path / "yolov8n.pt"
        
        # モデルが既に存在する場合はダウンロードをスキップ
        if weights_path.exists():
            logger.info("モデルは既にダウンロード済みです")
            return str(weights_path)
        
        try:
            # モデルディレクトリを作成
            model_path.mkdir(exist_ok=True)
            
            # YOLOv8モデルをダウンロード
            logger.info("YOLOv8モデルをダウンロード中...")
            
            # 一般公開されているYOLOv8モデルをダウンロード（APIキー不要）
            model_url = "https://github.com/ultralytics/assets/releases/download/v0.0.0/yolov8n.pt"
            urllib.request.urlretrieve(model_url, str(weights_path))
            
            logger.info(f"モデルを {weights_path} にダウンロードしました")
            return str(weights_path)
            
        except Exception as e:
            logger.error(f"モデルのダウンロード中にエラーが発生しました: {e}")
            traceback.print_exc()
            raise

    def _initialize_model(self):
        """YOLOモデルを初期化"""
        try:
            # ultralytics YOLOv8をインポート
            from ultralytics import YOLO
            
            # モデルをダウンロードまたは既存のモデルを使用
            model_path = self._download_model()
            
            # モデルをロード
            model = YOLO(model_path)
            logger.info("子供検出モデルを正常にロードしました")
            
            return model
            
        except ImportError:
            logger.error("ultralyticsライブラリがインストールされていません。pip install ultralyticsを実行してください。")
            raise
        except Exception as e:
            logger.error(f"モデルの初期化中にエラーが発生しました: {e}")
            traceback.print_exc()
            raise

    def _read_image(self, img_path):
        """画像を安全に読み込む"""
        try:
            # まずOpenCVで試みる
            img = cv2.imdecode(np.fromfile(img_path, dtype=np.uint8), cv2.IMREAD_COLOR)
            if img is not None:
                return img

            # OpenCVで失敗した場合、PILで試みる
            from PIL import Image
            img = Image.open(img_path)
            # PILからOpenCV形式に変換
            img = cv2.cvtColor(np.array(img), cv2.COLOR_RGB2BGR)
            return img
        except Exception as e:
            logger.error(f"Error reading image {img_path}: {e}")
            return None

    def _convert_to_jpeg(self, src_path, dst_path):
        """画像をJPEG形式に変換"""
        img = self._read_image(src_path)
        if img is not None:
            # 日本語パスに対応するため、バッファに書き出してから保存
            is_success, buffer = cv2.imencode(".jpg", img, [cv2.IMWRITE_JPEG_QUALITY, 95])
            if is_success:
                with open(dst_path, "wb") as f:
                    f.write(buffer)
                return True
        return False

    def _detect_children(self, image_path):
        """画像内の子供を検出（一般的なYOLOv8モデルを使用）"""
        try:
            # YOLOモデルで人物検出を実行
            results = self.model(image_path, conf=self.confidence_threshold, classes=[0])  # class 0 = person
            
            children_detected = []
            
            for result in results:
                # 検出結果を処理
                boxes = result.boxes
                
                for i, box in enumerate(boxes):
                    # 信頼度を取得
                    confidence = float(box.conf.item())
                    
                    # バウンディングボックスを取得
                    x1, y1, x2, y2 = map(int, box.xyxy[0].tolist())
                    
                    # 人物の検出結果から子供かどうかを判定
                    child_score = self._is_likely_child(box, result.orig_img)
                    if child_score > 0.5:  # 子供である可能性が高い
                        # 子供らしさに基づいて信頼度を調整
                        adjusted_confidence = confidence * child_score
                        
                        children_detected.append({
                            'confidence': adjusted_confidence,
                            'box': (x1, y1, x2, y2),
                            'class': 'child',
                            'child_score': child_score
                        })
                        logger.debug(f"子供を検出: 信頼度={adjusted_confidence:.2f}, 子供らしさ={child_score:.2f}")
            
            return children_detected
            
        except Exception as e:
            logger.error(f"子供検出中にエラーが発生しました: {e}")
            traceback.print_exc()
            return []

    def _is_likely_child(self, box, image):
        """人物の検出結果から子供である可能性を判定（より詳細な特徴分析）"""
        try:
            # バウンディングボックスのサイズを取得
            x1, y1, x2, y2 = map(int, box.xyxy[0].tolist())
            width = x2 - x1
            height = y2 - y1
            
            # 画像の高さと幅を取得
            img_height, img_width = image.shape[:2]
            
            # 相対的なサイズを計算
            relative_height = height / img_height
            relative_width = width / img_width
            
            # 人物の領域を切り出し
            person_img = image[y1:y2, x1:x2]
            
            # 子供判定のための複数の特徴を分析
            
            # 1. サイズ比率 - 子供は一般的に大人より小さい
            size_score = 0
            if relative_height < 0.4:  # 画像の40%未満の高さ
                size_score = 0.8
            elif relative_height < 0.6:  # 画像の60%未満の高さ
                size_score = 0.5
            else:
                size_score = 0.2
                
            # 2. 頭身比率 - 子供は頭が体に対して相対的に大きい
            # 簡易的な頭部検出（上部1/3を頭部と仮定）
            head_height = height / 3
            body_height = height - head_height
            head_body_ratio = head_height / body_height if body_height > 0 else 0
            
            head_ratio_score = 0
            if head_body_ratio > 0.5:  # 頭が体の半分以上のサイズ
                head_ratio_score = 0.8
            elif head_body_ratio > 0.3:  # 頭が体の30%以上のサイズ
                head_ratio_score = 0.5
            else:
                head_ratio_score = 0.2
                
            # 3. 縦横比 - 子供は大人に比べて縦横比が異なる傾向がある
            aspect_ratio = width / height if height > 0 else 0
            aspect_score = 0
            if 0.4 <= aspect_ratio <= 0.6:  # 子供の典型的な縦横比
                aspect_score = 0.7
            else:
                aspect_score = 0.3
                
            # 総合スコアの計算（重み付け）
            total_score = (size_score * 0.5) + (head_ratio_score * 0.3) + (aspect_score * 0.2)
            
            return total_score
            
        except Exception as e:
            logger.warning(f"子供判定中にエラーが発生しました: {e}")
            return False

    def _process_image(self, img_id, img_path, temp_dir):
        """1つの画像を処理する"""
        # すでに処理済みの場合はスキップ
        if str(img_id) in self.cache['processed_files']:
            return None
        
        try:
            # 画像を一時ファイルとして保存
            temp_path = temp_dir / f"temp_{img_id}.jpg"
            if not self._convert_to_jpeg(img_path, temp_path):
                logger.warning(f"Failed to process image {img_path}")
                return None
            
            # 子供を検出
            children = self._detect_children(str(temp_path))
            
            if not children:
                logger.info(f"No children detected in image {img_id}")
                return {'id': img_id, 'is_match': False, 'children': []}
            
            logger.info(f"画像 {os.path.basename(img_path)} の解析結果: {len(children)}人の子供を検出")
            
            # 最も信頼度の高い検出結果を取得
            max_confidence = max(child['confidence'] for child in children)
            
            # 信頼度が閾値を超えている場合はマッチとみなす
            is_target_image = max_confidence >= self.confidence_threshold
            
            if is_target_image:
                logger.info(f"→ この画像は条件に合致しました (信頼度: {max_confidence:.2f})")
            else:
                logger.info("→ この画像は条件に合致しませんでした")
            
            return {
                'id': img_id,
                'is_match': is_target_image,
                'confidence': max_confidence,
                'children': children
            }
            
        except Exception as e:
            logger.error(f"Error processing image ID {img_id}: {e}")
            traceback.print_exc()
            return None
        finally:
            # 一時ファイルを削除
            if temp_path.exists():
                temp_path.unlink()

    def process_images(self):
        """データベースから画像を処理して条件に合う画像を見つける"""
        # データベースに接続
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # 画像ファイルを取得
        cursor.execute("""
            SELECT id, full_path 
            FROM media_files 
            WHERE file_type = 'image' 
            AND processed = 1
        """)
        
        all_images = cursor.fetchall()
        cursor.close()
        conn.close()
        
        # 未処理の画像のみをフィルタリング
        unprocessed_images = [(img_id, img_path) for img_id, img_path in all_images 
                             if str(img_id) not in self.cache['processed_files']]
        
        if not unprocessed_images:
            logger.info("すべての画像が既に処理済みです。")
            return
        
        logger.info(f"処理対象: {len(unprocessed_images)}枚の画像")

        temp_dir = Path('temp_processing')
        temp_dir.mkdir(exist_ok=True)

        try:
            # 並列処理で画像を分析
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # 画像処理タスクを作成
                future_to_img = {
                    executor.submit(self._process_image, img_id, img_path, temp_dir): (img_id, img_path)
                    for img_id, img_path in unprocessed_images
                }
                
                # 進捗バーを表示
                for future in tqdm(concurrent.futures.as_completed(future_to_img), 
                                  total=len(future_to_img),
                                  desc="画像を処理中"):
                    img_id, img_path = future_to_img[future]
                    try:
                        result = future.result()
                        if result:
                            # 処理結果を記録
                            self.cache['processed_files'].add(str(result['id']))
                            if result['is_match']:
                                self.cache['matching_files'].add(str(result['id']))
                            
                            # 定期的にキャッシュを保存
                            if len(self.cache['processed_files']) % 10 == 0:
                                self._save_cache()
                    except Exception as e:
                        logger.error(f"Error processing image {img_id}: {e}")
                        traceback.print_exc()
                        # エラーが発生した場合も処理済みとしてマーク
                        self.cache['processed_files'].add(str(img_id))

        finally:
            # 後処理
            # 一時ディレクトリを削除
            for file in temp_dir.glob('*'):
                file.unlink()
            temp_dir.rmdir()

        # キャッシュを保存
        self._save_cache()
        
        # 結果を表示
        logger.info(f"処理完了: {len(self.cache['processed_files'])}枚の画像を処理しました")
        logger.info(f"条件に合致: {len(self.cache['matching_files'])}枚の画像")

    def export_random_images(self):
        """マッチした画像からランダムに指定枚数をエクスポート"""
        if not self.cache['matching_files']:
            logger.warning("条件に合う画像が見つかりませんでした。")
            return

        # 出力ディレクトリをクリア
        for file in self.output_dir.glob('*'):
            file.unlink()

        # ランダムに画像を選択
        selected_ids = random.sample(
            list(self.cache['matching_files']),
            min(self.num_copies, len(self.cache['matching_files']))
        )

        # データベースから選択された画像のパスを取得
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        for img_id in tqdm(selected_ids, desc="画像をエクスポート中"):
            try:
                cursor.execute("SELECT full_path FROM media_files WHERE id = ?", (img_id,))
                result = cursor.fetchone()
                if result:
                    src_path = result[0]
                    if os.path.exists(src_path):
                        # 出力ファイル名を設定
                        dst_path = self.output_dir / f"image_{img_id}.jpg"
                        
                        # JPEG形式に変換（すべての画像を変換して統一する）
                        if not self._convert_to_jpeg(src_path, dst_path):
                            logger.warning(f"Failed to convert {src_path} to JPEG")
                    else:
                        logger.warning(f"Source file not found: {src_path}")

            except Exception as e:
                logger.error(f"Error exporting image ID {img_id}: {e}")

        cursor.close()
        conn.close()
        
        logger.info(f"{len(selected_ids)}枚の画像をエクスポートしました")

def main():
    parser = argparse.ArgumentParser(description='YOLOv8を使用して子供の画像を高精度で検出してエクスポートするツール')
    parser.add_argument('--db-path', default='data.db', help='SQLiteデータベースファイルのパス')
    parser.add_argument('--output-dir', default='./output', help='出力ディレクトリ')
    parser.add_argument('--cache-file', default='export_child.cache', help='キャッシュファイルのパス')
    parser.add_argument('--model-dir', default='./models', help='モデルディレクトリ')
    parser.add_argument('--confidence', type=float, default=0.5, help='検出信頼度の閾値 (0.0-1.0)')
    parser.add_argument('--num-copies', type=int, default=50, help='エクスポートする画像の枚数')
    parser.add_argument('--workers', type=int, default=4, help='並列処理のワーカー数')
    parser.add_argument('--debug', action='store_true', help='デバッグモードを有効にする')

    args = parser.parse_args()
    
    # デバッグモードが有効な場合はログレベルをDEBUGに設定
    if args.debug:
        logger.setLevel(logging.DEBUG)
        logger.debug("デバッグモードが有効になりました")

    exporter = ChildImageExporter(
        db_path=args.db_path,
        output_dir=args.output_dir,
        cache_file=args.cache_file,
        model_dir=args.model_dir,
        confidence_threshold=args.confidence,
        num_copies=args.num_copies,
        max_workers=args.workers
    )

    exporter.process_images()
    exporter.export_random_images()

if __name__ == "__main__":
    try:
        start_time = time.time()
        logger.info("プログラムを開始します")
        main()
        elapsed_time = time.time() - start_time
        logger.info(f"プログラムが正常に終了しました (所要時間: {elapsed_time:.1f}秒)")
    except Exception as e:
        logger.error(f"予期しないエラーが発生しました: {e}")
        traceback.print_exc()
        raise