#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
get_media_data.py - メディアファイルメタデータ抽出・管理ツール
==============================================================

概要:
----
このスクリプトは指定されたディレクトリ内のファイル（デフォルトではすべてのファイル）をスキャンし、
メタデータを抽出してSQLiteデータベースに保存します。再実行時には新規または変更されたファイルのみを
処理するため、効率的に大量のファイルを管理できます。

主な機能:
-------
- ファイルのメタデータ（作成日時、更新日時、撮影日時など）を抽出
- 動画ファイルの長さを取得
- SQLiteデータベースにデータを保存
- 再実行時には変更されたファイルのみを更新
- マルチスレッドによる高速処理
- 詳細な統計情報の表示
- CSVエクスポート機能

対応ファイル形式:
-------------
- 画像: jpg, jpeg, png, gif, bmp, tiff, webp, heic, heif
- 動画: mp4, avi, mov, wmv, flv, mkv, webm, m4v, 3gp, mpg, mpeg
- その他: すべてのファイル（デフォルト）

コマンドラインオプション:
--------------------
--verbose, -v       詳細なログ出力を有効にする
--threads, -t       ワーカースレッド数を指定（デフォルト: CPU数）
--extensions, -e    追加のファイル拡張子をカンマ区切りで指定
--media-only, -m    メディアファイル（画像・動画）のみをスキャン
--export-csv, -c    データベースの内容をCSVファイルにエクスポート
--stats, -s         詳細な統計情報を表示
--force, -f         既存のデータベースエントリも強制的に再処理
--db-path, -d       SQLiteデータベースファイルのパスを指定（デフォルト: プログラムと同じディレクトリのdata.db）

使用例:
-----
# 基本的な使い方（すべてのファイルをスキャン）
python get_media_data.py /path/to/photos

# メディアファイル（画像・動画）のみをスキャン
python get_media_data.py /path/to/photos --media-only

# カスタム拡張子を追加
python get_media_data.py /path/to/photos --extensions "raw,cr2,arw"

# 詳細な統計情報を表示
python get_media_data.py /path/to/photos --stats

# データベースをCSVにエクスポート
python get_media_data.py /path/to/photos --export-csv /path/to/export.csv

# 複数のディレクトリを同じデータベースにスキャン
python get_media_data.py /path/to/photos/2025 --db-path /path/to/data/media.db
python get_media_data.py /path/to/photos/2024 --db-path /path/to/data/media.db

# 複数のオプションを組み合わせる
python get_media_data.py /path/to/photos --threads 8 --db-path /path/to/data/media.db --stats

# 既存のデータベースエントリも強制的に再処理
python get_media_data.py /path/to/photos --force

データベース構造:
-------------
テーブル名: media_files
- id: 主キー（自動採番）
- full_path: ファイルのフルパス（一意）
- file_name: ファイル名
- file_type: ファイルタイプ（image, video, other）
- file_extension: ファイル拡張子
- file_size: ファイルサイズ（バイト）
- file_creation_time: ファイル作成日時
- file_modification_time: ファイル更新日時
- capture_time: 撮影日時（メタデータから取得）
- duration: 動画の長さ（秒）
- error_message: エラーメッセージ（処理中にエラーが発生した場合）
- last_updated: データ取得・更新日時
- file_hash: ファイルハッシュ（変更検出用）
- processed: 処理状態（1=成功, 0=エラー）
"""

import os
import sys
import sqlite3
import datetime
import logging
import argparse
import time
import concurrent.futures
import hashlib
import re
import io
import subprocess
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Set, Union, Any

# For timezone handling
try:
    import pytz
except ImportError:
    print("pytz library not found. Installing...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pytz"])
    import pytz

# For YAML config file
try:
    import yaml
except ImportError:
    print("PyYAML library not found. Installing...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "PyYAML"])
    import yaml

# For image metadata
try:
    from PIL import Image, ImageFile
    from PIL.ExifTags import TAGS
except ImportError:
    print("Pillow library not found. Installing...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "Pillow"])
    from PIL import Image, ImageFile
    from PIL.ExifTags import TAGS

# For more robust EXIF extraction
try:
    import exifread
except ImportError:
    print("exifread library not found. Installing...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "exifread"])
    try:
        import exifread
    except ImportError:
        print("Warning: exifread installation failed. Some image metadata may not be available.")

# For even more robust metadata extraction
try:
    import pyexiv2
except ImportError:
    print("pyexiv2 library not found. Installing...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pyexiv2"])
    try:
        import pyexiv2
    except ImportError:
        print("Warning: pyexiv2 installation failed. Some image metadata may not be available.")

# For external exiftool command
def check_exiftool_installed():
    """Check if exiftool is installed on the system."""
    try:
        subprocess.run(["exiftool", "-ver"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
        return True
    except (subprocess.SubprocessError, FileNotFoundError):
        print("Warning: exiftool not found on system. Some metadata extraction may be limited.")
        return False

EXIFTOOL_AVAILABLE = check_exiftool_installed()

# For video metadata
try:
    import cv2
except ImportError:
    print("OpenCV library not found. Installing...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "opencv-python"])
    import cv2

# For MediaInfo metadata extraction
try:
    import pymediainfo
except ImportError:
    print("pymediainfo library not found. Installing...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pymediainfo"])
    try:
        import pymediainfo
    except ImportError:
        print("Warning: pymediainfo installation failed. Some video metadata may not be available.")

# For advanced video metadata extraction
try:
    import ffmpeg
except ImportError:
    print("ffmpeg-python library not found. Installing...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "ffmpeg-python"])
    try:
        import ffmpeg
    except ImportError:
        print("Warning: ffmpeg-python installation failed. Some video metadata may not be available.")

try:
    from hachoir.parser import createParser
    from hachoir.metadata import extractMetadata
except ImportError:
    print("hachoir library not found. Installing...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "hachoir"])
    try:
        from hachoir.parser import createParser
        from hachoir.metadata import extractMetadata
    except ImportError:
        print("Warning: hachoir installation failed. Some video metadata may not be available.")

# Common image and video extensions
IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.webp', '.heic', '.heif'}
VIDEO_EXTENSIONS = {'.mp4', '.avi', '.mov', '.wmv', '.flv', '.mkv', '.webm', '.m4v', '.3gp', '.mpg', '.mpeg'}
MEDIA_EXTENSIONS = IMAGE_EXTENSIONS.union(VIDEO_EXTENSIONS)

# Global timezone object and validation flag
TIMEZONE = None
STRICT_DATE_VALIDATION = False

def load_config(config_path: Optional[Union[str, Path]] = None) -> Dict[str, Any]:
    """Load configuration from YAML file."""
    if config_path is None:
        # Use default config file in the same directory as the script
        config_path = Path(__file__).parent.absolute() / "config.yaml"
    
    config_path = Path(config_path) if isinstance(config_path, str) else config_path
    
    # Default configuration
    default_config = {
        "timezone": "UTC",
        "database": {
            "filename": "data.db"
        },
        "logging": {
            "default_level": "INFO",
            "filename_format": "%Y%m%d-%H%M%S.log"
        }
    }
    
    # Load configuration from file if it exists
    if config_path.exists():
        try:
            with config_path.open('r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
                if config is None:
                    config = {}
        except Exception as e:
            print(f"Error loading config file: {e}")
            config = {}
    else:
        print(f"Config file not found: {config_path}")
        config = {}
    
    # Merge with default configuration
    merged_config = default_config.copy()
    
    # Update top-level keys
    for key, value in config.items():
        if isinstance(value, dict) and key in merged_config and isinstance(merged_config[key], dict):
            # If it's a nested dictionary, update it instead of replacing
            merged_config[key].update(value)
        else:
            # Otherwise, replace the value
            merged_config[key] = value
    
    # Initialize timezone
    global TIMEZONE
    try:
        TIMEZONE = pytz.timezone(merged_config["timezone"])
        print(f"Using timezone: {TIMEZONE}")
    except Exception as e:
        print(f"Error setting timezone: {e}")
        print("Falling back to UTC")
        TIMEZONE = pytz.UTC
    
    return merged_config

def setup_logging(verbose: bool = False, config: Optional[Dict[str, Any]] = None) -> logging.Logger:
    """Set up logging to both console and file."""
    if config is None:
        config = load_config()
    log_format = config["logging"]["filename_format"]
    timestamp = datetime.datetime.now(TIMEZONE).strftime(log_format)
    
    # Always use the directory where the script is located
    script_dir = Path(__file__).parent.absolute()
    
    log_file = script_dir / timestamp
    
    # Create logger
    logger = logging.getLogger("MediaScanner")
    logger.setLevel(logging.INFO)
    
    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG if verbose else logging.INFO)
    
    # Create file handler
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG if verbose else logging.INFO)
    
    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)
    
    # Add handlers to logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    return logger

def init_database(db_path: Union[str, Path]) -> sqlite3.Connection:
    """Initialize the SQLite database."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create table if it doesn't exist
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS media_files (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        full_path TEXT UNIQUE,
        file_name TEXT,
        file_type TEXT,
        file_extension TEXT,
        file_size INTEGER,
        file_creation_time TEXT,
        file_modification_time TEXT,
        capture_time TEXT,
        duration REAL,
        error_message TEXT,
        last_updated TEXT,
        file_hash TEXT,
        processed INTEGER DEFAULT 1
    )
    ''')
    
    # Create index on file_type for faster queries
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_file_type ON media_files(file_type)')
    
    conn.commit()
    return conn

def get_file_hash(file_path: Union[str, Path]) -> str:
    """Calculate a hash of the file's content for change detection."""
    try:
        # Convert to Path object if it's a string
        path_obj = Path(file_path) if isinstance(file_path, str) else file_path
        
        # For large files, just hash the first 8KB and last 8KB plus file size and modification time
        # This is a compromise between accuracy and performance
        file_stat = path_obj.stat()
        file_size = file_stat.st_size
        mtime = file_stat.st_mtime
        
        hasher = hashlib.md5()
        hasher.update(f"{file_size}_{mtime}".encode())
        
        with path_obj.open('rb') as f:
            # Read first 8KB
            first_chunk = f.read(8192)
            hasher.update(first_chunk)
            
            # If file is larger than 16KB, read last 8KB too
            if file_size > 16384:
                f.seek(file_size - 8192)
                last_chunk = f.read(8192)
                hasher.update(last_chunk)
        
        return hasher.hexdigest()
    except Exception as e:
        return f"hash_error_{str(e)}"

def get_existing_files(conn: sqlite3.Connection) -> Dict[str, str]:
    """Get existing files and their hashes from the database."""
    cursor = conn.cursor()
    cursor.execute("SELECT full_path, file_hash FROM media_files")
    return {row[0]: row[1] for row in cursor.fetchall()}
def get_capture_time_from_image(file_path: Union[str, Path]) -> Optional[str]:
    """Extract capture time from image metadata using multiple methods."""
    file_path_str = str(file_path)
    extension = Path(file_path).suffix.lower()
    capture_time = None
    
    # Method 1: Try PIL first (standard method)
    try:
        # Enable more robust parsing for problematic files
        ImageFile.LOAD_TRUNCATED_IMAGES = True
        
        with Image.open(file_path_str) as img:
            # For HEIC files, PIL might not extract EXIF directly, but we can try
            if hasattr(img, '_getexif'):
                exif_data = img._getexif()
                if exif_data:
                    # Look for DateTimeOriginal (tag 36867) or DateTime (tag 306)
                    for tag_id, value in exif_data.items():
                        tag = TAGS.get(tag_id, tag_id)
                        if tag == 'DateTimeOriginal' and value:
                            capture_time = value
                            print(f"Found date using PIL (DateTimeOriginal): {capture_time}")
                            break
                        elif tag == 'DateTime' and value:
                            capture_time = value
                            print(f"Found date using PIL (DateTime): {capture_time}")
                            break
            
            # Try to get all EXIF data for debugging
            if capture_time is None and extension in ['.heic', '.heif']:
                print(f"Trying to extract all available EXIF data from {file_path_str}")
                try:
                    # Get all available image info
                    info = img.info
                    for key, value in info.items():
                        if isinstance(key, str) and 'date' in key.lower() and value:
                            print(f"Found potential date in image info: {key}: {value}")
                            if capture_time is None:
                                capture_time = str(value)
                except Exception as e:
                    print(f"Failed to extract image info: {e}")
    except Exception as e:
        print(f"PIL metadata extraction failed for {file_path_str}: {e}")
    
    # Method 2: Try exifread (more robust for some formats)
    if capture_time is None and 'exifread' in sys.modules:
        try:
            with open(file_path_str, 'rb') as f:
                tags = exifread.process_file(f, details=False)
                # Try different date tags
                for tag_name in ['EXIF DateTimeOriginal', 'EXIF DateTimeDigitized', 'Image DateTime']:
                    if tag_name in tags:
                        capture_time = str(tags[tag_name])
                        print(f"Found date using exifread: {capture_time}")
                        break
                
                # If still not found, print all available tags for debugging
                if capture_time is None and extension in ['.heic', '.heif']:
                    print(f"Available exifread tags for {file_path_str}:")
                    for tag_name, tag_value in tags.items():
                        if 'date' in tag_name.lower() or 'time' in tag_name.lower():
                            print(f"  {tag_name}: {tag_value}")
        except Exception as e:
            print(f"exifread metadata extraction failed for {file_path_str}: {e}")
    
    # Method 3: Try pyexiv2 (most comprehensive)
    if capture_time is None and 'pyexiv2' in sys.modules:
        try:
            metadata = pyexiv2.ImageMetadata(file_path_str)
            metadata.read()
            
            # Try different date tags in different namespaces
            date_keys = [
                'Exif.Photo.DateTimeOriginal',
                'Exif.Image.DateTime',
                'Exif.Image.DateTimeOriginal',
                'Xmp.xmp.CreateDate',
                'Xmp.exif.DateTimeOriginal',
                'Xmp.photoshop.DateCreated',
                'Iptc.Application2.DateCreated'
            ]
            
            for key in date_keys:
                if key in metadata:
                    value = metadata[key].value
                    if isinstance(value, (datetime.datetime, datetime.date)):
                        capture_time = value.strftime('%Y:%m:%d %H:%M:%S')
                    else:
                        capture_time = str(value)
                    print(f"Found date using pyexiv2 ({key}): {capture_time}")
                    break
            
            # If still not found, try to extract from filename for iOS files
            if capture_time is None and ('iOS' in file_path_str or '_iOS' in file_path_str):
                # Try to extract from filename as a last resort
                filename = os.path.basename(file_path_str)
                match = re.match(r'(\d{8})_(\d{9})_iOS', filename)
                if match:
                    date_str = match.group(1)
                    time_str = match.group(2)
                    # Format: YYYYMMDD_HHMMSSMMM
                    year = date_str[:4]
                    month = date_str[4:6]
                    day = date_str[6:8]
                    hour = time_str[:2]
                    minute = time_str[2:4]
                    second = time_str[4:6]
                    
                    # Create datetime object
                    dt = datetime.datetime(int(year), int(month), int(day),
                                          int(hour), int(minute), int(second))
                    capture_time = dt.strftime('%Y:%m:%d %H:%M:%S')
                    print(f"Extracted date from filename: {capture_time}")
        except Exception as e:
            print(f"pyexiv2 metadata extraction failed for {file_path_str}: {e}")
    
    # Method 4: Last resort, use exiftool (external command)
    if capture_time is None and EXIFTOOL_AVAILABLE:
        try:
            # Run exiftool to extract DateTimeOriginal
            cmd = ["exiftool", "-DateTimeOriginal", "-s3", file_path_str]
            result = subprocess.run(cmd, capture_output=True, text=True, check=False)
            if result.stdout.strip():
                capture_time = result.stdout.strip()
                print(f"Found date using exiftool: {capture_time}")
            else:
                # Try other date tags
                cmd = ["exiftool", "-CreateDate", "-s3", file_path_str]
                result = subprocess.run(cmd, capture_output=True, text=True, check=False)
                if result.stdout.strip():
                    capture_time = result.stdout.strip()
                    print(f"Found date using exiftool (CreateDate): {capture_time}")
                else:
                    # Try FileModifyDate as last resort
                    cmd = ["exiftool", "-FileModifyDate", "-s3", file_path_str]
                    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
                    if result.stdout.strip():
                        capture_time = result.stdout.strip()
                        print(f"Found date using exiftool (FileModifyDate): {capture_time}")
        except Exception as e:
            print(f"exiftool metadata extraction failed for {file_path_str}: {e}")
    
    # Convert capture_time to ISO 8601 format with timezone if found
    if capture_time:
        try:
            # Try to parse various date formats
            dt = None
            
            # EXIF standard format: YYYY:MM:DD HH:MM:SS
            if ':' in capture_time and ' ' in capture_time:
                try:
                    parts = capture_time.split(' ')
                    if len(parts) >= 2 and parts[0].count(':') == 2:
                        dt = datetime.datetime.strptime(capture_time, '%Y:%m:%d %H:%M:%S')
                except:
                    pass
            
            # Try other common formats
            if dt is None:
                # Try various formats
                formats = [
                    '%Y:%m:%d %H:%M:%S',
                    '%Y-%m-%d %H:%M:%S',
                    '%Y-%m-%dT%H:%M:%S',
                    '%Y-%m-%d %H:%M:%S%z',
                    '%Y-%m-%dT%H:%M:%S%z'
                ]
                
                for fmt in formats:
                    try:
                        dt = datetime.datetime.strptime(capture_time, fmt)
                        break
                    except:
                        continue
            
            # If we successfully parsed the date
            if dt:
                # Add UTC timezone if not specified
                if dt.tzinfo is None:
                    dt = pytz.UTC.localize(dt)
                
                # Always convert to configured timezone
                dt = dt.astimezone(TIMEZONE)
                return dt.isoformat()
            else:
                # If parsing fails, return the original string
                return capture_time
        except Exception as e:
            print(f"Error converting capture time to ISO format: {e}")
            return capture_time
    
    return None

def get_video_metadata(file_path: Union[str, Path]) -> Tuple[Optional[str], Optional[float]]:
    """Extract metadata from video file using multiple libraries."""
    # Convert to string for libraries
    file_path_str = str(file_path)
    path_obj = Path(file_path) if isinstance(file_path, str) else file_path
    capture_time = None
    duration = None
    
    # Method 1: Try OpenCV first (for duration)
    try:
        cap = cv2.VideoCapture(file_path_str)
        if cap.isOpened():
            fps = cap.get(cv2.CAP_PROP_FPS)
            frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
            if fps > 0 and frame_count > 0:
                duration = frame_count / fps
            cap.release()
    except Exception as e:
        print(f"OpenCV metadata extraction failed: {e}")
    
    # Method 2: Try ffmpeg-python
    if 'ffmpeg' in sys.modules:
        try:
            probe = ffmpeg.probe(file_path_str)
            video_stream = next((stream for stream in probe['streams'] if stream['codec_type'] == 'video'), None)
            if video_stream:
                # Try to get duration if not already set
                if duration is None and 'duration' in video_stream:
                    duration = float(video_stream['duration'])
                
                # Try to get creation time
                if capture_time is None:
                    creation_time = None
                    
                    # Try to get from format tags first (often more reliable)
                    if 'format' in probe and 'tags' in probe['format']:
                        format_tags = probe['format']['tags']
                        # Check multiple possible tag names
                        for tag_name in ['creation_time', 'date', 'creation_date', 'date_created']:
                            if tag_name in format_tags and format_tags[tag_name]:
                                creation_time = format_tags[tag_name]
                                break
                    
                    # If not found in format tags, try video stream tags
                    if creation_time is None and 'tags' in video_stream:
                        stream_tags = video_stream['tags']
                        for tag_name in ['creation_time', 'date', 'creation_date', 'date_created']:
                            if tag_name in stream_tags and stream_tags[tag_name]:
                                creation_time = stream_tags[tag_name]
                                break
                    
                    # Convert creation time to configured timezone
                    if creation_time:
                        try:
                            # Most ffmpeg creation_time values are in ISO format with Z
                            if 'Z' in creation_time:
                                dt = datetime.datetime.fromisoformat(creation_time.replace('Z', '+00:00'))
                                # Always convert to configured timezone
                                dt = dt.astimezone(TIMEZONE)
                                capture_time = dt.isoformat()
                            else:
                                # Try to parse as ISO format
                                dt = datetime.datetime.fromisoformat(creation_time)
                                # Add UTC timezone if not specified
                                if dt.tzinfo is None:
                                    dt = pytz.UTC.localize(dt)
                                # Always convert to configured timezone
                                dt = dt.astimezone(TIMEZONE)
                                capture_time = dt.isoformat()
                        except:
                            # If parsing fails, store the original value
                            capture_time = creation_time
        except Exception as e:
            print(f"ffmpeg metadata extraction failed: {e}")
    
    # Method 3: Try hachoir
    if capture_time is None and all(m in sys.modules for m in ['hachoir.parser', 'hachoir.metadata']):
        try:
            parser = createParser(file_path_str)
            if parser:
                metadata = extractMetadata(parser)
                if metadata:
                    # Try different metadata fields for creation date
                    for date_key in ['creation_date', 'last_modification', 'date_time_original']:
                        if hasattr(metadata, date_key):
                            date_value = getattr(metadata, date_key)
                            if date_value:
                                # Skip empty or obviously wrong values
                                date_str = str(date_value)
                                if not date_str or date_str == '0' or len(date_str) < 8:
                                    continue
                                try:
                                    # Try to parse the date
                                    if ':' in date_str and len(date_str) >= 19:
                                        # YYYY:MM:DD HH:MM:SS format
                                        dt = datetime.datetime.strptime(date_str, '%Y:%m:%d %H:%M:%S')
                                    else:
                                        # Try as ISO format
                                        dt = datetime.datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                                    
                                    # Add UTC timezone if not specified
                                    if dt.tzinfo is None:
                                        dt = pytz.UTC.localize(dt)
                                    # Always convert to configured timezone
                                    dt = dt.astimezone(TIMEZONE)
                                    capture_time = dt.isoformat()
                                except:
                                    # If parsing fails, use the original string
                                    capture_time = date_str
                                break
                    
                    # Try to get duration if not already set
                    if duration is None and hasattr(metadata, 'duration'):
                        duration_value = metadata.get('duration')
                        if duration_value:
                            duration = duration_value.total_seconds()
                
                parser.close()
        except Exception as e:
            print(f"hachoir metadata extraction failed: {e}")
    
    # Method 4: Try MediaInfo (especially good for 3GP files)
    if (capture_time is None or duration is None) and 'pymediainfo' in sys.modules:
        try:
            media_info = pymediainfo.MediaInfo.parse(file_path_str)
            for track in media_info.tracks:
                # Try to get creation time from general track
                if track.track_type == 'General':
                    if capture_time is None:
                        # Try different date fields
                        for date_field in ['encoded_date', 'tagged_date', 'recorded_date', 'mastered_date', 'encoded_date', 'file_creation_date', 'file_modification_date']:
                            if hasattr(track, date_field) and getattr(track, date_field):
                                date_str = getattr(track, date_field)
                                # Skip empty or obviously wrong values
                                if not date_str or date_str == '0' or len(date_str) < 8:
                                    continue
                                
                                try:
                                    # MediaInfo dates often have UTC prefix
                                    if date_str.startswith('UTC '):
                                        dt = datetime.datetime.strptime(date_str[4:], '%Y-%m-%d %H:%M:%S')
                                        # Add UTC timezone and convert to configured timezone
                                        dt = pytz.UTC.localize(dt).astimezone(TIMEZONE)
                                    else:
                                        # Try to parse as ISO format
                                        dt = datetime.datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                                        # Add UTC timezone if not specified
                                        if dt.tzinfo is None:
                                            dt = pytz.UTC.localize(dt)
                                        # Always convert to configured timezone
                                        dt = dt.astimezone(TIMEZONE)
                                    capture_time = dt.isoformat()
                                except:
                                    # If parsing fails, use the original string
                                    capture_time = date_str
                                break
                    
                    # Try to get duration
                    if duration is None and hasattr(track, 'duration'):
                        try:
                            duration = float(track.duration) / 1000.0  # Convert ms to seconds
                        except (ValueError, TypeError):
                            pass
                
                # Also check video track for additional metadata
                if track.track_type == 'Video' and capture_time is None:
                    for date_field in ['encoded_date', 'tagged_date']:
                        if hasattr(track, date_field) and getattr(track, date_field):
                            date_str = getattr(track, date_field)
                            try:
                                # MediaInfo dates often have UTC prefix
                                if date_str.startswith('UTC '):
                                    dt = datetime.datetime.strptime(date_str[4:], '%Y-%m-%d %H:%M:%S')
                                    # Add UTC timezone and convert to configured timezone
                                    dt = pytz.UTC.localize(dt).astimezone(TIMEZONE)
                                else:
                                    # Try to parse as ISO format
                                    dt = datetime.datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                                    # Add UTC timezone if not specified
                                    if dt.tzinfo is None:
                                        dt = pytz.UTC.localize(dt)
                                    # Always convert to configured timezone
                                    dt = dt.astimezone(TIMEZONE)
                                capture_time = dt.isoformat()
                            except:
                                # If parsing fails, use the original string
                                capture_time = date_str
                            break
        except Exception as e:
            print(f"MediaInfo metadata extraction failed: {e}")
    
    # Check for "UTC" suffix in capture_time (e.g. "2018-06-30 01:11:12.506 UTC")
    if capture_time and isinstance(capture_time, str) and capture_time.endswith(" UTC"):
        try:
            # Remove the UTC suffix and parse the date
            date_str = capture_time.replace(" UTC", "")
            dt = datetime.datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S.%f')
            # Add UTC timezone and convert to configured timezone
            dt = pytz.UTC.localize(dt).astimezone(TIMEZONE)
            capture_time = dt.isoformat()
            print(f"Converted UTC suffix date: {capture_time}")
        except Exception as e:
            print(f"Error converting UTC suffix date: {e}")
    
    # Validate capture_time - filter out obviously incorrect dates
    if capture_time:
        try:
            # Check if the date is obviously wrong (future date or too old)
            dt = None
            if isinstance(capture_time, str):
                # Try to parse the date string
                if 'Z' in capture_time:
                    dt = datetime.datetime.fromisoformat(capture_time.replace('Z', '+00:00'))
                elif 'T' in capture_time:
                    dt = datetime.datetime.fromisoformat(capture_time)
                elif ':' in capture_time and len(capture_time) >= 19:
                    dt = datetime.datetime.strptime(capture_time, '%Y:%m:%d %H:%M:%S')
                elif capture_time.startswith('UTC '):
                    dt = datetime.datetime.strptime(capture_time[4:], '%Y-%m-%d %H:%M:%S')
                
                # If we successfully parsed the date
                if dt:
                    # Check if date is in the future
                    now = datetime.datetime.now(TIMEZONE if dt.tzinfo else None)
                    if dt > now:
                        print(f"Warning: Future date detected in metadata: {capture_time}, ignoring")
                        capture_time = None
                    # Check if date is too old (before digital cameras were common)
                    elif dt.year < 1990 and STRICT_DATE_VALIDATION:
                        print(f"Warning: Very old date detected in metadata: {capture_time}, ignoring")
                        capture_time = None
                    # Check if date is suspiciously recent for an old file
                    elif dt.year > 2015 and '2009' in str(file_path) and STRICT_DATE_VALIDATION:
                        print(f"Warning: Recent date for old file: {capture_time}, file path suggests older date: {file_path}")
        except Exception as e:
            print(f"Error validating capture_time: {e}")
    
    # Standardize capture_time format to ISO 8601 with timezone if it's still valid
    if capture_time:
        try:
            # Skip if already in ISO format with timezone
            if isinstance(capture_time, str) and 'T' in capture_time and ('+' in capture_time or capture_time.endswith('Z')):
                # Already in ISO format with timezone, just ensure it's in the configured timezone
                try:
                    dt = datetime.datetime.fromisoformat(capture_time.replace('Z', '+00:00'))
                    if dt.tzinfo is not None and dt.tzinfo != TIMEZONE:
                        dt = dt.astimezone(TIMEZONE)
                        capture_time = dt.isoformat()
                except:
                    pass
            else:
                # Try to parse various date formats
                dt = None
                
                # Case 1: ISO format with Z (UTC)
                if isinstance(capture_time, str) and 'Z' in capture_time:
                    try:
                        dt = datetime.datetime.fromisoformat(capture_time.replace('Z', '+00:00'))
                    except:
                        pass
                
                # Case 2: ISO format without Z
                elif isinstance(capture_time, str) and 'T' in capture_time:
                    try:
                        dt = datetime.datetime.fromisoformat(capture_time)
                    except:
                        pass
                
                # Case 3: YYYY:MM:DD HH:MM:SS format (common in EXIF)
                elif isinstance(capture_time, str) and ':' in capture_time and ' ' in capture_time:
                    try:
                        # Try to match EXIF format more loosely
                        parts = capture_time.split(' ')
                        if len(parts) >= 2 and parts[0].count(':') == 2:
                            dt = datetime.datetime.strptime(capture_time, '%Y:%m:%d %H:%M:%S')
                    except:
                        pass
                
                # Case 4: UTC prefixed date
                elif isinstance(capture_time, str) and capture_time.startswith('UTC '):
                    try:
                        dt = datetime.datetime.strptime(capture_time[4:], '%Y-%m-%d %H:%M:%S')
                    except:
                        pass
                
                # If we successfully parsed the date, convert to configured timezone and ISO 8601
                if dt:
                    # Add UTC timezone if not specified
                    if dt.tzinfo is None:
                        dt = pytz.UTC.localize(dt)
                    
                    # Always convert to configured timezone
                    dt = dt.astimezone(TIMEZONE)
                    capture_time = dt.isoformat()
                    print(f"Converted date to ISO format with timezone: {capture_time}")
        except Exception as e:
            print(f"Error standardizing capture_time: {e}")
            # If all parsing fails, keep the original format
    
    return capture_time, duration

def get_file_type(extension: str) -> str:
    """Determine file type based on extension."""
    if extension in IMAGE_EXTENSIONS:
        return "image"
    elif extension in VIDEO_EXTENSIONS:
        return "video"
    else:
        return "other"

def process_file(file_path: Union[str, Path], existing_files: Dict[str, str], media_only: bool = False, force: bool = False, conn: Optional[sqlite3.Connection] = None) -> Optional[Dict]:
    """Process a single file and extract its metadata."""
    try:
        # Convert to Path object if it's a string
        path_obj = Path(file_path) if isinstance(file_path, str) else file_path
        file_path_str = str(path_obj)
        file_name = path_obj.name
        extension = path_obj.suffix.lower()
        
        # Skip if media_only is True and this is not a media file
        if media_only and extension not in MEDIA_EXTENSIONS:
            return None
        
        # Get file stats
        file_stat = path_obj.stat()
        file_size = file_stat.st_size
        # Store all dates in ISO 8601 format with timezone for consistency
        # Convert file timestamps to configured timezone
        ctime_dt = datetime.datetime.fromtimestamp(file_stat.st_ctime)
        mtime_dt = datetime.datetime.fromtimestamp(file_stat.st_mtime)
        
        # Always localize to configured timezone
        # Add timezone info if not specified
        ctime_dt = TIMEZONE.localize(ctime_dt) if ctime_dt.tzinfo is None else ctime_dt.astimezone(TIMEZONE)
        mtime_dt = TIMEZONE.localize(mtime_dt) if mtime_dt.tzinfo is None else mtime_dt.astimezone(TIMEZONE)
        
        file_creation_time = ctime_dt.isoformat()
        file_modification_time = mtime_dt.isoformat()
        
        # Calculate file hash
        file_hash = get_file_hash(file_path_str)
        
        # Skip if file hasn't changed and force is not enabled
        if not force and file_path_str in existing_files and existing_files[file_path_str] == file_hash:
            return None
        
        # Initialize variables
        capture_time = None
        duration = None
        error_message = None
        file_type = get_file_type(extension)
        
        try:
            # Process based on file type
            if extension in IMAGE_EXTENSIONS:
                capture_time = get_capture_time_from_image(file_path_str)
                
                # Additional check for EXIF format from image files
                if capture_time and isinstance(capture_time, str) and ':' in capture_time and ' ' in capture_time:
                    try:
                        # Try to match EXIF format more loosely
                        parts = capture_time.split(' ')
                        if len(parts) >= 2 and parts[0].count(':') == 2:
                            # Try to parse as EXIF format
                            dt = datetime.datetime.strptime(capture_time, '%Y:%m:%d %H:%M:%S')
                            # EXIF dates are usually in local time, but we'll treat them as UTC
                            # and convert to configured timezone for consistency
                            dt = pytz.UTC.localize(dt).astimezone(TIMEZONE)
                            capture_time = dt.isoformat()
                            print(f"Converted EXIF format date from image in process_file: {capture_time}")
                    except Exception as e:
                        print(f"Error converting EXIF format date from image in process_file: {e}")
                
                # If still no capture_time, try to extract from filename for iOS files
                if capture_time is None and ('iOS' in file_name or '_iOS' in file_name):
                    # Try to extract from filename as a last resort
                    match = re.match(r'(\d{8})_(\d{9})_iOS', file_name)
                    if match:
                        date_str = match.group(1)
                        time_str = match.group(2)
                        # Format: YYYYMMDD_HHMMSSMMM
                        year = date_str[:4]
                        month = date_str[4:6]
                        day = date_str[6:8]
                        hour = time_str[:2]
                        minute = time_str[2:4]
                        second = time_str[4:6]
                        
                        # Create datetime object
                        try:
                            dt = datetime.datetime(int(year), int(month), int(day),
                                                int(hour), int(minute), int(second))
                            dt = pytz.UTC.localize(dt).astimezone(TIMEZONE)
                            capture_time = dt.isoformat()
                            print(f"Extracted date from filename: {capture_time}")
                        except Exception as e:
                            print(f"Error extracting date from filename: {e}")
                
                # Special handling for iOS HEIC/HEIF files with filename pattern
                if (capture_time is None and extension in ['.heic', '.heif', '.jpg'] and
                    ('iOS' in file_name or '_iOS' in file_name)):
                    print(f"Special handling for iOS file: {file_path_str}")
                    # Try exiftool as a last resort for iOS files
                    try:
                        cmd = ["exiftool", "-AllDates", "-s3", file_path_str]
                        result = subprocess.run(cmd, capture_output=True, text=True, check=False)
                        if result.stdout.strip():
                            capture_time = result.stdout.strip()
                            print(f"Found date using exiftool for iOS file: {capture_time}")
                            
                            # Convert to ISO format
                            try:
                                dt = datetime.datetime.strptime(capture_time, '%Y:%m:%d %H:%M:%S')
                                dt = pytz.UTC.localize(dt).astimezone(TIMEZONE)
                                capture_time = dt.isoformat()
                                print(f"Converted iOS file date to ISO format: {capture_time}")
                            except Exception as e:
                                print(f"Error converting iOS file date: {e}")
                    except Exception as e:
                        print(f"exiftool failed for iOS file: {e}")
            elif extension in VIDEO_EXTENSIONS:
                capture_time, duration = get_video_metadata(file_path_str)
                
                # Final check to ensure all dates are in ISO format with timezone
                if capture_time and isinstance(capture_time, str):
                    # Case 1: "UTC" suffix
                    if capture_time.endswith(" UTC"):
                        try:
                            # Remove the UTC suffix and parse the date
                            date_str = capture_time.replace(" UTC", "")
                            dt = datetime.datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S.%f')
                            # Add UTC timezone and convert to configured timezone
                            dt = pytz.UTC.localize(dt).astimezone(TIMEZONE)
                            capture_time = dt.isoformat()
                            print(f"Converted UTC suffix date in process_file: {capture_time}")
                        except Exception as e:
                            print(f"Error converting UTC suffix date in process_file: {e}")
                    
                    # Case 2: EXIF format (YYYY:MM:DD HH:MM:SS)
                    elif ':' in capture_time and ' ' in capture_time:
                        try:
                            # Try to match EXIF format more loosely
                            parts = capture_time.split(' ')
                            if len(parts) >= 2 and parts[0].count(':') == 2:
                                # Try to parse as EXIF format
                                dt = datetime.datetime.strptime(capture_time, '%Y:%m:%d %H:%M:%S')
                                # EXIF dates are usually in local time, but we'll treat them as UTC
                                # and convert to configured timezone for consistency
                                dt = pytz.UTC.localize(dt).astimezone(TIMEZONE)
                                capture_time = dt.isoformat()
                                print(f"Converted EXIF format date in process_file: {capture_time}")
                        except Exception as e:
                            print(f"Error converting EXIF format date in process_file: {e}")
                    
                    # Case 3: Already in ISO format but without timezone
                    elif 'T' in capture_time and '+' not in capture_time and '-' not in capture_time.split('T')[1]:
                        try:
                            dt = datetime.datetime.fromisoformat(capture_time)
                            # Add UTC timezone if not specified
                            if dt.tzinfo is None:
                                dt = pytz.UTC.localize(dt)
                            # Always convert to configured timezone
                            dt = dt.astimezone(TIMEZONE)
                            capture_time = dt.isoformat()
                            print(f"Added timezone to ISO format date in process_file: {capture_time}")
                        except Exception as e:
                            print(f"Error adding timezone to ISO format date in process_file: {e}")
        except Exception as e:
            error_message = str(e)
        
        # Return file data
        return {
            'full_path': file_path_str,
            'file_name': file_name,
            'file_type': file_type,
            'file_extension': extension,
            'file_size': file_size,
            'file_creation_time': file_creation_time,
            'file_modification_time': file_modification_time,
            'capture_time': capture_time,
            'duration': duration,
            'error_message': error_message,
            # Ensure all dates are in ISO 8601 format with configured timezone
            'last_updated': datetime.datetime.now(TIMEZONE).isoformat(),
            'file_hash': file_hash,
            'processed': 1
        }
    except Exception as e:
        try:
            # Try to get some basic info even if processing failed
            path_obj = Path(file_path) if isinstance(file_path, str) else file_path
            extension = path_obj.suffix.lower()
            file_name = path_obj.name
        except:
            # If even that fails, use string methods
            file_path_str = str(file_path)
            extension = os.path.splitext(file_path_str)[1].lower()
            file_name = os.path.basename(file_path_str)
            
        return {
            'full_path': str(file_path),
            'file_name': file_name,
            'file_type': 'unknown',
            'file_extension': extension,
            'file_size': 0,
            'error_message': f"Failed to process file: {str(e)}",
            # Ensure all dates are in ISO 8601 format with configured timezone
            'last_updated': datetime.datetime.now(TIMEZONE).isoformat(),
            'file_hash': None,
            'processed': 0
        }

def save_to_database(conn: sqlite3.Connection, file_data: Dict) -> None:
    """Save file metadata to the database."""
    cursor = conn.cursor()
    
    # Check if the file already exists in the database
    cursor.execute("SELECT id FROM media_files WHERE full_path = ?", (file_data['full_path'],))
    existing_id = cursor.fetchone()
    
    if existing_id:
        # Update existing record
        cursor.execute('''
        UPDATE media_files SET
            file_name = ?,
            file_type = ?,
            file_extension = ?,
            file_size = ?,
            file_creation_time = ?,
            file_modification_time = ?,
            capture_time = ?,
            duration = ?,
            error_message = ?,
            last_updated = ?,
            file_hash = ?,
            processed = ?
        WHERE full_path = ?
        ''', (
            file_data['file_name'],
            file_data['file_type'],
            file_data['file_extension'],
            file_data['file_size'],
            file_data['file_creation_time'],
            file_data['file_modification_time'],
            file_data['capture_time'],
            file_data['duration'],
            file_data['error_message'],
            file_data['last_updated'],
            file_data['file_hash'],
            file_data['processed'],
            file_data['full_path']
        ))
    else:
        # Insert new record
        cursor.execute('''
        INSERT INTO media_files (
            full_path, file_name, file_type, file_extension, file_size,
            file_creation_time, file_modification_time, capture_time,
            duration, error_message, last_updated, file_hash, processed
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            file_data['full_path'],
            file_data['file_name'],
            file_data['file_type'],
            file_data['file_extension'],
            file_data['file_size'],
            file_data['file_creation_time'],
            file_data['file_modification_time'],
            file_data['capture_time'],
            file_data['duration'],
            file_data['error_message'],
            file_data['last_updated'],
            file_data['file_hash'],
            file_data['processed']
        ))
    
    conn.commit()

def scan_directory(directory: Union[str, Path], logger: logging.Logger, db_path: Union[str, Path] = None,
                   max_workers: int = None, media_only: bool = False, force: bool = False) -> Dict:
    """Scan the directory for files and process them."""
    start_time = time.time()
    
    # Convert to Path objects
    dir_path = Path(directory) if isinstance(directory, str) else directory
    
    # Ensure the directory exists
    if not dir_path.is_dir():
        logger.error(f"Directory not found: {dir_path}")
        sys.exit(1)
    
    # Initialize database
    if db_path is None:
        # Use the directory where the script is located
        script_dir = Path(__file__).parent.absolute()
        db_path = script_dir / "data.db"
    conn = init_database(db_path)
    
    # Get existing files from database
    existing_files = get_existing_files(conn)
    logger.info(f"Found {len(existing_files)} existing files in database")
    
    # Collect all media files
    logger.info(f"Scanning directory: {dir_path}")
    all_files = []
    for path in dir_path.rglob('*'):
        if path.is_file():
            if not media_only:
                all_files.append(path)
            else:
                extension = path.suffix.lower()
                if extension in MEDIA_EXTENSIONS:
                    all_files.append(path)
    
    total_files = len(all_files)
    logger.info(f"Found {total_files} files to process")
    
    # Process files with a thread pool
    processed_count = 0
    updated_count = 0
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Create a list of arguments for each file
        futures = []
        for file in all_files:
            # Always pass conn for date format conversion
            future = executor.submit(process_file, file, existing_files, media_only, force, conn)
            futures.append((future, file))
        
        future_to_file = {future: file for future, file in futures}
        
        for future in concurrent.futures.as_completed(future_to_file):
            file = future_to_file[future]
            processed_count += 1
            
            # Calculate and display progress
            progress = (processed_count / total_files) * 100
            if processed_count % 10 == 0 or processed_count == total_files:
                logger.info(f"Progress: {progress:.2f}% ({processed_count}/{total_files})")
            
            try:
                file_data = future.result()
                if file_data:
                    save_to_database(conn, file_data)
                    updated_count += 1
                    logger.info(f"Updated: {file_data['file_name']}")
            except Exception as e:
                logger.error(f"Error processing {file}: {str(e)}")
    
    # Close database connection
    conn.close()
    
    # Log summary
    end_time = time.time()
    duration = end_time - start_time
    logger.info(f"Scan completed in {duration:.2f} seconds")
    logger.info(f"Total files scanned: {total_files}")
    logger.info(f"Files added/updated in database: {updated_count}")
    
    # Return statistics
    return {
        'total_files': total_files,
        'updated_files': updated_count,
        'duration': duration
    }

def main(config_path: Optional[str] = None):
    """Main function."""
    # Load configuration
    config = load_config(config_path)
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Scan files and extract metadata')
    parser.add_argument('directory', help='Directory path to scan for files')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    parser.add_argument('--threads', '-t', type=int, default=os.cpu_count(),
                        help=f'Number of worker threads (default: {os.cpu_count()})')
    parser.add_argument('--extensions', '-e', help='Additional file extensions to scan (comma-separated)')
    parser.add_argument('--media-only', '-m', action='store_true',
                        help='Scan only media files (images and videos)')
    parser.add_argument('--export-csv', '-c', help='Export database to CSV file')
    parser.add_argument('--stats', '-s', action='store_true',
                        help='Show detailed statistics after scanning')
    parser.add_argument('--force', '-f', action='store_true',
                        help='Force reprocessing of all files, even if they exist in the database (includes timezone conversion)')
    parser.add_argument('--force-all-dates', '-a', action='store_true',
                        help='Force update of all date formats in the database (use with --force)')
    parser.add_argument('--strict-dates', '--sd', action='store_true',
                        help='Use strict date validation (reject suspicious dates)')
    parser.add_argument('--db-path', '-d', help='Path to the SQLite database file (default: script_directory/data.db)')
    parser.add_argument('--config', '-g', help='Path to the configuration file (default: script_directory/config.yaml)')
    args = parser.parse_args()
    
    directory = args.directory
    
    # Convert to Path object
    dir_path = Path(directory)
    
    # Ensure the directory exists
    if not dir_path.is_dir():
        print(f"Error: Directory not found: {dir_path}")
        sys.exit(1)
    
    # Setup logging with configuration
    logger = setup_logging(args.verbose, config)
    
    # Log start
    logger.info(f"Starting media scan for directory: {directory}")
    
    # Process additional extensions if provided
    global MEDIA_EXTENSIONS
    if args.extensions:
        additional_extensions = {f".{ext.strip().lower()}" for ext in args.extensions.split(',')}
        MEDIA_EXTENSIONS = MEDIA_EXTENSIONS.union(additional_extensions)
        logger.info(f"Added custom extensions: {', '.join(additional_extensions)}")
    
    try:
        # Set global flag for strict date validation if requested
        global STRICT_DATE_VALIDATION
        STRICT_DATE_VALIDATION = args.strict_dates
        
        # If force-all-dates is specified, set force to true as well
        if args.force_all_dates:
            args.force = True
        if STRICT_DATE_VALIDATION:
            logger.info("Using strict date validation")
        
        # Scan directory
        stats = scan_directory(
            directory,
            logger,
            db_path=args.db_path,
            max_workers=args.threads,
            media_only=args.media_only,
            force=args.force
        )
        
        # Export to CSV if requested
        if args.export_csv:
            script_dir = Path(__file__).parent.absolute()
            db_path_to_use = Path(args.db_path) if args.db_path else script_dir / "data.db"
            export_to_csv(db_path_to_use, args.export_csv, logger)
        
        # Show detailed statistics if requested
        if args.stats:
            script_dir = Path(__file__).parent.absolute()
            db_path_to_use = Path(args.db_path) if args.db_path else script_dir / "data.db"
            show_statistics(db_path_to_use, logger, stats)
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        sys.exit(1)

def export_to_csv(db_path: Union[str, Path], csv_path: Union[str, Path], logger: logging.Logger) -> None:
    """Export database contents to a CSV file."""
    import csv
    
    logger.info(f"Exporting database to CSV: {csv_path}")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Get column names
    cursor.execute("PRAGMA table_info(media_files)")
    columns = [row[1] for row in cursor.fetchall()]
    
    # Get all data
    cursor.execute("SELECT * FROM media_files")
    rows = cursor.fetchall()
    
    # Write to CSV
    # Convert to Path objects
    db_path_obj = Path(db_path) if isinstance(db_path, str) else db_path
    csv_path_obj = Path(csv_path) if isinstance(csv_path, str) else csv_path
    
    with csv_path_obj.open('w', newline='', encoding='utf-8') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(columns)
        csv_writer.writerows(rows)
    
    conn.close()
    logger.info(f"Exported {len(rows)} records to {csv_path}")

def show_statistics(db_path: Union[str, Path], logger: logging.Logger, scan_stats: Dict) -> None:
    """Show detailed statistics about the database."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Get total count
    cursor.execute("SELECT COUNT(*) FROM media_files")
    total_count = cursor.fetchone()[0]
    
    # Get counts by file type
    cursor.execute("SELECT file_type, COUNT(*) FROM media_files GROUP BY file_type")
    type_counts = cursor.fetchall()
    
    # Get counts by extension
    cursor.execute("SELECT file_extension, COUNT(*) FROM media_files GROUP BY file_extension ORDER BY COUNT(*) DESC LIMIT 10")
    extension_counts = cursor.fetchall()
    
    # Get total size
    cursor.execute("SELECT SUM(file_size) FROM media_files")
    total_size = cursor.fetchone()[0] or 0
    
    # Get size by file type
    cursor.execute("SELECT file_type, SUM(file_size) FROM media_files GROUP BY file_type")
    type_sizes = cursor.fetchall()
    
    # Display statistics
    logger.info("=" * 50)
    logger.info("DATABASE STATISTICS")
    logger.info("=" * 50)
    logger.info(f"Total files in database: {total_count}")
    logger.info(f"Total size: {total_size / (1024*1024*1024):.2f} GB")
    
    logger.info("\nFiles by type:")
    for file_type, count in type_counts:
        logger.info(f"  {file_type}: {count} files")
    
    logger.info("\nTop 10 extensions:")
    for extension, count in extension_counts:
        logger.info(f"  {extension}: {count} files")
    
    logger.info("\nSize by type:")
    for file_type, size in type_sizes:
        if size:
            logger.info(f"  {file_type}: {size / (1024*1024*1024):.2f} GB")
    
    logger.info("\nScan statistics:")
    logger.info(f"  Files scanned: {scan_stats['total_files']}")
    logger.info(f"  Files updated: {scan_stats['updated_files']}")
    logger.info(f"  Scan duration: {scan_stats['duration']:.2f} seconds")
    logger.info("=" * 50)
    
    conn.close()

if __name__ == "__main__":
    # Parse command line arguments to get config path
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--config', '-g')
    args, _ = parser.parse_known_args()
    
    # Start main function with config path
    main(args.config)