#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
export_media.py - メディアファイルデータベース検索・エクスポートツール
==================================================================

概要:
----
このスクリプトは、get_media_data.pyで作成されたSQLiteデータベースから、
指定された日付に基づいてメディアファイルを検索し、結果を表示します。
--exportオプションを使用すると、ファイルを./exportディレクトリにコピーし、
ファイル名を撮影日時（capture_time）に基づいてリネームします。

主な機能:
-------
- 指定された日付のメディアファイルをデータベースから検索
- 撮影日時の昇順で結果を表示
- ファイルを./exportディレクトリにコピー（--exportオプション使用時）
- ファイル名を撮影日時（yyyymmdd-hhmmss形式）に変更
- 動画の撮影時間内に撮影された静止画ファイルに"-include"を付加
- 重複ファイル名の自動処理

コマンドラインオプション:
--------------------
--export            ファイルを./exportディレクトリにコピーし、撮影日時でリネーム
--clean             エクスポート前に./exportディレクトリをクリーンアップ
<dates>             検索対象の日付（YYYY-MM-DD形式、複数指定可）

使用例:
-----
# 基本的な使い方（指定日付のファイルを検索して表示）
python export_media.py 2025-03-22 2025-03-23 2025-03-24

# ファイルをエクスポートする（撮影日時でリネーム）
python export_media.py 2025-03-22 2025-03-23 2025-03-24 --export

# エクスポート前にexportディレクトリをクリーンアップ
python export_media.py 2025-03-22 2025-03-23 2025-03-24 --export --clean

出力:
----
- 指定された日付に一致するメディアファイルの撮影日時とパスのリスト
- 動画ファイルの撮影時間範囲の情報
- エクスポートされたファイルの情報（--exportオプション使用時）
"""

import os
import sys
import shutil
import datetime
import sqlite3
import argparse
from pathlib import Path
from typing import List, Tuple


def get_media_by_dates(db_path: str, dates: List[str]) -> List[Tuple[str, str, str, float]]:
    """
    Get media files from database by dates.
    
    Args:
        db_path: Path to the SQLite database
        dates: List of dates in YYYY-MM-DD format
        
    Returns:
        List of tuples containing (capture_time, full_path, file_type, duration)
    """
    # Connect to the database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Build the SQL query with placeholders for each date
    placeholders = ', '.join(['?' for _ in dates])
    like_conditions = []
    params = []
    
    # Create LIKE conditions for each date
    for date in dates:
        like_conditions.append("capture_time LIKE ?")
        params.append(f"{date}%")
    
    # Combine conditions with OR
    where_clause = " OR ".join(like_conditions)
    
    # Execute the query
    query = f"""
    SELECT capture_time, full_path, file_type, duration
    FROM media_files
    WHERE capture_time IS NOT NULL AND ({where_clause})
    ORDER BY capture_time ASC
    """
    
    cursor.execute(query, params)
    results = cursor.fetchall()
    
    # Close the connection
    conn.close()
    
    return results


def get_video_time_ranges(media_files: List[Tuple[str, str, str, float]]) -> List[Tuple[datetime.datetime, datetime.datetime, str]]:
    """
    Extract time ranges for video files.
    
    Args:
        media_files: List of tuples containing (capture_time, full_path, file_type, duration)
        
    Returns:
        List of tuples containing (start_time, end_time, video_path)
    """
    video_ranges = []
    
    for capture_time, full_path, file_type, duration in media_files:
        # Only process video files with valid duration
        if file_type == 'video' and duration is not None and duration > 0:
            try:
                # Parse the capture time
                start_time = datetime.datetime.fromisoformat(capture_time)
                
                # Calculate end time by adding duration (in seconds)
                end_time = start_time + datetime.timedelta(seconds=duration)
                
                # Add to the list of video ranges
                video_ranges.append((start_time, end_time, full_path))
                print(f"  Video time range: {full_path} - {start_time.isoformat()} to {end_time.isoformat()} (duration: {duration}s)")
            except Exception as e:
                print(f"Error processing video time range for {full_path}: {e}")
    
    return video_ranges


def is_within_video_duration(image_time: datetime.datetime, video_ranges: List[Tuple[datetime.datetime, datetime.datetime, str]]) -> bool:
    """
    Check if an image was captured within the duration of any video.
    
    Args:
        image_time: Capture time of the image
        video_ranges: List of tuples containing (start_time, end_time, video_path)
        
    Returns:
        True if the image was captured within any video duration, False otherwise
    """
    for start_time, end_time, video_path in video_ranges:
        print(f"  Checking if {image_time.isoformat()} is between {start_time.isoformat()} and {end_time.isoformat()}")
        if start_time <= image_time <= end_time:
            print(f"  → Image captured during video: {video_path} ({start_time} to {end_time})")
            return True
    
    return False


def main():
    """Main function."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Export media files from database by date')
    parser.add_argument('--export', action='store_true', help='Export files to ./export directory with renamed filenames')
    parser.add_argument('--clean', action='store_true', help='Clean export directory before exporting files')
    parser.add_argument('dates', nargs='+', help='Dates in YYYY-MM-DD format')
    args = parser.parse_args()
    
    # Validate dates format
    for date in args.dates:
        if not date.strip():
            print(f"Error: Empty date provided")
            sys.exit(1)
        
        parts = date.split('-')
        if len(parts) != 3:
            print(f"Error: Invalid date format: {date}. Expected format: YYYY-MM-DD")
            sys.exit(1)
        
        try:
            year, month, day = parts
            if not (len(year) == 4 and len(month) == 2 and len(day) == 2):
                raise ValueError("Invalid date components")
            if not (year.isdigit() and month.isdigit() and day.isdigit()):
                raise ValueError("Date components must be numeric")
            if not (1 <= int(month) <= 12 and 1 <= int(day) <= 31):
                raise ValueError("Invalid month or day")
        except ValueError as e:
            print(f"Error: Invalid date format: {date}. {e}")
            sys.exit(1)
    
    # Get the database path (current directory)
    db_path = Path.cwd() / "data.db"
    
    # Check if the database file exists
    if not db_path.exists():
        print(f"Error: Database file not found: {db_path}")
        sys.exit(1)
    
    # Get media files by dates
    results = get_media_by_dates(str(db_path), args.dates)
    
    # Create export directory if --export is specified
    if args.export:
        export_dir = Path.cwd() / "export"
        if not export_dir.exists():
            export_dir.mkdir()
        elif args.clean:
            # Clean export directory if --clean is specified
            print(f"Cleaning export directory: {export_dir}")
            for file_path in export_dir.glob('*'):
                if file_path.is_file():
                    file_path.unlink()
                    print(f"  Deleted: {file_path}")
        print(f"Export directory: {export_dir}")
    
    # Print the results
    if not results:
        print(f"No media files found for the specified dates: {', '.join(args.dates)}")
    else:
        print(f"Found {len(results)} media files for dates: {', '.join(args.dates)}")
        print("\nCapture Time                   | Full Path")
        print("-" * 80)
        
        # Extract video time ranges
        video_ranges = get_video_time_ranges(results)
        print(f"Found {len(video_ranges)} video files with duration information")
        
        # Print image files for reference
        image_files = [(ct, fp) for ct, fp, ft, _ in results if ft == 'image']
        print(f"Found {len(image_files)} image files to check against video durations")
        
        for capture_time, full_path, file_type, duration in results:
            print(f"{capture_time} | {full_path}")
            
            # Export files if --export is specified
            if args.export:
                try:
                    # Parse capture_time to create new filename
                    if capture_time and 'T' in capture_time:
                        # Parse ISO 8601 format (2025-03-24T12:35:38+09:00)
                        dt = datetime.datetime.fromisoformat(capture_time)
                        new_filename = dt.strftime('%Y%m%d-%H%M%S')
                        
                        # Check if this is an image file captured during a video
                        is_image = file_type == 'image'
                        if is_image:
                            print(f"Checking if image {full_path} was captured during a video...")
                            if is_within_video_duration(dt, video_ranges):
                                print(f"  → Image was captured during a video, adding -include suffix")
                                new_filename += "-include"
                            else:
                                print(f"  → Image was NOT captured during any video")
                        else:
                            print(f"  → Not an image file, skipping -include check")
                    else:
                        # Fallback to original filename if capture_time is not in expected format
                        new_filename = Path(full_path).stem
                    
                    # Get original file extension
                    original_path = Path(full_path)
                    extension = original_path.suffix
                    
                    # Create new path
                    new_path = export_dir / f"{new_filename}{extension}"
                    
                    # Handle duplicate filenames
                    counter = 1
                    while new_path.exists():
                        new_path = export_dir / f"{new_filename}-{counter}{extension}"
                        counter += 1
                    
                    # Copy file
                    shutil.copy2(full_path, new_path)
                    print(f"  → Exported to: {new_path}")
                except Exception as e:
                    print(f"  → Error exporting file: {e}")


if __name__ == "__main__":
    main()