# MediaMetaDB

## Overview
MediaMetaDB is a powerful tool for extracting and managing metadata from media files (images and videos). It scans specified directories, extracts metadata such as creation time, modification time, capture time, and duration (for videos), and stores this information in a SQLite database. The tool is designed to be efficient, only processing new or modified files on subsequent runs, making it ideal for managing large collections of media files.

## Installation

### Prerequisites
- Python 3.8 or higher
- Git

### Steps
1. Clone the repository:
   ```
   git clone https://github.com/daishir0/MediaMetaDB.git
   cd MediaMetaDB
   ```

2. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```

3. Create a configuration file:
   ```
   cp config.sample.yaml config.yaml
   ```
   
4. (Optional) Edit the configuration file to customize settings:
   ```
   # Edit config.yaml to set your preferred timezone, database settings, etc.
   ```

## Usage

### Basic Usage
```
python get_media_data.py /path/to/photos
```

### Command Line Options
- `--verbose`, `-v`: Enable detailed logging
- `--threads`, `-t`: Specify the number of worker threads (default: CPU count)
- `--extensions`, `-e`: Add custom file extensions (comma-separated)
- `--media-only`, `-m`: Scan only media files (images and videos)
- `--export-csv`, `-c`: Export database to CSV file
- `--stats`, `-s`: Show detailed statistics after scanning
- `--force`, `-f`: Force reprocessing of all files, including timezone conversion
- `--force-all-dates`, `-a`: Force update of all date formats (use with `--force`)
- `--strict-dates`, `--sd`: Use strict date validation
- `--db-path`, `-d`: Specify SQLite database file path
- `--config`, `-g`: Specify configuration file path

### Examples

#### Scan only media files
```
python get_media_data.py /path/to/photos --media-only
```

#### Add custom extensions
```
python get_media_data.py /path/to/photos --extensions "raw,cr2,arw"
```

#### Show detailed statistics
```
python get_media_data.py /path/to/photos --stats
```

#### Export database to CSV
```
python get_media_data.py /path/to/photos --export-csv /path/to/export.csv
```

#### Force reprocessing all files
```
python get_media_data.py /path/to/photos --force
```

## Database Structure
The SQLite database (`data.db`) contains a single table `media_files` with the following columns:

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER | Primary key (auto-increment) |
| full_path | TEXT | Full path to the file (unique) |
| file_name | TEXT | File name |
| file_type | TEXT | File type (image, video, other) |
| file_extension | TEXT | File extension |
| file_size | INTEGER | File size in bytes |
| file_creation_time | TEXT | File creation time (ISO 8601 format with timezone) |
| file_modification_time | TEXT | File modification time (ISO 8601 format with timezone) |
| capture_time | TEXT | Capture time from metadata (ISO 8601 format with timezone) |
| duration | REAL | Video duration in seconds |
| error_message | TEXT | Error message if processing failed |
| last_updated | TEXT | Last update time (ISO 8601 format with timezone) |
| file_hash | TEXT | File hash for change detection |
| processed | INTEGER | Processing status (1=success, 0=error) |

## Notes
- All date/time values are stored in ISO 8601 format with timezone information
- For iOS device photos with no metadata, the capture time is extracted from the filename
- The tool uses multiple libraries to extract metadata, ensuring maximum compatibility
- HEIC/HEIF files require additional libraries for full metadata extraction
- The tool creates a log file for each run in the format `YYYYMMDD-HHMMSS.log`
- Configuration can be customized using a YAML file (`config.yaml`)
- A sample configuration file (`config.sample.yaml`) is provided as a template

## License
This project is licensed under the MIT License - see the LICENSE file for details.

---

# MediaMetaDB

## 概要
MediaMetaDBは、メディアファイル（画像や動画）からメタデータを抽出して管理するための強力なツールです。指定されたディレクトリをスキャンし、作成日時、更新日時、撮影日時、動画の長さなどのメタデータを抽出して、SQLiteデータベースに保存します。このツールは効率的に設計されており、再実行時には新規または変更されたファイルのみを処理するため、大量のメディアファイルを管理するのに最適です。

## インストール方法

### 前提条件
- Python 3.8以上
- Git

### 手順
1. リポジトリをクローンします：
   ```
   git clone https://github.com/daishir0/MediaMetaDB.git
   cd MediaMetaDB
   ```

2. 必要な依存関係をインストールします：
   ```
   pip install -r requirements.txt
   ```

3. 設定ファイルを作成します：
   ```
   cp config.sample.yaml config.yaml
   ```
   
4. （オプション）設定ファイルを編集してカスタマイズします：
   ```
   # config.yamlを編集して、タイムゾーン、データベース設定などを設定します
   ```

## 使い方

### 基本的な使い方
```
python get_media_data.py /path/to/photos
```

### コマンドラインオプション
- `--verbose`, `-v`: 詳細なログ出力を有効にする
- `--threads`, `-t`: ワーカースレッド数を指定（デフォルト：CPU数）
- `--extensions`, `-e`: カスタムファイル拡張子を追加（カンマ区切り）
- `--media-only`, `-m`: メディアファイル（画像・動画）のみをスキャン
- `--export-csv`, `-c`: データベースをCSVファイルにエクスポート
- `--stats`, `-s`: スキャン後に詳細な統計情報を表示
- `--force`, `-f`: すべてのファイルを強制的に再処理（タイムゾーン変換を含む）
- `--force-all-dates`, `-a`: すべての日付形式を強制的に更新（`--force`と併用）
- `--strict-dates`, `--sd`: 厳密な日付検証を使用
- `--db-path`, `-d`: SQLiteデータベースファイルのパスを指定
- `--config`, `-g`: 設定ファイルのパスを指定

### 使用例

#### メディアファイルのみをスキャン
```
python get_media_data.py /path/to/photos --media-only
```

#### カスタム拡張子を追加
```
python get_media_data.py /path/to/photos --extensions "raw,cr2,arw"
```

#### 詳細な統計情報を表示
```
python get_media_data.py /path/to/photos --stats
```

#### データベースをCSVにエクスポート
```
python get_media_data.py /path/to/photos --export-csv /path/to/export.csv
```

#### すべてのファイルを強制的に再処理
```
python get_media_data.py /path/to/photos --force
```

## データベース構造
SQLiteデータベース（`data.db`）には、以下のカラムを持つ`media_files`テーブルが含まれています：

| カラム | 型 | 説明 |
|--------|------|-------------|
| id | INTEGER | 主キー（自動採番） |
| full_path | TEXT | ファイルのフルパス（一意） |
| file_name | TEXT | ファイル名 |
| file_type | TEXT | ファイルタイプ（image, video, other） |
| file_extension | TEXT | ファイル拡張子 |
| file_size | INTEGER | ファイルサイズ（バイト） |
| file_creation_time | TEXT | ファイル作成日時（ISO 8601形式、タイムゾーン付き） |
| file_modification_time | TEXT | ファイル更新日時（ISO 8601形式、タイムゾーン付き） |
| capture_time | TEXT | メタデータから取得した撮影日時（ISO 8601形式、タイムゾーン付き） |
| duration | REAL | 動画の長さ（秒） |
| error_message | TEXT | 処理中にエラーが発生した場合のエラーメッセージ |
| last_updated | TEXT | データ取得・更新日時（ISO 8601形式、タイムゾーン付き） |
| file_hash | TEXT | 変更検出用のファイルハッシュ |
| processed | INTEGER | 処理状態（1=成功, 0=エラー） |

## 注意点
- すべての日時はISO 8601形式でタイムゾーン情報を含めて保存されます
- メタデータのないiOSデバイスの写真の場合、ファイル名から撮影日時が抽出されます
- このツールは複数のライブラリを使用してメタデータを抽出し、最大限の互換性を確保しています
- HEIC/HEIFファイルの完全なメタデータ抽出には追加のライブラリが必要です
- 実行ごとに`YYYYMMDD-HHMMSS.log`形式のログファイルが作成されます
- YAMLファイル（`config.yaml`）を使用して設定をカスタマイズできます
- サンプル設定ファイル（`config.sample.yaml`）がテンプレートとして提供されています

## ライセンス
このプロジェクトはMITライセンスの下でライセンスされています。詳細はLICENSEファイルを参照してください。