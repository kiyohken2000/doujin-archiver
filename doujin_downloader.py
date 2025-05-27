import os
import re
import time
import logging
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import pickle
from datetime import datetime

# ロギングの設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('doujin_downloader.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# 設定
BASE_URL = 'https://ddd-smart.net'
TOP_URL = 'https://ddd-smart.net/'  # トップページを使用
DOWNLOAD_DIR = 'downloads'  # ダウンロードディレクトリ
HISTORY_FILE = 'downloaded_history.pkl'  # ダウンロード履歴ファイル

# セッション作成
session = requests.Session()
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}
session.headers.update(headers)

# ダウンロードディレクトリの作成
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

def load_download_history():
    """ダウンロード履歴を読み込む"""
    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE, 'rb') as f:
            return pickle.load(f)
    return set()

def save_download_history(history):
    """ダウンロード履歴を保存する"""
    with open(HISTORY_FILE, 'wb') as f:
        pickle.dump(history, f)

def clean_filename(filename):
    """ファイル名に使えない文字を半角スペースまたは全角に置き換える"""
    # コロンは全角に変換
    filename = filename.replace(':', '：')
    # その他の使用できない文字は半角スペースに変換
    return re.sub(r'[\\/*?"<>|]', ' ', filename)

def get_today_items():
    """今日の更新アイテムのURLを取得する"""
    try:
        logging.info("トップページにアクセスしています...")
        response = session.get(TOP_URL)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 今日の日付を取得 (YYYYMMDD形式)
        today_date = datetime.now().strftime('%Y%m%d')
        today_formatted = datetime.now().strftime('%Y年%m月%d日')
        logging.info(f"今日の日付: {today_date}")
        logging.info(f"検索する日付形式: {today_formatted}")
        
        items = []
        
        # 正確なクラス名でh2ヘッダーを検索
        headers = soup.find_all('h2', class_='card-panel white-text blue accent-2')
        logging.info(f"見つかったヘッダー数: {len(headers)}")
        
        today_header = None
        for i, header in enumerate(headers):
            header_text = header.get_text().strip()
            logging.info(f"ヘッダー{i+1}: '{header_text}'")
            if today_formatted in header_text and '更新同人誌' in header_text:
                today_header = header
                logging.info(f"今日の更新セクションを発見: {header_text}")
                break
        
        if today_header:
            # 今日の更新セクション内の項目を取得
            # ヘッダーの後の div.list-all を探す
            current_element = today_header
            while current_element:
                current_element = current_element.find_next_sibling()
                if current_element and current_element.name == 'div':
                    if 'list-all' in current_element.get('class', []):
                        today_items = current_element.select('ul.package-list li a.pop_separate')
                        logging.info(f"今日の更新セクション内でリンクを{len(today_items)}件発見")
                        
                        for item in today_items:
                            href = item.get('href')
                            if href and '/doujinshi3/show-m.php' in href:
                                full_url = urljoin(BASE_URL, href)
                                items.append(full_url)
                                logging.info(f"今日の更新URLを追加: {full_url}")
                        break
                elif current_element and current_element.name == 'h2':
                    # 次のセクションに到達したら終了
                    break
        
        # セクションが見つからない場合は、トップページから今日の日付を含むURLを検索
        if not items:
            logging.info("セクション検索で見つからないため、今日の日付でフィルタリングして検索します...")
            all_links = soup.select('a.pop_separate[href*="/doujinshi3/show-m.php"]')
            logging.info(f"全体で{len(all_links)}件のリンクを発見")
            
            # 最初の10件のURLをデバッグ用に表示
            for i, item in enumerate(all_links[:10]):
                href = item.get('href')
                logging.info(f"URL例{i+1}: {href}")
            
            for item in all_links:
                href = item.get('href')
                if href and f'g={today_date}' in href:
                    full_url = urljoin(BASE_URL, href)
                    items.append(full_url)
                    logging.info(f"今日の日付のURLを発見: {full_url}")
        
        # 重複を除去
        items = list(set(items))
        
        logging.info(f'今日の更新アイテム: {len(items)}件')
        for i, item in enumerate(items[:5]):
            logging.info(f'  {i+1}. {item}')
            
        return items
    except Exception as e:
        logging.error(f'今日の更新アイテム取得エラー: {e}')
        return []

def get_dl_page_url(detail_page_url):
    """詳細ページからDLページのURLを取得する"""
    try:
        logging.info(f"詳細ページにアクセスしています: {detail_page_url}")
        response = session.get(detail_page_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # DLページへのリンクを取得
        dl_link = None
        dl_buttons = soup.select('a[href*="dl-m-m.php"]')
        for button in dl_buttons:
            if 'DLページ' in button.text:
                dl_link = urljoin(BASE_URL, button.get('href'))
                logging.info(f"DLページへのリンクを発見: {dl_link}")
                return dl_link
        
        logging.error(f"DLページへのリンクが見つかりません: {detail_page_url}")
        return None
    except Exception as e:
        logging.error(f"詳細ページ取得エラー ({detail_page_url}): {e}")
        return None

def get_pdf_url(dl_page_url):
    """DLページからPDFのURLを取得する"""
    try:
        logging.info(f"DLページにアクセスしています: {dl_page_url}")
        response = session.get(dl_page_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # PDFダウンロードリンクを取得
        pdf_links = soup.select('a.pop_dl-btn[href*=".pdf"]')
        if pdf_links:
            pdf_url = pdf_links[0].get('href')
            logging.info(f"PDFのURLを発見: {pdf_url}")
            return pdf_url
        
        logging.error(f"PDFリンクが見つかりません: {dl_page_url}")
        return None
    except Exception as e:
        logging.error(f"DLページ取得エラー ({dl_page_url}): {e}")
        return None

def get_item_details(url):
    """アイテムの詳細情報を取得する"""
    try:
        logging.info(f"詳細情報を取得しています: {url}")
        response = session.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # タイトルを取得
        title_element = soup.select_one('h1.list-pickup-header')
        if title_element:
            title = title_element.text.strip()
        else:
            title_element = soup.select_one('div.blue > h2')
            title = title_element.text.strip() if title_element else "Unknown Title"
        
        logging.info(f"タイトル: {title}")
        
        # 原作情報を取得
        original_work = ""
        original_elements = soup.select('div.detail-box:has(span.anime-icon) div.foot-box a')
        if original_elements:
            original_work = original_elements[0].text.strip()
            logging.info(f"原作: {original_work}")
        
        # サークル名を取得
        circle = ""
        circle_elements = soup.select('div.detail-box:has(span.circle-icon) div.foot-box a')
        if circle_elements:
            circle = circle_elements[0].text.strip()
            logging.info(f"サークル: {circle}")
        
        # イベント名（タグ）を取得
        event_name = ""
        tag_elements = soup.select('div.detail-box:has(span.tag-icon) div.foot-box a')
        for tag in tag_elements:
            tag_text = tag.text.strip()
            # コミケ関連のタグを見つけた場合
            if re.search(r'(C\d+|コミケ|COMIC|コミックマーケット|冬コミ|夏コミ)', tag_text):
                # Cxxx形式のイベント番号を抽出
                c_match = re.search(r'C(\d+)', tag_text)
                if c_match:
                    # 既にCxxx形式の場合はそのまま使用
                    event_name = f"C{c_match.group(1)}"
                else:
                    # コミケの年と季節から番号を推定
                    year_match = re.search(r'20(\d{2})', tag_text)
                    if year_match and ('夏' in tag_text or '冬' in tag_text):
                        year = int(year_match.group(1))
                        # 夏コミはC9x, C10x, ..., 冬コミはC9x+1, C10x+1, ...
                        if '夏' in tag_text:
                            if year >= 22:  # 2022年以降
                                event_name = f"C{100 + (year - 22) * 2}"
                            else:
                                event_name = f"C{96 + (year - 19) * 2}"  # 2019年C96から
                        elif '冬' in tag_text:
                            if year >= 22:  # 2022年以降
                                event_name = f"C{101 + (year - 22) * 2}"
                            else:
                                event_name = f"C{97 + (year - 19) * 2}"  # 2019年C97から
                    else:
                        # 番号が推定できない場合はタグをそのまま使用
                        event_name = tag_text
                
                logging.info(f"イベント名: {event_name}")
                break
                
        # 更新日と発行日を取得
        update_date = ""
        publish_date = ""
        
        update_elements = soup.select('div.detail-box:has(span.upload-day-icon) div.foot-box a')
        if update_elements:
            update_date = update_elements[0].text.strip()
            logging.info(f"更新日: {update_date}")
        
        publish_elements = soup.select('div.detail-box:has(span.issue-day-icon) div.foot-box')
        if publish_elements:
            publish_date = publish_elements[0].text.strip()
            logging.info(f"発行日: {publish_date}")
        
        return {
            'title': title,
            'original_work': original_work,
            'circle': circle,
            'event_name': event_name,
            'update_date': update_date,
            'publish_date': publish_date,
            'page_url': url
        }
    except Exception as e:
        logging.error(f'アイテム詳細取得エラー ({url}): {e}')
        return None

def download_pdf(item, dl_page_url):
    """PDFをダウンロードする"""
    try:
        # PDFのURLを取得
        pdf_url = get_pdf_url(dl_page_url)
        if not pdf_url:
            logging.error(f'PDFのURLが取得できません: {item["title"]}')
            return False
        
        # ファイル名を生成
        filename = generate_filename(item)
        filepath = os.path.join(DOWNLOAD_DIR, filename)
        
        # PDFをダウンロード
        logging.info(f'PDFダウンロード開始: {pdf_url}')
        pdf_response = session.get(pdf_url, stream=True)
        pdf_response.raise_for_status()
        
        with open(filepath, 'wb') as f:
            for chunk in pdf_response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        logging.info(f'ダウンロード成功: {filename}')
        return True
    except Exception as e:
        logging.error(f'ダウンロードエラー ({item["title"]}): {e}')
        return False

def generate_filename(item):
    """ファイル名を生成する"""
    # ファイル名の各パートがない場合のデフォルト値を設定
    circle = item['circle'] if item['circle'] else "不明サークル"
    original_work = item['original_work'] if item['original_work'] else "不明作品"
    
    # イベント名がある場合: ({イベント名})[{サークル名}]{原作}.pdf
    if item['event_name']:
        filename = f"({item['event_name']})[{circle}]{original_work}.pdf"
    # イベント名がない場合は発行日を使用: [{サークル名}]{原作}{発行日}.pdf
    elif item['publish_date']:
        filename = f"[{circle}]{original_work} {item['publish_date']}.pdf"
    # 発行日もない場合は更新日を使用: [{サークル名}]{原作}{更新日}.pdf
    else:
        update_date = item['update_date'] if item['update_date'] else ""
        filename = f"[{circle}]{original_work} {update_date}.pdf"
    
    return clean_filename(filename)

def main():
    """メイン処理"""
    logging.info('ダウンロード処理を開始します')
    
    # ダウンロード履歴を読み込む
    downloaded_history = load_download_history()
    
    # 今日の更新アイテムを取得
    today_items = get_today_items()
    
    if not today_items:
        logging.error('今日の更新アイテムが見つかりませんでした')
        return
    
    # 各アイテムを処理
    new_downloads = 0
    for url in today_items:
        # 既にダウンロード済みかチェック
        if url in downloaded_history:
            logging.info(f'スキップ (既にダウンロード済み): {url}')
            continue
        
        logging.info(f'処理中: {url}')
        
        # アイテムの詳細情報を取得
        item = get_item_details(url)
        if not item:
            continue
        
        # DLページのURLを取得
        dl_page_url = get_dl_page_url(url)
        if not dl_page_url:
            logging.error(f'DLページのURLが取得できません: {item["title"]}')
            continue
        
        # PDFをダウンロード
        success = download_pdf(item, dl_page_url)
        if success:
            downloaded_history.add(url)
            new_downloads += 1
        
        # サーバーに負荷をかけないように少し待機
        time.sleep(3)
    
    # ダウンロード履歴を保存
    save_download_history(downloaded_history)
    
    logging.info(f'処理完了: {new_downloads}件の新規ダウンロード')

if __name__ == '__main__':
    main()