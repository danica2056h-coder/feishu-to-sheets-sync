import os
import json
import re
import requests
import gspread
import time
from datetime import datetime
import pytz

FEISHU_TOKEN_URL = 'https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal'
FEISHU_BASE_URL = 'https://open.feishu.cn/open-apis/bitable/v1/apps'
MAX_SYNC_COL = 60

def get_feishu_token(app_id, app_secret):
    res = requests.post(FEISHU_TOKEN_URL, json={"app_id": app_id, "app_secret": app_secret})
    return res.json().get('tenant_access_token')

def extract_id_from_url(url):
    match = re.search(r'/d/([a-zA-Z0-9-_]+)', url)
    return match.group(1) if match else None

def parse_feishu_url(url):
    app_token = re.search(r'(?:base|wiki)/([a-zA-Z0-9]+)', url)
    table_id = re.search(r'table=([a-zA-Z0-9]+)', url)
    view_id = re.search(r'view=([a-zA-Z0-9]+)', url)
    return {"appToken": app_token.group(1) if app_token else None,
            "tableId": table_id.group(1) if table_id else None,
            "viewId": view_id.group(1) if view_id else None}

def fetch_feishu_data(params, token):
    headers = {'Authorization': f'Bearer {token}'}
    fields_url = f"{FEISHU_BASE_URL}/{params['appToken']}/tables/{params['tableId']}/fields?view_id={params['viewId']}&page_size=100"
    fields_res = requests.get(fields_url, headers=headers).json()
    items = fields_res.get('data', {}).get('items', [])
    header_names = [i['field_name'] for i in items if not i.get('is_hidden')][:MAX_SYNC_COL]
    
    all_values, page_token, has_more = [header_names], "", True
    while has_more:
        data_url = f"{FEISHU_BASE_URL}/{params['appToken']}/tables/{params['tableId']}/records?page_size=500&view_id={params['viewId']}"
        if page_token: data_url += f"&page_token={page_token}"
        data_res = requests.get(data_url, headers=headers).json()
        records = data_res.get('data', {}).get('items', [])
        for record in records:
            row_data = []
            fields = record.get('fields', {})
            for h in header_names:
                val = fields.get(h, "")
                if isinstance(val, list):
                    val = ", ".join([str(v.get('text', v.get('name', v))) if isinstance(v, dict) else str(v) for v in val])
                elif isinstance(val, dict):
                    val = str(val.get('text', val.get('name', val)))
                row_data.append(str(val))
            all_values.append(row_data)
        has_more = data_res.get('data', {}).get('has_more', False)
        page_token = data_res.get('data', {}).get('page_token', "")
    return all_values

def main():
    gc = gspread.service_account_from_dict(json.loads(os.environ['GOOGLE_CREDENTIALS']))
    token = get_feishu_token(os.environ['FEISHU_APP_ID'], os.environ['FEISHU_APP_SECRET'])
    tz = pytz.timezone('Asia/Shanghai')
    
    master_ws = gc.open_by_key(os.environ['MASTER_SHEET_ID']).get_worksheet(0)
    sub_urls = master_ws.col_values(1)[1:] 
    
    for url in sub_urls:
        sub_id = extract_id_from_url(url)
        if not sub_id: continue
        try:
            doc = gc.open_by_key(sub_id)
            summary_ws = doc.worksheet("汇总表")
            rows = summary_ws.get_all_values()
            for i, row in enumerate(rows[1:], start=2):
                if len(row) < 2: continue
                fs_url, target = row[0], row[1]
                if not fs_url or not target: continue
                try:
                    data = fetch_feishu_data(parse_feishu_url(fs_url), token)
                    try:
                        target_ws = doc.worksheet(target)
                    except:
                        target_ws = doc.add_worksheet(title=target, rows=100, cols=MAX_SYNC_COL)
                    target_ws.clear()
                    target_ws.update(data, "A1")
                    summary_ws.update_cell(i, 4, f"✅ 云端成功 ({len(data)-1}条)")
                    summary_ws.update_cell(i, 5, datetime.now(tz).strftime("%Y-%m-%d %H:%M"))
                except Exception as e:
                    summary_ws.update_cell(i, 4, f"❌ 错误: {str(e)[:15]}")
                time.sleep(1) 
        except Exception as e: print(f"无法访问子表 {sub_id}: {e}")

if __name__ == "__main__":
    main()
