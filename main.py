import os, json, time, requests, re
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta

# 从环境变量读取 Secrets
MASTER_ID = os.environ.get("MASTER_SHEET_ID")

def get_gc():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds_json = os.environ.get("GOOGLE_CREDENTIALS")
    if not creds_json:
        raise Exception("GOOGLE_CREDENTIALS Secrets 为空，请检查配置")
    creds = ServiceAccountCredentials.from_json_keyfile_dict(json.loads(creds_json), scope)
    return gspread.authorize(creds)

def extract_sheet_id(url):
    match = re.search(r"/d/([a-zA-Z0-9-_]+)", url)
    return match.group(1) if match else url

def parse_feishu_url(url):
    """从飞书链接提取 app_token 和 table_id"""
    app_token_match = re.search(r"base/([a-zA-Z0-9]+)", url)
    table_id_match = re.search(r"table=([a-zA-Z0-9]+)", url)
    return (app_token_match.group(1) if app_token_match else None, 
            table_id_match.group(1) if table_id_match else None)

def sync_sub_sheet(gc, master_sheet, row_idx, mode_tag):
    start_time = time.time()
    # A列：副本链接
    row_data = master_sheet.row_values(row_idx)
    sub_url = row_data[0]
    
    try:
        sub_id = extract_sheet_id(sub_url)
        sub_ss = gc.open_by_key(sub_id)
        # 假设副本里第一个 Sheet 叫“汇总表”或就是第1页
        sub_main_ws = sub_ss.get_worksheet(0) 
        sub_rows = sub_main_ws.get_all_values()

        app_id, app_secret = os.environ.get("FEISHU_APP_ID"), os.environ.get("FEISHU_APP_SECRET")
        auth_res = requests.post("https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal", 
                                 json={"app_id": app_id, "app_secret": app_secret})
        fs_token = auth_res.json().get("tenant_access_token")

        # 遍历副本内 A3 开始的行
        for i, r in enumerate(sub_rows[2:], start=3):
            fs_url, target_tab = r[0], r[1]
            if not fs_url or "feishu" not in fs_url: continue
            
            app_token, table_id = parse_feishu_url(fs_url)
            if not app_token or not table_id: continue

            # 抓取并写入
            res = requests.get(f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records",
                               headers={"Authorization": f"Bearer {fs_token}"}, params={"page_size": 500})
            items = res.json().get('data', {}).get('items', [])
            
            if items:
                headers = list(items[0]['fields'].keys())
                output = [headers] + [[str(it['fields'].get(k, "")) for k in headers] for it in items]
                try:
                    ws = sub_ss.worksheet(target_tab)
                except:
                    ws = sub_ss.add_worksheet(title=target_tab, rows="1000", cols="20")
                ws.clear()
                ws.update('A1', output)

        # 回写总控状态
        duration = f"{time.time() - start_time:.2f}s"
        bj_now = (datetime.utcnow() + timedelta(hours=8)).strftime('%H:%M')
        master_sheet.update_cell(row_idx, 4, f"{mode_tag}-成功")
        master_sheet.update_cell(row_idx, 5, bj_now)
        master_sheet.update_cell(row_idx, 6, duration)
        master_sheet.update_cell(row_idx, 3, False)

    except Exception as e:
        master_sheet.update_cell(row_idx, 4, f"错误:{str(e)[:15]}")
        master_sheet.update_cell(row_idx, 3, False)

def main():
    payload = json.loads(os.environ.get('PAYLOAD', '{}'))
    gc = get_gc()
    master_sheet = gc.open_by_key(MASTER_ID).get_worksheet(0)

    if payload and payload.get('priority') == "1_MANUAL":
        sync_sub_sheet(gc, master_sheet, payload['row'], "手触")
    else:
        # 遍历总控 150 行
        for i in range(3, 153): # A3开始
            sync_sub_sheet(gc, master_sheet, i, "定时")

if __name__ == "__main__":
    main()
