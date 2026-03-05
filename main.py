import os, json, time, requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta

# 基础 ID
MASTER_ID = "1X7yDRVlOgG42flnSuki7BUF68kbO0cgn5GF-wu8g9cw"

def get_gc():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_dict(json.loads(os.environ.get("G_SERVICE_ACCOUNT")), scope)
    return gspread.authorize(creds)

def sync_row(gc, master_sheet, row_idx, mode_tag):
    start_time = time.time()
    row_data = master_sheet.row_values(row_idx)
    # 假设：A:表名, B:副本ID, G:飞书Token, H:飞书TableID
    row_data += [""] * (10 - len(row_data))
    
    try:
        # 1. 抓取飞书
        app_id, app_secret = os.environ.get("FEISHU_APP_ID"), os.environ.get("FEISHU_APP_SECRET")
        auth_res = requests.post("https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal", json={"app_id": app_id, "app_secret": app_secret})
        fs_token = auth_res.json().get("tenant_access_token")
        
        # 请求 Bitable API
        res = requests.get(f"https://open.feishu.cn/open-apis/bitable/v1/apps/{row_data[6]}/tables/{row_data[7]}/records",
                           headers={"Authorization": f"Bearer {fs_token}"}, params={"page_size": 500})
        items = res.json().get('data', {}).get('items', [])
        if not items: raise Exception("无数据/404")

        # 2. 写入谷歌
        headers = list(items[0]['fields'].keys())
        output = [headers] + [[str(i['fields'].get(k, "")) for k in headers] for i in items]
        
        target_ss = gc.open_by_key(row_data[1])
        ws = target_ss.worksheet(row_data[0])
        ws.clear()
        ws.update('A1', output)

        # 3. 回写状态
        duration = f"{time.time() - start_time:.2f}s"
        bj_now = (datetime.utcnow() + timedelta(hours=8)).strftime('%H:%M')
        master_sheet.update_cell(row_idx, 4, f"{mode_tag}-完成")
        master_sheet.update_cell(row_idx, 5, bj_now)
        master_sheet.update_cell(row_idx, 6, duration)
        master_sheet.update_cell(row_idx, 3, False)

    except Exception as e:
        master_sheet.update_cell(row_idx, 4, f"错误:{str(e)[:10]}")
        master_sheet.update_cell(row_idx, 3, False)

def main():
    payload = json.loads(os.environ.get('PAYLOAD', '{}'))
    gc = get_gc()
    master_sheet = gc.open_by_key(MASTER_ID).get_worksheet(0)

    if payload and payload.get('priority') == "1_MANUAL":
        sync_row(gc, master_sheet, payload['row'], "手触")
    else:
        # 定时同步全量 150 行
        for i in range(2, 152): sync_row(gc, master_sheet, i, "定时")

if __name__ == "__main__":
    main()
