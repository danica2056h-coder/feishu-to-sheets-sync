import os, json, time, requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta

# --- 基础配置 ---
MASTER_ID = "1X7yDRVlOgG42flnSuki7BUF68kbO0cgn5GF-wu8g9cw"
APP_ID = os.environ.get("FEISHU_APP_ID")
APP_SECRET = os.environ.get("FEISHU_APP_SECRET")

def get_gc():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_dict(json.loads(os.environ.get("G_SERVICE_ACCOUNT")), scope)
    return gspread.authorize(creds)

def get_feishu_token():
    r = requests.post("https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal", 
                      json={"app_id": APP_ID, "app_secret": APP_SECRET})
    return r.json().get("tenant_access_token")

def sync_row(gc, master_sheet, row_idx, mode_tag):
    """
    核心搬运：假设 B:副本ID, G:飞书Token, H:飞书TableID, A:Sheet名
    """
    start_time = time.time()
    row_data = master_sheet.row_values(row_idx)
    # 填充默认值防止越界
    row_data += [""] * (10 - len(row_data))
    
    target_ss_id = row_data[1]    # B列
    target_sheet_name = row_data[0] # A列
    bitable_token = row_data[6]   # G列
    table_id = row_data[7]        # H列

    try:
        # 1. 抓取飞书 (全量抓取)
        fs_token = get_feishu_token()
        res = requests.get(f"https://open.feishu.cn/open-apis/bitable/v1/apps/{bitable_token}/tables/{table_id}/records",
                           headers={"Authorization": f"Bearer {fs_token}"}, params={"page_size": 500})
        items = res.json().get('data', {}).get('items', [])
        if not items: raise Exception("无数据")
        
        # 转换数据格式
        headers = list(items[0]['fields'].keys())
        output = [headers] + [[str(i['fields'].get(k, "")) for k in headers] for i in items]

        # 2. 写入谷歌 (强制覆盖)
        target_ss = gc.open_by_key(target_ss_id)
        try:
            ws = target_ss.worksheet(target_sheet_name)
        except:
            ws = target_ss.add_worksheet(title=target_sheet_name, rows="1000", cols="26")
        ws.clear()
        ws.update('A1', output)

        # 3. 回写状态
        duration = f"{time.time() - start_time:.2f}s"
        bj_now = (datetime.utcnow() + timedelta(hours=8)).strftime('%H:%M')
        
        master_sheet.update_cell(row_idx, 4, f"{mode_tag}-完成") # D列
        master_sheet.update_cell(row_idx, 5, bj_now)          # E列
        master_sheet.update_cell(row_idx, 6, duration)        # F列
        master_sheet.update_cell(row_idx, 3, False)           # C列复位

    except Exception as e:
        master_sheet.update_cell(row_idx, 4, f"失败:{str(e)[:15]}")
        master_sheet.update_cell(row_idx, 3, False)

def main():
    payload = json.loads(os.environ.get('PAYLOAD', '{}'))
    gc = get_gc()
    master_sheet = gc.open_by_key(MASTER_ID).get_worksheet(0)

    if payload.get('priority') == "1_MANUAL":
        # 情况：手动刷新单行
        sync_row(gc, master_sheet, payload['row'], "手触")
    else:
        # 情况：15分钟定时全量刷新
        print("开始 150 行全量搬运...")
        for i in range(2, 152): # 遍历第2到151行
            sync_row(gc, master_sheet, i, "定时")

if __name__ == "__main__":
    main()
