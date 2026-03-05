import os, json, time, requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta

# 从环境变量读取你设置的 MASTER_SHEET_ID
MASTER_ID = os.environ.get("MASTER_SHEET_ID")

def get_gc():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    # 👈 这里改成了你图片里的名称 GOOGLE_CREDENTIALS
    creds_json = os.environ.get("GOOGLE_CREDENTIALS")
    if not creds_json:
        raise Exception("无法读取 GOOGLE_CREDENTIALS，请检查 GitHub Secrets 配置")
    creds = ServiceAccountCredentials.from_json_keyfile_dict(json.loads(creds_json), scope)
    return gspread.authorize(creds)

def sync_row(gc, master_sheet, row_idx, mode_tag):
    """
    全量搬运逻辑
    假设配置列：A:表名, B:副本ID, G:飞书Token, H:飞书TableID
    """
    start_time = time.time()
    row_data = master_sheet.row_values(row_idx)
    row_data += [""] * (10 - len(row_data)) # 补齐列防止索引错误
    
    try:
        # 1. 飞书认证
        app_id = os.environ.get("FEISHU_APP_ID")
        app_secret = os.environ.get("FEISHU_APP_SECRET")
        auth_res = requests.post("https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal", 
                                 json={"app_id": app_id, "app_secret": app_secret})
        fs_token = auth_res.json().get("tenant_access_token")
        
        # 2. 抓取飞书数据 (Bitable API)
        # 注意：此处使用 row_data[6] 是 G 列，row_data[7] 是 H 列
        res = requests.get(f"https://open.feishu.cn/open-apis/bitable/v1/apps/{row_data[6]}/tables/{row_data[7]}/records",
                           headers={"Authorization": f"Bearer {fs_token}"}, params={"page_size": 500})
        
        if res.status_code != 200: raise Exception(f"飞书请求失败: {res.status_code}")
        items = res.json().get('data', {}).get('items', [])
        
        # 3. 写入谷歌表格
        if items:
            headers = list(items[0]['fields'].keys())
            output = [headers] + [[str(i['fields'].get(k, "")) for k in headers] for i in items]
            
            target_ss = gc.open_by_key(row_data[1]) # B列副本ID
            ws = target_ss.worksheet(row_data[0])   # A列表名
            ws.clear()
            ws.update('A1', output)

        # 4. 回写状态 (D:状态, E:时间, F:时长) 并复位 C 列
        duration = f"{time.time() - start_time:.2f}s"
        bj_now = (datetime.utcnow() + timedelta(hours=8)).strftime('%H:%M')
        master_sheet.update_cell(row_idx, 4, f"{mode_tag}-完成")
        master_sheet.update_cell(row_idx, 5, bj_now)
        master_sheet.update_cell(row_idx, 6, duration)
        master_sheet.update_cell(row_idx, 3, False) 

    except Exception as e:
        master_sheet.update_cell(row_idx, 4, f"错误:{str(e)[:15]}")
        master_sheet.update_cell(row_idx, 3, False)

def main():
    payload_raw = os.environ.get('PAYLOAD', '{}')
    payload = json.loads(payload_raw) if payload_raw and payload_raw != 'null' else {}
    gc = get_gc()
    master_sheet = gc.open_by_key(MASTER_ID).get_worksheet(0)

    if payload and payload.get('priority') == "1_MANUAL":
        # 手动单行同步
        sync_row(gc, master_sheet, payload['row'], "手触")
    else:
        # 定时全量同步 (2-151行)
        for i in range(2, 152): 
            sync_row(gc, master_sheet, i, "定时")

if __name__ == "__main__":
    main()
