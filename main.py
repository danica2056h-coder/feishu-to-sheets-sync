import os, json, time, requests, re
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta

# 从环境变量读取配置
MASTER_ID = os.environ.get("MASTER_SHEET_ID")

def get_gc():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds_json = os.environ.get("GOOGLE_CREDENTIALS")
    if not creds_json:
        raise Exception("GOOGLE_CREDENTIALS 未配置")
    creds = ServiceAccountCredentials.from_json_keyfile_dict(json.loads(creds_json), scope)
    return gspread.authorize(creds)

def parse_fs_url(url):
    app_token = re.search(r"base/([a-zA-Z0-9]+)", url)
    table_id = re.search(r"table=([a-zA-Z0-9]+)", url)
    return (app_token.group(1) if app_token else None, table_id.group(1) if table_id else None)

def sync_sub_sheet(gc, master_sheet, row_idx, mode_tag):
    start_time = time.time()
    # 1. 严格读取总控 A3 开始不为空的行
    row_data = master_sheet.row_values(row_idx)
    if not row_data or not row_data[0]: return 
    
    sub_url = row_data[0]
    try:
        sub_ss = gc.open_by_url(sub_url)
        # 假设副本第一页为配置汇总页
        sub_main_ws = sub_ss.get_worksheet(0)
        all_sub_rows = sub_main_ws.get_all_values()
        
        # 飞书授权
        f_id, f_secret = os.environ.get("FEISHU_APP_ID"), os.environ.get("FEISHU_APP_SECRET")
        auth = requests.post("https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal", 
                             json={"app_id": f_id, "app_secret": f_secret})
        fs_token = auth.json().get("tenant_access_token")

        # 2. 遍历副本内 A3 开始的行
        for i, r in enumerate(all_sub_rows[2:], start=3):
            if not r or not r[0]: continue
            fs_url, target_tab = r[0], r[1]
            
            # --- 副本状态回写：同步中 ---
            sub_main_ws.update_cell(i, 4, "🚀 极速同步中...")
            
            app_token, table_id = parse_fs_url(fs_url)
            if not app_token or not table_id:
                sub_main_ws.update_cell(i, 4, "❌ URL无效")
                continue

            # 抓取数据并排序
            f_res = requests.get(f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/fields", 
                                 headers={"Authorization": f"Bearer {fs_token}"})
            ordered_fields = [f['field_name'] for f in f_res.json().get('data', {}).get('items', [])]
            
            r_res = requests.get(f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records",
                                 headers={"Authorization": f"Bearer {fs_token}"}, params={"page_size": 500})
            items = r_res.json().get('data', {}).get('items', [])

            output = [ordered_fields]
            for it in items:
                output.append([it.get('fields', {}).get(name, "") for name in ordered_fields])

            # 写入目标页
            ws = sub_ss.worksheet(target_tab)
            ws.clear()
            ws.update('A1', output, value_input_option='USER_ENTERED') # 解决单引号问题

            # --- 副本状态回写：完成 ---
            sub_duration = int(time.time() - start_time)
            bj_now = (datetime.utcnow() + timedelta(hours=8)).strftime('%Y-%m-%d %H:%M')
            sub_main_ws.update_cell(i, 4, f"✅ 完成 ({len(items)}条/{sub_duration}s)")
            sub_main_ws.update_cell(i, 5, bj_now)
            time.sleep(1) # 限速保护

        # 3. 总控表回写：该副本整体完成
        total_duration = f"{time.time() - start_time:.2f}s"
        master_sheet.update_cell(row_idx, 4, f"{mode_tag}-✅")
        master_sheet.update_cell(row_idx, 5, bj_now)
        master_sheet.update_cell(row_idx, 6, total_duration)
        master_sheet.update_cell(row_idx, 3, False) # 复选框复位

    except Exception as e:
        master_sheet.update_cell(row_idx, 4, f"错误:{str(e)[:15]}")
        master_sheet.update_cell(row_idx, 3, False)

def main():
    payload_raw = os.environ.get('PAYLOAD', '{}')
    payload = json.loads(payload_raw) if payload_raw and payload_raw != 'null' else {}
    gc = get_gc()
    master_sheet = gc.open_by_key(MASTER_ID).get_worksheet(0)

    if payload and payload.get('priority') == "1_MANUAL":
        sync_sub_sheet(gc, master_sheet, payload['row'], "手触")
    else:
        # 定时全量遍历总控表 A3 开始
        all_vals = master_sheet.get_all_values()
        for i in range(3, len(all_vals) + 1):
            if i > 152: break
            sync_sub_sheet(gc, master_sheet, i, "定时")
            time.sleep(2)

if __name__ == "__main__":
    main()
