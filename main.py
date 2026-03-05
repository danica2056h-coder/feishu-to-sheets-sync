import os, json, time, requests, re
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta

MASTER_ID = os.environ.get("MASTER_SHEET_ID")
TARGET_ROW = int(os.environ.get("TARGET_ROW", 3))

def get_gc():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_dict(json.loads(os.environ.get("GOOGLE_CREDENTIALS")), scope)
    return gspread.authorize(creds)

def get_fs_token():
    res = requests.post("https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal", 
                        json={"app_id": os.environ.get("FEISHU_APP_ID"), "app_secret": os.environ.get("FEISHU_APP_SECRET")})
    return res.json().get("tenant_access_token")

def parse_fs_url(url):
    app_token = re.search(r"base/([a-zA-Z0-9]+)", url)
    table_id = re.search(r"table=([a-zA-Z0-9]+)", url)
    return (app_token.group(1), table_id.group(1)) if app_token and table_id else (None, None)

def sync_matrix_worker():
    payload_raw = os.environ.get('PAYLOAD', '{}')
    payload = json.loads(payload_raw) if payload_raw and payload_raw != 'null' else {}

    gc = get_gc()
    master_ws = gc.open_by_key(MASTER_ID).get_worksheet(0)
    row_data = master_ws.row_values(TARGET_ROW)
    row_data += [""] * (6 - len(row_data))

    if not row_data[0] or "google" not in row_data[0]: return 

    sub_url = row_data[0]
    sub_id_match = re.search(r"/d/([a-zA-Z0-9-_]+)", sub_url)
    if not sub_id_match: return
    sub_id = sub_id_match.group(1)

    is_manual = payload.get('priority') == '1_MANUAL'
    manual_source_id = payload.get('source_id')
    manual_row = payload.get('row')

    should_run = False
    sync_all_in_sub = False
    target_sub_row = None

    if not is_manual:
        should_run = True
        sync_all_in_sub = True
    else:
        if manual_source_id == MASTER_ID:
            if manual_row == 2:
                should_run = True
                sync_all_in_sub = True
            elif manual_row == TARGET_ROW:
                should_run = True
                sync_all_in_sub = True
        elif manual_source_id == sub_id:
            should_run = True
            if manual_row == 2:
                sync_all_in_sub = True
            else:
                sync_all_in_sub = False
                target_sub_row = manual_row

    if not should_run: return

    start_time = time.time()
    sub_ss = gc.open_by_key(sub_id)
    sub_ws = sub_ss.get_worksheet(0)
    sub_rows = sub_ws.get_all_values()

    if is_manual and manual_source_id == MASTER_ID:
        master_ws.update_cell(TARGET_ROW, 4, "🚀 矩阵节点处理中...")

    fs_token = get_fs_token()

    for i, r in enumerate(sub_rows[2:], start=3):
        r += [""] * (3 - len(r))
        fs_url, target_tab = r[0], r[1]

        if not fs_url or "feishu" not in fs_url: continue

        if sync_all_in_sub or (target_sub_row == i):
            sub_ws.update_cell(i, 4, "🚀 抓取中...")
            app_token, table_id = parse_fs_url(fs_url)
            if not app_token: continue

            fields_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/fields"
            f_res = requests.get(fields_url, headers={"Authorization": f"Bearer {fs_token}"}).json()
            ordered_fields = [f['field_name'] for f in f_res.get('data', {}).get('items', [])]

            all_items = []
            page_token, has_more = "", True
            while has_more:
                params = {"page_size": 500, "page_token": page_token}
                res = requests.get(f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records",
                                   headers={"Authorization": f"Bearer {fs_token}"}, params=params).json()
                all_items.extend(res.get('data', {}).get('items', []))
                has_more = res.get('data', {}).get('has_more', False)
                page_token = res.get('data', {}).get('page_token', "")

            if all_items:
                output = [ordered_fields] + [[it.get('fields', {}).get(name, "") for name in ordered_fields] for it in all_items]
                try:
                    ws = sub_ss.worksheet(target_tab)
                except:
                    ws = sub_ss.add_worksheet(title=target_tab, rows="1000", cols="20")
                ws.clear()
                ws.update(values=output, range_name='A1', value_input_option='USER_ENTERED')

            bj_now = (datetime.utcnow() + timedelta(hours=8)).strftime('%Y-%m-%d %H:%M')
            sub_ws.update(values=[[False, f"✅ 完成({len(all_items)}条)", bj_now, f"{int(time.time() - start_time)}s"]], range_name=f'C{i}:F{i}')

    bj_now = (datetime.utcnow() + timedelta(hours=8)).strftime('%Y-%m-%d %H:%M')
    if is_manual:
        if manual_source_id == sub_id and manual_row == 2:
            sub_ws.update_cell(2, 3, False)
        elif manual_source_id == MASTER_ID:
            if manual_row == TARGET_ROW:
                master_ws.update(values=[[False, "✅ 手触完成", bj_now, f"{time.time()-start_time:.1f}s"]], range_name=f'C{TARGET_ROW}:F{TARGET_ROW}')
            elif manual_row == 2:
                master_ws.update(values=[[False, "✅ 一键全量完成", bj_now, f"{time.time()-start_time:.1f}s"]], range_name=f'C{TARGET_ROW}:F{TARGET_ROW}')
                time.sleep(1.5)
                master_ws.update_cell(2, 3, False)
    else:
        master_ws.update(values=[["", "✅ 定时完成", bj_now, f"{time.time()-start_time:.1f}s"]], range_name=f'C{TARGET_ROW}:F{TARGET_ROW}')

if __name__ == "__main__":
    sync_matrix_worker()
