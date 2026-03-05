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
        raise Exception("GOOGLE_CREDENTIALS Secrets 未配置")
    creds = ServiceAccountCredentials.from_json_keyfile_dict(json.loads(creds_json), scope)
    return gspread.authorize(creds)

def extract_id(url):
    match = re.search(r"/d/([a-zA-Z0-9-_]+)", url)
    return match.group(1) if match else url

def parse_fs_url(url):
    app_token = re.search(r"base/([a-zA-Z0-9]+)", url)
    table_id = re.search(r"table=([a-zA-Z0-9]+)", url)
    return (app_token.group(1) if app_token else None, 
            table_id.group(1) if table_id else None)

def get_feishu_data_ordered(app_token, table_id, fs_token):
    """核心：获取排好序的飞书数据"""
    # 1. 先获取字段列表（确定列顺序）
    fields_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/fields"
    f_res = requests.get(fields_url, headers={"Authorization": f"Bearer {fs_token}"})
    field_list = f_res.json().get('data', {}).get('items', [])
    # 过滤出非计算类的基本字段，并按飞书视觉顺序排列
    ordered_field_names = [f['field_name'] for f in field_list]

    # 2. 获取记录数据
    records_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records"
    r_res = requests.get(records_url, headers={"Authorization": f"Bearer {fs_token}"}, params={"page_size": 500})
    items = r_res.json().get('data', {}).get('items', [])

    if not items:
        return [ordered_field_names] # 仅返回表头

    # 3. 严格按照表头顺序重新组织数据
    output = [ordered_field_names]
    for item in items:
        fields_data = item.get('fields', {})
        # 按照有序表头提取值，缺失则补空字符串
        row = [str(fields_data.get(name, "")) for name in ordered_field_names]
        output.append(row)
    return output

def sync_sub_sheet(gc, master_sheet, row_idx, mode_tag):
    start_time = time.time()
    # 补齐 6 列防止总控表 IndexError
    row_data = master_sheet.row_values(row_idx)
    row_data += [""] * (6 - len(row_data)) 
    
    sub_url = row_data[0] # A列: 副本链接
    if not sub_url or "google.com" not in sub_url: return

    try:
        sub_id = extract_id(sub_url)
        sub_ss = gc.open_by_key(sub_id)
        sub_main_ws = sub_ss.get_worksheet(0) # 副本 A3 开始为飞书 URL
        all_sub_rows = sub_main_ws.get_all_values()

        # 飞书授权
        f_id, f_secret = os.environ.get("FEISHU_APP_ID"), os.environ.get("FEISHU_APP_SECRET")
        auth = requests.post("https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal", 
                             json={"app_id": f_id, "app_secret": f_secret})
        fs_token = auth.json().get("tenant_access_token")

        # 遍历副本内数据行
        for i, r in enumerate(all_sub_rows[2:], start=3):
            r += [""] * (2 - len(r)) # 补齐防止 IndexError
            fs_url, target_tab = r[0], r[1]
            if not fs_url or "feishu" not in fs_url: continue
            
            app_token, table_id = parse_fs_url(fs_url)
            if not app_token or not table_id: continue

            # 同步数据（严格排序版）
            data_to_write = get_feishu_data_ordered(app_token, table_id, fs_token)
            
            try:
                ws = sub_ss.worksheet(target_tab)
            except:
                ws = sub_ss.add_worksheet(title=target_tab, rows="2000", cols="26")
            
            ws.clear()
            ws.update('A1', data_to_write)

        # 回写总控表状态
        duration = f"{time.time() - start_time:.2f}s"
        bj_now = (datetime.utcnow() + timedelta(hours=8)).strftime('%H:%M')
        master_sheet.update_cell(row_idx, 4, f"{mode_tag}-完成")
        master_sheet.update_cell(row_idx, 5, bj_now)
        master_sheet.update_cell(row_idx, 6, duration)
        master_sheet.update_cell(row_idx, 3, False) # C列复选框复位

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
        # A3 开始到 A151
        for i in range(3, 152): 
            sync_sub_sheet(gc, master_sheet, i, "定时")

if __name__ == "__main__":
    main()
