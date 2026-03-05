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

def get_feishu_data_all_pages(app_token, table_id, fs_token):
    """【核心更新】翻页抓取飞书所有数据，严格对齐顺序"""
    # 1. 获取字段定义确定列排序
    fields_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/fields"
    f_res = requests.get(fields_url, headers={"Authorization": f"Bearer {fs_token}"})
    ordered_fields = [f['field_name'] for f in f_res.json().get('data', {}).get('items', [])]

    # 2. 循环翻页抓取记录
    all_items = []
    has_more = True
    page_token = ""
    
    while has_more:
        params = {"page_size": 500, "page_token": page_token}
        recs_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records"
        r_res = requests.get(recs_url, headers={"Authorization": f"Bearer {fs_token}"}, params=params)
        data = r_res.json().get('data', {})
        all_items.extend(data.get('items', []))
        
        has_more = data.get('has_more', False)
        page_token = data.get('page_token', "")
        if has_more: time.sleep(0.5) # 避开频率限制

    # 3. 组织全量数据
    output = [ordered_fields]
    for item in all_items:
        fields_data = item.get('fields', {})
        output.append([fields_data.get(name, "") for name in ordered_fields])
    return output

def sync_sub_sheet(gc, master_sheet, row_idx, mode_tag):
    start_time = time.time()
    row_data = master_sheet.row_values(row_idx)
    if not row_data or not row_data[0]: return 
    
    sub_url = row_data[0] # A列: 副本链接
    try:
        sub_ss = gc.open_by_url(sub_url)
        sub_main_ws = sub_ss.get_worksheet(0)
        all_sub_rows = sub_main_ws.get_all_values()
        
        # 飞书授权
        f_id, f_secret = os.environ.get("FEISHU_APP_ID"), os.environ.get("FEISHU_APP_SECRET")
        auth = requests.post("https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal", 
                             json={"app_id": f_id, "app_secret": f_secret})
        fs_token = auth.json().get("tenant_access_token")

        # 遍历副本内 A3 开始的行
        for i, r in enumerate(all_sub_rows[2:], start=3):
            if not r or not r[0]: continue
            fs_url, target_tab = r[0], r[1]
            
            # 副本状态回写：🚀 同步中
            sub_main_ws.update_cell(i, 4, "🚀 极速同步中...")
            
            app_token, table_id = parse_fs_url(fs_url)
            if not app_token or not table_id:
                sub_main_ws.update_cell(i, 4, "❌ URL无效")
                continue

            # 执行全量翻页同步
            data_to_write = get_feishu_data_all_pages(app_token, table_id, fs_token)
            
            ws = sub_ss.worksheet(target_tab)
            ws.clear()
            ws.update('A1', data_to_write, value_input_option='USER_ENTERED')

            # 副本状态回写：✅ 完成
            sub_duration = int(time.time() - start_time)
            bj_now = (datetime.utcnow() + timedelta(hours=8)).strftime('%Y-%m-%d %H:%M')
            sub_main_ws.update_cell(i, 4, f"✅ 完成 ({len(data_to_write)-1}条/{sub_duration}s)")
            sub_main_ws.update_cell(i, 5, bj_now)
            time.sleep(1.5) # 保护谷歌 API 频率

        # 总控表回写总进度
        total_duration = f"{time.time() - start_time:.2f}s"
        master_sheet.update_cell(row_idx, 4, f"{mode_tag}-✅全量")
        master_sheet.update_cell(row_idx, 5, bj_now)
        master_sheet.update_cell(row_idx, 6, total_duration)
        master_sheet.update_cell(row_idx, 3, False)

    except Exception as e:
        master_sheet.update_cell(row_idx, 4, f"错误:{str(e)[:15]}")
        master_sheet.update_cell(row_idx, 3, False)

def main():
    gc = get_gc()
    master_sheet = gc.open_by_key(MASTER_ID).get_worksheet(0)
    # 全量 150 行遍历
    all_vals = master_sheet.get_all_values()
    for i in range(3, len(all_vals) + 1):
        if i > 152: break
        sync_sub_sheet(gc, master_sheet, i, "定时")
        time.sleep(2)

if __name__ == "__main__":
    main()
