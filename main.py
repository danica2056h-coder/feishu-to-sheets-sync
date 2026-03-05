import os, json, time, requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta

# === 基础配置 (从环境变量读取) ===
APP_ID = os.environ.get("FEISHU_APP_ID")
APP_SECRET = os.environ.get("FEISHU_APP_SECRET")
# 总控表 ID
MASTER_ID = "1X7yDRVlOgG42flnSuki7BUF68kbO0cgn5GF-wu8g9cw" 

# === 1. 授权与工具函数 ===
def get_gs_client():
    """授权 Google Sheets API"""
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds_json = os.environ.get("G_SERVICE_ACCOUNT")
    if not creds_json: raise Exception("未配置 G_SERVICE_ACCOUNT Secrets")
    creds = ServiceAccountCredentials.from_json_keyfile_dict(json.loads(creds_json), scope)
    return gspread.authorize(creds)

def get_tenant_token():
    """获取飞书通行证"""
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    r = requests.post(url, json={"app_id": APP_ID, "app_secret": APP_SECRET})
    return r.json().get("tenant_access_token")

def get_feishu_data(token, app_token, table_id):
    """抓取飞书多维表格所有数据"""
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records"
    headers = {"Authorization": f"Bearer {token}"}
    params = {"page_size": 500} # 每次最多500条
    r = requests.get(url, headers=headers, params=params)
    data = r.json()
    if data.get("code") != 0: return None
    
    records = data['data'].get('items', [])
    if not records: return []
    
    # 转换为二维数组（包含表头）
    headers_list = list(records[0]['fields'].keys())
    rows = [headers_list]
    for item in records:
        rows.append([str(item['fields'].get(k, "")) for k in headers_list])
    return rows

# === 2. 核心同步逻辑 ===
def sync_single_table(gc, spreadsheet_id, sheet_name, feishu_app_token, feishu_table_id, log_sheet, log_row, mode_label):
    """
    同步单个 Sheet 并回写状态
    """
    start_time = time.time()
    try:
        # 1. 抓取飞书数据
        token = get_tenant_token()
        data = get_feishu_data(token, feishu_app_token, feishu_table_id)
        if data is None: raise Exception("飞书数据抓取失败")

        # 2. 写入谷歌表格
        ss = gc.open_by_key(spreadsheet_id)
        try:
            target_sheet = ss.worksheet(sheet_name)
        except:
            target_sheet = ss.add_worksheet(title=sheet_name, rows="100", cols="20")
        
        target_sheet.clear()
        target_sheet.update('A1', data) # 批量写入

        # 3. 计算耗时与时间
        duration = f"{time.time() - start_time:.2f}s"
        bj_now = (datetime.utcnow() + timedelta(hours=8)).strftime('%H:%M')

        # 4. 回写日志 (D:状态, E:时间, F:时长) 并复位 C 列
        log_sheet.update_cell(log_row, 4, f"{mode_label}-成功")
        log_sheet.update_cell(log_row, 5, bj_now)
        log_sheet.update_cell(log_row, 6, duration)
        log_sheet.update_cell(log_row, 3, False) # 复选框弹回
        print(f"✅ 已完成: {sheet_name} ({duration})")

    except Exception as e:
        log_sheet.update_cell(log_row, 4, f"失败: {str(e)[:20]}")
        log_sheet.update_cell(log_row, 3, False)
        print(f"❌ 失败: {sheet_name}, 错误: {e}")

# === 3. 调度引擎 ===
def main():
    payload_str = os.environ.get('PAYLOAD', '{}')
    payload = json.loads(payload_str) if payload_str and payload_str != 'null' else {}
    gc = get_gs_client()
    
    # 场景 A: 手动触发 (Priority 1)
    if payload.get('priority') == "1_MANUAL":
        source_id = payload['source_id']
        row = payload['row']
        ss = gc.open_by_key(source_id)
        log_sheet = ss.get_worksheet(0) # 默认第一页为日志页
        
        # 读取该行配置 (假设 B 列是副本ID或TableID，这里根据你表格实际列调整)
        row_data = log_sheet.row_values(row)
        # 示例：sync_single_table(gc, source_id, row_data[0], row_data[1], row_data[2], log_sheet, row, "手触")
        # 这里需要根据你表格 A, B, C 列的具体内容来传入参数
        print(f"正在处理手动刷新: 第 {row} 行")
        # 为演示完整性，此处执行同步动作...
        sync_single_table(gc, source_id, "Sheet1", "飞书Token", "飞书TableID", log_sheet, row, "手触")

    # 场景 B: 15分钟定时刷新 (Priority 3)
    else:
        print("⏰ 启动 15 分钟全量巡检...")
        master_ss = gc.open_by_key(MASTER_ID)
        master_log = master_ss.get_worksheet(0)
        configs = master_log.get_all_values()
        
        # 遍历总控表 150 行 (跳过表头)
        for i, config in enumerate(configs[1:], start=2):
            if i > 151: break
            # 逻辑：逐个搬运
            # sync_single_table(gc, config[1], config[0], ...)
            pass

if __name__ == "__main__":
    main()
