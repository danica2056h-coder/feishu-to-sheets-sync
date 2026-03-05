import os, json, re, requests, gspread, time
from datetime import datetime
import pytz

FEISHU_TOKEN_URL = 'https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal'
FEISHU_BASE_URL = 'https://open.feishu.cn/open-apis/bitable/v1/apps'
MAX_SYNC_COL = 30 

def get_feishu_token(app_id, app_secret):
    res = requests.post(FEISHU_TOKEN_URL, json={"app_id": app_id, "app_secret": app_secret})
    return res.json().get('tenant_access_token')

def parse_feishu_url(url):
    app_token = re.search(r'(?:base|wiki)/([a-zA-Z0-9]+)', url)
    table_id = re.search(r'table=([a-zA-Z0-9]+)', url)
    view_id = re.search(r'view=([a-zA-Z0-9]+)', url)
    return {"appToken": app_token.group(1) if app_token else None,
            "tableId": table_id.group(1) if table_id else None,
            "viewId": view_id.group(1) if view_id else None}

def process_value(val):
    """核心改进：清理格式，去除单引号前缀"""
    if val is None: return ""
    
    # 1. 处理列表/多选 (提取文本并合并)
    if isinstance(val, list):
        names = []
        for item in val:
            if isinstance(item, dict):
                names.append(str(item.get('name', item.get('text', item))))
            else:
                names.append(str(item))
        return ", ".join(names)
    
    # 2. 处理字典 (单选/单个人员)
    if isinstance(val, dict):
        return val.get('name', val.get('text', str(val)))
    
    # 3. 处理数字或字符串类型的数字
    if isinstance(val, (int, float)):
        # 时间戳识别 (13位毫秒)
        if val > 1000000000000:
            return datetime.fromtimestamp(val/1000).strftime('%Y-%m-%d %H:%M')
        return val # 直接返回数字类型，Google Sheets 就不会加 ' 了

    if isinstance(val, str):
        # 尝试清理：去掉千分位逗号，然后尝试转成数字
        clean_val = val.replace(',', '').strip()
        try:
            if '.' in clean_val: return float(clean_val)
            return int(clean_val)
        except:
            return val # 实在转不成数字的（如文字内容）再保持字符串
            
    return str(val)

def fetch_feishu_data(params, token):
    headers = {'Authorization': f'Bearer {token}'}
    fields_res = requests.get(f"{FEISHU_BASE_URL}/{params['appToken']}/tables/{params['tableId']}/fields?view_id={params['viewId']}", headers=headers).json()
    items = fields_res.get('data', {}).get('items', [])
    header_names = [i['field_name'] for i in items if not i.get('is_hidden')][:MAX_SYNC_COL]
    
    all_values, page_token, has_more = [header_names], "", True
    while has_more:
        url = f"{FEISHU_BASE_URL}/{params['appToken']}/tables/{params['tableId']}/records?page_size=500&view_id={params['viewId']}"
        if page_token: url += f"&page_token={page_token}"
        res = requests.get(url, headers=headers).json()
        records = res.get('data', {}).get('items', [])
        for record in records:
            fields = record.get('fields', {})
            all_values.append([process_value(fields.get(h)) for h in header_names])
        has_more = res.get('data', {}).get('has_more', False)
        page_token = res.get('data', {}).get('page_token', "")
    return all_values

def main():
    gc = gspread.service_account_from_dict(json.loads(os.environ['GOOGLE_CREDENTIALS']))
    token = get_feishu_token(os.environ['FEISHU_APP_ID'], os.environ['FEISHU_APP_SECRET'])
    tz = pytz.timezone('Asia/Shanghai')
    
    master_ws = gc.open_by_key(os.environ['MASTER_SHEET_ID']).get_worksheet(0)
    sub_urls = [u for u in master_ws.col_values(1)[1:] if u]
    
    for url in sub_urls:
        try:
            sub_id = re.search(r'/d/([a-zA-Z0-9-_]+)', url).group(1)
            doc = gc.open_by_key(sub_id)
            summary_ws = doc.worksheet("汇总表")
            instructions = summary_ws.get_all_values()[1:]
            
            updates = []
            for row in instructions:
                if len(row) < 2 or not row[0]: continue
                try:
                    data = fetch_feishu_data(parse_feishu_url(row[0]), token)
                    try: target_ws = doc.worksheet(row[1])
                    except: target_ws = doc.add_worksheet(title=row[1], rows=100, cols=MAX_SYNC_COL)
                    target_ws.clear()
                    # 关键：RAW 模式配合 Python 类型转换，彻底解决格式问题
                    target_ws.update(data, "A1", value_input_option='USER_ENTERED')
                    updates.append([f"✅成功({len(data)-1}条)", datetime.now(tz).strftime("%H:%M")])
                except Exception as e:
                    updates.append([f"❌失败: {str(e)[:15]}", ""])
            
            if updates:
                summary_ws.update(updates, f"D2:E{len(updates)+1}")
        except Exception as e: print(f"副本执行失败: {e}")

if __name__ == "__main__":
    main()
