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
    view_id = re.search(r"view=([a-zA-Z0-9]+)", url)
    return (
        app_token.group(1) if app_token else None, 
        table_id.group(1) if table_id else None,
        view_id.group(1) if view_id else None
    )

def get_col_letter(col_idx):
    letter = ""
    while col_idx > 0:
        col_idx, remainder = divmod(col_idx - 1, 26)
        letter = chr(65 + remainder) + letter
    return letter

def clean_value(val):
    """将飞书的复杂数据（人员、多选、链接等）强制降维为谷歌能接受的纯文本"""
    if val is None:
        return ""
    if isinstance(val, (int, float, bool, str)):
        return val
    if isinstance(val, list):
        try:
            # 尝试提取人员名字或多选标签文本
            return ", ".join([str(v.get('name', v.get('text', v))) if isinstance(v, dict) else str(v) for v in val])
        except:
            pass
    if isinstance(val, dict):
        return str(val.get('name', val.get('text', str(val))))
    return str(val)

def sync_matrix_worker():
    try:
        payload_raw = os.environ.get('PAYLOAD', '{}')
        payload = json.loads(payload_raw) if payload_raw and payload_raw != 'null' else {}
        is_manual = payload.get('priority') == '1_MANUAL'
        manual_source_id = payload.get('source_id')
        manual_row = payload.get('row')

        gc = get_gc()
        master_ws = gc.open_by_key(MASTER_ID).get_worksheet(0)

        row_data = master_ws.row_values(TARGET_ROW)
        row_data += [""] * (6 - len(row_data))

        if not row_data[0] or "google" not in row_data[0]: return

        sub_url = row_data[0]
        current_status = row_data[3]

        sub_id_match = re.search(r"/d/([a-zA-Z0-9-_]+)", sub_url)
        if not sub_id_match: return
        sub_id = sub_id_match.group(1)

        if not is_manual and "暂停" in current_status: return

        should_run = False
        sync_all_in_sub = False
        target_sub_row = None

        if is_manual:
            if manual_source_id == MASTER_ID:
                if manual_row == 2 or manual_row == TARGET_ROW:
                    should_run = True
                    sync_all_in_sub = True
            elif manual_source_id == sub_id:
                if manual_row == 2:
                    should_run = True
                    sync_all_in_sub = True
                elif manual_row > 2:
                    should_run = True
                    sync_all_in_sub = False
                    target_sub_row = manual_row

        if not should_run: return

        start_time = time.time()
        sub_ss = gc.open_by_key(sub_id)
        
        try:
            sub_ws = sub_ss.worksheet("汇总表")
        except:
            return
            
        sub_rows = sub_ws.get_all_values()

        fs_token = get_fs_token()
        tables_to_sync = []
        
        for i, r in enumerate(sub_rows[2:], start=3):
            r += [""] * (3 - len(r))
            fs_url, target_tab = r[0], r[1]
            if not fs_url or "feishu" not in fs_url: continue
            if sync_all_in_sub or (target_sub_row == i):
                tables_to_sync.append((i, fs_url, target_tab))

        if not tables_to_sync: return

        total_tables = len(tables_to_sync)

        for idx, (i, fs_url, target_tab) in enumerate(tables_to_sync, 1):
            progress_msg = f"正在刷新({idx}/{total_tables}): {target_tab}"
            sub_ws.update_cell(i, 4, progress_msg)
            master_ws.update_cell(TARGET_ROW, 4, progress_msg)

            app_token, table_id, view_id = parse_fs_url(fs_url)
            if not app_token or not table_id: continue

            fields_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/fields"
            if view_id:
                fields_url += f"?view_id={view_id}"
                
            f_res = requests.get(fields_url, headers={"Authorization": f"Bearer {fs_token}"}).json()
            
            if f_res.get("code") != 0:
                sub_ws.update_cell(i, 4, f"❌ 获取列失败: {f_res.get('msg')}")
                continue
                
            data_dict = f_res.get('data') or {}
            items_list = data_dict.get('items') or []
            ordered_fields = [f['field_name'] for f in items_list if f['field_name'] != 'SourceID']

            if not ordered_fields:
                sub_ws.update_cell(i, 4, f"❌ 未找到可用列")
                continue

            all_items = []
            page_token, has_more = "", True
            while has_more:
                params = {"page_size": 500, "page_token": page_token}
                res = requests.get(f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records",
                                   headers={"Authorization": f"Bearer {fs_token}"}, params=params).json()
                
                res_data = res.get('data') or {}
                all_items.extend(res_data.get('items') or [])
                has_more = res_data.get('has_more', False)
                page_token = res_data.get('page_token', "")

            if all_items:
                # 🛠️ 关键修复处：过滤所有单元格数据，彻底消除谷歌表格的排异反应
                output = [ordered_fields]
                for it in all_items:
                    row = []
                    for name in ordered_fields:
                        raw_val = it.get('fields', {}).get(name, "")
                        row.append(clean_value(raw_val))
                    output.append(row)

                try:
                    ws = sub_ss.worksheet(target_tab)
                except:
                    ws = sub_ss.add_worksheet(title=target_tab, rows="1000", cols="20")
                
                num_cols = len(ordered_fields)
                if num_cols > 0:
                    col_letter = get_col_letter(num_cols)
                    ws.batch_clear([f"A:{col_letter}"])
                
                try:
                    ws.update(values=output, range_name='A1', value_input_option='USER_ENTERED')
                except Exception as e:
                    sub_ws.update_cell(i, 4, f"❌ 数据写入失败: {str(e)[:40]}")
                    continue

            bj_now_str = (datetime.utcnow() + timedelta(hours=8)).strftime('%Y-%m-%d %H:%M')
            sub_ws.update(values=[[False, f"✅ 完成({len(all_items)}条)", bj_now_str, f"{int(time.time() - start_time)}s"]], range_name=f'C{i}:F{i}')

        bj_now_str = (datetime.utcnow() + timedelta(hours=8)).strftime('%Y-%m-%d %H:%M')
        
        if sync_all_in_sub:
            try:
                sub_ws.update(values=[[False, "✅ 全部完成", bj_now_str, f"{time.time()-start_time:.1f}s"]], range_name='C2:F2')
            except:
                pass

        if manual_source_id == sub_id:
            master_ws.update(values=[["", "✅ 副本触发完成", bj_now_str, f"{time.time()-start_time:.1f}s"]], range_name=f'C{TARGET_ROW}:F{TARGET_ROW}')
        elif manual_source_id == MASTER_ID:
            if manual_row == TARGET_ROW:
                master_ws.update(values=[[False, "✅ 单行手触完成", bj_now_str, f"{time.time()-start_time:.1f}s"]], range_name=f'C{TARGET_ROW}:F{TARGET_ROW}')
            elif manual_row == 2:
                master_ws.update(values=[[False, "✅ 全量准时完成", bj_now_str, f"{time.time()-start_time:.1f}s"]], range_name=f'C{TARGET_ROW}:F{TARGET_ROW}')
                time.sleep(1.5)
                master_ws.update_cell(2, 3, False)
    
    except Exception as e:
        # 🛡️ 终极气囊：就算天塌下来，也会把具体原因写在总控表里，再也不会“死得不明不白”
        try:
            gc = get_gc()
            master_ws = gc.open_by_key(MASTER_ID).get_worksheet(0)
            master_ws.update_cell(TARGET_ROW, 4, f"❌ 崩溃详情: {str(e)[:50]}")
        except:
            pass

if __name__ == "__main__":
    sync_matrix_worker()
