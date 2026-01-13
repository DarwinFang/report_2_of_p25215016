import os
import re
import sys
import gzip
import urllib.request
import xml.etree.ElementTree as ET
import csv
import time

# =================配置区域=================
INPUT_HTML = "./scc20241231.html"
OUTPUT_CSV = "tenhou_bootstrap_dataset.csv"
TARGET_ROOM = "四鳳南喰赤－"
DOWNLOAD_DELAY = 1.0
# ==========================================


HEADER = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:65.0) Gecko/20100101 Firefox/65.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate',
    'Connection': 'keep-alive'
}

def download_log(log_url):
    """下载并解压天凤XML日志"""
    try:
        data_url = log_url.replace("?log=", "log/?")
        domain = data_url.split('/')[2]
        req_header = HEADER.copy()
        req_header['Host'] = domain

        req = urllib.request.Request(url=data_url, headers=req_header)
        opener = urllib.request.build_opener()
        
        with opener.open(req, timeout=10) as response:
            content = response.read()
            # 自动处理 gzip
            if response.info().get('Content-Encoding') == 'gzip':
                content = gzip.decompress(content)
            return content.decode('utf-8')
    except Exception as e:
        print(f" [Error] 下载失败 {log_url}: {e}")
        return None

def parse_xml_game(xml_content):
    """
    核心解析逻辑：从 XML 中提取每一次和牌的数据
    """
    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError:
        # 有些日志可能不完整，尝试包裹 root 标签
        try:
            root = ET.fromstring(f"<root>{xml_content}</root>")
        except:
            return []

    extracted_data = []
    
    # 状态变量
    current_dealer = -1  # 当前庄家 (0-3)
    player_draw_counts = [0, 0, 0, 0] # 记录每个玩家摸了几张牌 (近似巡目)

    # 遍历 XML 节点
    for elem in root:
        tag = elem.tag
        attr = elem.attrib

        # 1. 每一局开始 (INIT)
        if tag == 'INIT':
            current_dealer = int(attr.get('oya'))
            player_draw_counts = [0, 0, 0, 0] # 重置巡目计数

        # 2. 摸牌行为 (T~W) -> 用于计算巡目
        # T=P0摸牌, U=P1, V=P2, W=P3
        elif tag[0] in ['T', 'U', 'V', 'W'] and tag[1:].isdigit():
            player_idx = {'T':0, 'U':1, 'V':2, 'W':3}[tag[0]]
            player_draw_counts[player_idx] += 1

        # 3. 碰/杠 (N) -> 虽然不摸牌，但为了严谨，这里不增加巡目
        # (注：如果不碰不杠，摸牌数=巡目；如果有副露，摸牌数≈巡目，Bootstrap分析足够了)

        # 4. 和牌 (AGARI) -> 提取数据的核心时刻！
        elif tag == 'AGARI':
            winner = int(attr.get('who'))
            from_who = int(attr.get('fromWho'))
            
            # --- 变量 1: Score (得点) ---
            # ten="符数,得点,满贯代码" -> 取第2个
            ten_list = attr.get('ten').split(',')
            score = int(ten_list[1])

            # --- 变量 2: Fu (符数) ---
            fu = int(ten_list[0])

            # --- 变量 3 & 4 & 7: Han (番数), Dora, Is_Riichi ---
            # yaku="ID,番,ID,番..."
            yaku_str = attr.get('yaku')
            
            total_han = 0
            dora_count = 0
            is_riichi = 0
            
            if yaku_str:
                yaku_items = [int(x) for x in yaku_str.split(',')]
                # 遍历 "ID, 番" 对
                for i in range(0, len(yaku_items), 2):
                    y_id = yaku_items[i]
                    y_han = yaku_items[i+1]
                    
                    # 累加番数 (排除役满ID，役满虽然ID很大但番数显示可能不同，这里简化处理常规役)
                    # 如果是役满，ten 里的得点会极其巨大，番数统计可能失效，但 Bootstrap 主要看 Score
                    total_han += y_han

                    # 统计立直 (ID=1: 立直, ID=21: 双立直)
                    if y_id == 1 or y_id == 21:
                        is_riichi = 1
                    
                    # 统计 Dora (ID=52: Dora, 53: Ura, 54: Aka)
                    if y_id in [52, 53, 54]:
                        dora_count += y_han
            
            # 处理役满情况 (XML中 yakuman 属性存在时)
            if 'yakuman' in attr:
                # 役满通常记为 13番 或更多
                total_han = 13 

            # --- 变量 5: Is_Dealer (庄家) ---
            is_dealer = 1 if winner == current_dealer else 0

            # --- 变量 6: Win_Type (自摸/荣和) ---
            # 自摸: who == fromWho
            win_type = 1 if winner == from_who else 0 # 1=Tsumo, 0=Ron

            # --- 变量 7: Junme (巡目) ---
            # 取 winner 当前摸了多少张牌
            junme = player_draw_counts[winner]
            if junme == 0: junme = 1 # 防止地和等极端情况

            # 收集一行数据
            row = {
                'Score': score,
                'Han': total_han,
                'Fu': fu,
                'Junme': junme,
                'Dora_Count': dora_count,
                'Is_Dealer': is_dealer,
                'Is_Riichi': is_riichi,
                'Win_Type': win_type
            }
            extracted_data.append(row)

    return extracted_data

def main():
    if not os.path.exists(INPUT_HTML):
        print(f"错误: 找不到文件 {INPUT_HTML}")
        return

    # 正则提取链接
    # 格式: <a href="http://tenhou.net/0/?log=20241231...">
    url_pattern = re.compile(r'http://tenhou\.net/0/\?log=[\w-]+')
    
    # 准备 CSV 写入
    headers = ['Score', 'Han', 'Fu', 'Junme', 'Dora_Count', 'Is_Dealer', 'Is_Riichi', 'Win_Type']
    
    all_rows = []
    log_count = 0
    
    print(f"正在读取 {INPUT_HTML} ...")
    with open(INPUT_HTML, 'r', encoding='utf-8') as f:
        for line in f:
            # 筛选房间：只看凤凰桌南场，保证数据水平
            if TARGET_ROOM not in line:
                continue
            
            match = url_pattern.search(line)
            if match:
                log_url = match.group(0)
                log_count += 1
                print(f"处理第 {log_count} 个牌谱: {log_url} ...", end="", flush=True)
                
                # 1. 下载
                xml_content = download_log(log_url)
                if not xml_content:
                    print(" [跳过]")
                    continue
                
                # 2. 解析
                rows = parse_xml_game(xml_content)
                all_rows.extend(rows)
                print(f" 提取了 {len(rows)} 次和牌")
                
                # 3. 延时
                time.sleep(DOWNLOAD_DELAY)
                
                if len(all_rows) > 3000:
                    print("\n数据量已足够 (N > 3000)，停止抓取。")
                    break

    # 保存 CSV
    if all_rows:
        print(f"\n正在保存 {len(all_rows)} 条数据到 {OUTPUT_CSV} ...")
        with open(OUTPUT_CSV, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(all_rows)
        print("完成！请检查 CSV 文件。")
    else:
        print("\n未提取到任何数据，请检查 HTML 文件或网络连接。")

if __name__ == "__main__":
    main()
