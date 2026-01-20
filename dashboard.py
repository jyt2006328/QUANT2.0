import streamlit as st
import pandas as pd
import sqlite3
import json
import os
import time
import re
import subprocess  # [新增] 用于执行系统命令
from datetime import datetime

# ================= 页面配置 =================
st.set_page_config(
    page_title="QuantBot 指挥室",
    layout="wide",
    page_icon="🛸",
    initial_sidebar_state="expanded"
)

# ================= 🔐 安全门禁 =================
# 这里设置你的密码，建议复杂一点
ADMIN_PASSWORD = "zzhwan" 

def check_password():
    """Returns `True` if the user had a correct password."""
    if "password_correct" not in st.session_state:
        st.session_state.password_correct = False

    if st.session_state.password_correct:
        return True

    # 显示输入框
    st.markdown("### 🛑 绝密区域: 请验证身份")
    pwd = st.text_input("请输入指挥官密码", type="password")
    
    if st.button("登录"):
        if pwd == ADMIN_PASSWORD:
            st.session_state.password_correct = True
            st.rerun() # 刷新页面进入
        else:
            st.error("密码错误，已记录 IP。")
            
    return False

if not check_password():
    st.stop() # ⛔ 停止渲染后续所有内容！
# ===============================================

# ================= 路径配置 =================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, 'data', 'market_data.db')
STATE_PATH = os.path.join(BASE_DIR, 'bot_state.json')
CONFIG_PATH = os.path.join(BASE_DIR, 'config.json')

# 日志路径 (Systemd 标准路径)
MAIN_LOG_FILE = '/var/log/quantbot.log'
# 备用路径列表
LOG_FILES = [
    MAIN_LOG_FILE,
    os.path.join(BASE_DIR, 'bot.log'),
    os.path.join(BASE_DIR, 'quantbot.log')
]

# ================= 核心函数 =================

def load_config():
    try:
        if os.path.exists(CONFIG_PATH):
            with open(CONFIG_PATH, 'r') as f: return json.load(f)
    except: pass
    return {}

def load_state():
    try:
        if os.path.exists(STATE_PATH):
            with open(STATE_PATH, 'r') as f: return json.load(f)
    except: pass
    return {}

def save_config(new_config):
    try:
        with open(CONFIG_PATH, 'w') as f:
            json.dump(new_config, f, indent=4)
        st.toast("✅ 配置已保存，下个周期自动生效！")
        time.sleep(1) # 给一点时间写入
    except Exception as e:
        st.error(f"保存失败: {e}")

def read_raw_logs(lines_limit=100):
    """
    [新增] 读取原始日志文件 (模拟 tail -n)
    """
    content = ""
    # 优先读取 Systemd 日志文件
    target_file = MAIN_LOG_FILE if os.path.exists(MAIN_LOG_FILE) else LOG_FILES[1]
    
    if os.path.exists(target_file):
        try:
            # 使用二进制模式读取防止编码错误，只读最后 N 字节然后解码
            # 这里简单处理：读取最后 N 行
            with open(target_file, 'r', encoding='utf-8', errors='ignore') as f:
                # 这种方式对于大文件可能慢，但在日志轮转机制下通常还好
                lines = f.readlines()
                last_lines = lines[-lines_limit:]
                content = "".join(last_lines)
        except Exception as e:
            content = f"Error reading logs: {e}"
    else:
        content = "⚠️ Log file not found."
    return content

def load_trade_history():
    """解析日志获取结构化交易记录"""
    trades = []
    pattern = re.compile(r'(\d{2}:\d{2}:\d{2})\s+.*🚀\s+\[(.*?)\]\s+(.*?):\s+(BUY|SELL)\s+([\d.]+)\s+\(\$([\d.]+)\)')
    
    # 遍历所有可能的日志源
    for log_path in LOG_FILES:
        if os.path.exists(log_path):
            try:
                with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
                    lines = f.readlines()[-2000:] 
                    for line in lines:
                        match = pattern.search(line)
                        if match:
                            t, mode, sym, side, amt, val = match.groups()
                            trades.append({
                                'Time': t, 'Mode': mode, 'Symbol': sym, 
                                'Side': side, 'Amount': amt, 'Value': float(val)
                            })
            except: pass
            
    if trades:
        return pd.DataFrame(trades).iloc[::-1] 
    return pd.DataFrame()

# ================= 侧边栏：指挥官控制台 =================

st.sidebar.title("🛸 QuantBot V18")

# 加载最新配置
config = load_config()
alloc = config.get('strategy_allocation', {})

# --- 1. 动态参数调优 (Config Tuner) ---
with st.sidebar.expander("🎛️ 策略参数调优", expanded=False):
    st.caption("修改后点击保存，无需重启")
    
    # 使用 Form 表单，防止每动一下滑块就刷新页面
    with st.form("config_form"):
        # A. 杠杆限制
        new_lev = st.number_input("全局杠杆上限 (x)", min_value=1, max_value=20, value=int(config.get('leverage_limit', 5)))
        
        st.markdown("---")
        st.caption("🤖 AI Alpha")
        ai_w = st.slider("AI 权重", 0.0, 2.0, float(alloc.get('ai_weight', 0.0)), 0.1)
        ai_c = st.slider("AI 资金上限", 0.1, 1.0, float(alloc.get('ai_cap', 0.2)), 0.1)
        
        st.markdown("---")
        st.caption("🔫 Sniper")
        sn_w = st.slider("Sniper 权重", 0.0, 2.0, float(alloc.get('sniper_weight', 1.0)), 0.1)
        sn_c = st.slider("Sniper 资金上限", 0.1, 1.0, float(alloc.get('sniper_cap', 0.4)), 0.1)
        
        st.markdown("---")
        st.caption("☁️ QingYun")
        qy_w = st.slider("青云 权重", 0.0, 2.0, float(alloc.get('qingyun_weight', 0.5)), 0.1)
        qy_c = st.slider("青云 资金上限", 0.1, 1.0, float(alloc.get('qingyun_cap', 0.3)), 0.1)
        
        st.markdown("---")
        st.caption("🌊 Resonance")
        res_w = st.slider("Resonance 权重", 0.0, 2.0, float(alloc.get('resonance_weight', 0.3)), 0.1)
        res_c = st.slider("Resonance 资金上限", 0.1, 1.0, float(alloc.get('resonance_cap', 0.5)), 0.1)
        
        st.markdown("---")
        st.caption("🛡️ Trend / Pair / Basket")
        tr_w = st.slider("Trend 权重", 0.0, 2.0, float(alloc.get('trend_weight', 0.2)), 0.1)
        # Pair/Basket 暂时只给开关或小权重，以免太乱，或者也加上
        
        submitted = st.form_submit_button("💾 保存配置")
        
        if submitted:
            # 更新 Config 字典
            config['leverage_limit'] = new_lev
            config['strategy_allocation']['ai_weight'] = ai_w
            config['strategy_allocation']['ai_cap'] = ai_c
            config['strategy_allocation']['sniper_weight'] = sn_w
            config['strategy_allocation']['sniper_cap'] = sn_c
            config['strategy_allocation']['qingyun_weight'] = qy_w
            config['strategy_allocation']['qingyun_cap'] = qy_c
            config['strategy_allocation']['resonance_weight'] = res_w
            config['strategy_allocation']['resonance_cap'] = res_c
            config['strategy_allocation']['trend_weight'] = tr_w
            
            # 执行保存
            save_config(config)
            st.rerun()

# --- 2. 运维控制 ---
st.sidebar.subheader("⚙️ 运维控制")
col_btn1, col_btn2 = st.sidebar.columns(2)
if col_btn1.button("🔄 重启机器人"):
    subprocess.run(["systemctl", "restart", "quantbot"])
    st.sidebar.success("已发送重启指令")
if col_btn2.button("🛑 停止机器人"):
    subprocess.run(["systemctl", "stop", "quantbot"])
    st.sidebar.warning("已发送停止指令")

# --- 3. 状态监测 ---
state = load_state()
last_update_str = state.get('timestamp', '')
is_alive = False
time_diff_str = "Unknown"

if last_update_str:
    try:
        last_dt = datetime.fromisoformat(last_update_str)
        now_dt = datetime.now()
        diff_seconds = (now_dt - last_dt).total_seconds()
        if diff_seconds < 120: 
            is_alive = True
            time_diff_str = f"{int(diff_seconds)}s ago"
        else:
            time_diff_str = f"{int(diff_seconds/60)}m ago"
    except: pass

if is_alive:
    st.sidebar.success(f"🟢 **ONLINE** ({time_diff_str})")
else:
    st.sidebar.error(f"🔴 **OFFLINE** ({time_diff_str})")

st.sidebar.markdown("---")

# --- 4. 策略权重概览 ---
st.sidebar.caption("策略分配")
c1, c2 = st.sidebar.columns(2)
c1.metric("Trend", f"{alloc.get('trend_weight',0):.1%}")
c2.metric("Sniper", f"{alloc.get('sniper_weight',0):.1%}")
c3, c4 = st.sidebar.columns(2)
c3.metric("Resonance", f"{alloc.get('resonance_weight',0):.1%}")
c4.metric("AI Alpha", f"{alloc.get('ai_weight',0):.1%}")

# 自动刷新开关
auto_refresh = st.sidebar.checkbox("启用自动刷新 (30s)", value=False)
if auto_refresh:
    time.sleep(30)
    st.rerun()

# ================= 主界面 =================

# 顶部 KPI
k1, k2, k3, k4 = st.columns(4)
with k1:
    st.metric("📡 系统状态", "运行中" if is_alive else "已停止")
with k2:
    sniper_count = len(state.get('sniper_positions', {}))
    st.metric("🔫 Sniper 持仓", f"{sniper_count}")
with k3:
    res_count = len(state.get('resonance_positions', {}))
    st.metric("🌊 Resonance 持仓", f"{res_count}")
with k4:
    # 简单的 AI 信号指示
    # 这里只是占位，实际可以读最新的 AI 预测
    st.metric("🧠 AI 模型", "Ready")

# 核心页面
tab1, tab2, tab3 = st.tabs(["🔭 持仓详情", "🖥️ 系统终端 (Logs)", "📜 交易流水"])

with tab1:
    st.subheader("🔫 Sniper Strategy")
    snipers = state.get('sniper_positions', {})
    if snipers:
        st.dataframe(pd.DataFrame.from_dict(snipers, orient='index'), width='stretch')
    else:
        st.caption("暂无持仓")
            
    st.markdown("---")

    st.subheader("🌊 Resonance Strategy")
    resonances = state.get('resonance_positions', {})
    if resonances:
        st.dataframe(pd.DataFrame.from_dict(resonances, orient='index'), width='stretch')
    else:
        st.caption("暂无持仓")

with tab2:
    st.subheader("System Logs (Real-time)")
    
    # 增加手动刷新按钮，方便看日志
    if st.button("刷新日志"):
        st.rerun()
        
    # 读取原始日志
    raw_logs = read_raw_logs(lines_limit=200) # 读取最后 200 行
    
    # 使用代码块展示，保留格式
    st.code(raw_logs, language="text", line_numbers=True)
    
    st.caption(f"Log Source: {MAIN_LOG_FILE}")

with tab3:
    st.subheader("交易历史")
    df_trades = load_trade_history()
    if not df_trades.empty:
        st.dataframe(df_trades, height=800, width='stretch', hide_index=True)
    else:
        st.info("暂无记录")