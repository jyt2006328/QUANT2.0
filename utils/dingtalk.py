import time
import hmac
import hashlib
import base64
import urllib.parse
import requests
import json

class DingTalkBot:
    """
    [工具] 钉钉机器人助手
    负责将交易信号和系统状态推送到手机。
    """
    def __init__(self, config):
        self.enabled = config.get('enabled', False)
        self.webhook = config.get('webhook', '')
        self.secret = config.get('secret', '')

    def _get_signed_url(self):
        """生成带签名的请求 URL"""
        timestamp = str(round(time.time() * 1000))
        secret_enc = self.secret.encode('utf-8')
        string_to_sign = '{}\n{}'.format(timestamp, self.secret)
        string_to_sign_enc = string_to_sign.encode('utf-8')
        hmac_code = hmac.new(secret_enc, string_to_sign_enc, digestmod=hashlib.sha256).digest()
        sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
        return f"{self.webhook}&timestamp={timestamp}&sign={sign}"

    def send_markdown(self, title, text):
        """发送 Markdown 消息"""
        if not self.enabled or not self.webhook:
            return
            
        try:
            url = self._get_signed_url()
            headers = {'Content-Type': 'application/json'}
            data = {
                "msgtype": "markdown",
                "markdown": {
                    "title": title,
                    "text": text
                }
            }
            resp = requests.post(url, headers=headers, data=json.dumps(data), timeout=5)
            if resp.json().get('errcode') != 0:
                print(f"⚠️ DingTalk Error: {resp.text}")
        except Exception as e:
            print(f"⚠️ DingTalk Failed: {e}")

    def send_trade_alert(self, symbol, side, price, amount, value, strategy_tag):
        """发送交易专用模版"""
        color = "#00FF00" if side.upper() == 'BUY' else "#FF0000"
        emoji = "🟢" if side.upper() == 'BUY' else "🔴"
        
        text = (
            f"### {emoji} {strategy_tag} 信号触发\n"
            f"**标的**: {symbol}\n"
            f"**方向**: <font color='{color}'>{side.upper()}</font>\n"
            f"**价格**: {price:.4f}\n"
            f"**数量**: {amount:.4f} ({value:.2f} U)\n"
            f"**时间**: {time.strftime('%H:%M:%S')}\n"
            f"> QuantBot V12"
        )
        self.send_markdown(f"交易: {symbol}", text)

    def send_sop_update(self, mode, risk, trend_mult):
        """发送 SOP 变动通知"""
        text = (
            f"### 🧠 AI 指挥官指令更新\n"
            f"**市场状态**: `{mode.upper()}`\n"
            f"**风控偏好**: `{risk.upper()}`\n"
            f"**Trend 杠杆**: `x{trend_mult}`\n"
            f"> 策略权重已自动重配"
        )
        self.send_markdown("SOP 更新", text)