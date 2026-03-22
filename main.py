import os
import re
import sqlite3
import logging
import asyncio
import httpx  # Thêm thư viện này để tự ping (pip install httpx)
from datetime import datetime
from typing import Optional

import uvicorn
from fastapi import FastAPI, BackgroundTasks, Request
from telegram import (
    Update, 
    InlineKeyboardButton, 
    InlineKeyboardMarkup
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application, 
    CommandHandler, 
    CallbackQueryHandler, 
    ContextTypes
)

# ==========================================================
#                      CẤU HÌNH HỆ THỐNG
# ==========================================================
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

CONFIG = {
    "bot_token": "8560020347:AAECTuhAhuIvYz2pvDmwXS9mK4nEN-g-0EM",
    "admin_id": 7816353760,
    "admin_handle": "@nth_dev", 
    "bank_name": "MSB",
    "bank_bin": "970426",
    "bank_stk": "96886693002613",
    "bank_owner": "NGUYEN THANH HOP",
    "fee_min": 5000,
    "fee_percent": 0.01,
    "app_url": os.environ.get("APP_URL", "https://your-app-name.onrender.com"), # Cần điền URL Render vào env
    "log_channel": "@kiemtienonline48h", # === TÍNH NĂNG MỚI: Kênh thông báo GD thành công ===
    "aml_note": "⚠️ <b>LƯU Ý:</b> Hệ thống nghiêm cấm hành vi rửa tiền. Mọi nguồn tiền bẩn, tiền vi phạm pháp luật nếu bị phát hiện sẽ bị phong tỏa vĩnh viễn và cung cấp thông tin cho cơ quan chức năng."
}

DB_FILE = "system_v15.sqlite3"

class Status:
    PENDING = "CHO_THANH_TOAN"
    HOLDING = "BOT_DANG_GIU_TIEN"
    BUYER_DONE = "NGUOI_MUA_XAC_NHAN"
    PAYOUT_WAIT = "CHO_GIAI_NGAN"
    REFUND_WAIT = "CHO_HOAN_TIEN" 
    COMPLETED = "THANH_CONG"
    CANCELLED = "DA_HUY"
    REFUNDED = "DA_HOAN_TIEN"    

# ==========================================================
#                      DATABASE ARCHITECTURE
# ==========================================================
class Database:
    def __init__(self):
        self.conn = sqlite3.connect(DB_FILE, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._init_db()

    def _init_db(self):
        with self.conn:
            self.conn.execute('''CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT UNIQUE, group_id INTEGER, group_name TEXT,
                buyer_id INTEGER, buyer_name TEXT, buyer_user TEXT,
                seller_name TEXT, amount INTEGER, fee INTEGER, total_pay INTEGER,
                product_name TEXT, seller_bank_info TEXT, status TEXT, 
                qr_msg_id INTEGER, status_msg_id INTEGER, created_at TEXT)''')
            
            # === TÍNH NĂNG MỚI: BẢNG DỮ LIỆU NÂNG CẤP ===
            self.conn.execute('''CREATE TABLE IF NOT EXISTS blacklist (
                user_id INTEGER PRIMARY KEY, reason TEXT, created_at TEXT)''')
            self.conn.execute('''CREATE TABLE IF NOT EXISTS bot_groups (
                chat_id INTEGER PRIMARY KEY, chat_name TEXT)''')

    def create_trade(self, data):
        with self.conn:
            self.conn.execute("""INSERT INTO trades 
                (code, group_id, group_name, buyer_id, buyer_name, buyer_user, seller_name, 
                 amount, fee, total_pay, product_name, status, created_at) 
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (data['code'], data['group_id'], data['group_name'], data['buyer_id'], data['buyer_name'],
                 data['buyer_user'], data['seller_name'], data['amount'], data['fee'],
                 data['total_pay'], data['product_name'], Status.PENDING, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

    def get_trade(self, code):
        return self.conn.execute("SELECT * FROM trades WHERE code = ?", (code,)).fetchone()

    def update_trade(self, code, **kwargs):
        set_clause = ", ".join([f"{k} = ?" for k in kwargs.keys()])
        values = list(kwargs.values()) + [code]
        with self.conn:
            self.conn.execute(f"UPDATE trades SET {set_clause} WHERE code = ?", values)
    
    def get_stats(self):
        with self.conn:
            res = self.conn.execute("""SELECT 
                COUNT(*) as total_count, 
                SUM(amount) as total_amount, 
                SUM(fee) as total_fee 
                FROM trades WHERE status = ?""", (Status.COMPLETED,)).fetchone()
            return res

    # === TÍNH NĂNG MỚI: QUẢN LÝ DATABASE NÂNG CẤP ===
    def add_blacklist(self, user_id, reason):
        with self.conn:
            self.conn.execute("INSERT OR REPLACE INTO blacklist (user_id, reason, created_at) VALUES (?, ?, ?)", 
                              (user_id, reason, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

    def remove_blacklist(self, user_id):
        with self.conn:
            self.conn.execute("DELETE FROM blacklist WHERE user_id = ?", (user_id,))

    def is_blacklisted(self, user_id):
        res = self.conn.execute("SELECT reason FROM blacklist WHERE user_id = ?", (user_id,)).fetchone()
        return res['reason'] if res else None

    def add_group(self, chat_id, chat_name):
        with self.conn:
            self.conn.execute("INSERT OR IGNORE INTO bot_groups (chat_id, chat_name) VALUES (?, ?)", (chat_id, chat_name))

    def get_all_groups(self):
        return self.conn.execute("SELECT chat_id FROM bot_groups").fetchall()

    def get_top_buyers(self):
        return self.conn.execute("""
            SELECT buyer_name, COUNT(*) as count, SUM(amount) as total 
            FROM trades WHERE status = ? 
            GROUP BY buyer_id ORDER BY total DESC LIMIT 5
        """, (Status.COMPLETED,)).fetchall()

db = Database()
app = FastAPI()
tg_app = Application.builder().token(CONFIG["bot_token"]).build()

# ==========================================================
#                      CHỐNG TREO (KEEP ALIVE)
# ==========================================================
async def keep_alive():
    """Tự động ping để tránh Render tắt bot"""
    await asyncio.sleep(10) # Đợi bot khởi động xong
    while True:
        try:
            async with httpx.AsyncClient() as client:
                # Ping chính endpoint của bot
                response = await client.get(CONFIG["app_url"], timeout=10)
                logger.info(f"🔄 Keep-alive ping: {response.status_code}")
        except Exception as e:
            logger.error(f"❌ Keep-alive error: {e}")
        await asyncio.sleep(300) # Ping mỗi 5 phút

# ==========================================================
#                      WEBHOOK SEPAY (NHẬN BILL)
# ==========================================================
@app.get("/")
async def health_check():
    return {"status": "online", "timestamp": datetime.now().isoformat()}

@app.post("/webhook")
async def sepay_webhook(request: Request):
    try:
        data = await request.json()
        logger.info(f"📩 Webhook Incoming: {data}")
        content = str(data.get("content", "")).upper()
        
        raw_val = data.get("amount_in") or data.get("amount") or data.get("transferAmount") or "0"
        clean_val = re.sub(r"\D", "", str(raw_val))
        amount_in = int(clean_val) if clean_val else 0
        
        match = re.search(r"GD(\d+)", content)
        if match:
            code = f"GD{match.group(1)}"
            logger.info(f"🔔 BILL NHẬN: {code} | Số tiền: {amount_in}")
            asyncio.create_task(process_paid_invoice(code, amount_in))
        
        return {"status": "success"}
    except Exception as e:
        logger.error(f"Lỗi Webhook: {e}")
        return {"status": "error"}

async def process_paid_invoice(code, amount_received):
    trade = db.get_trade(code)
    if not trade or trade['status'] != Status.PENDING:
        return

    total_needed = int(trade['total_pay'])
    if int(amount_received) >= total_needed:
        db.update_trade(code, status=Status.HOLDING)
        
        try: await tg_app.bot.unpin_chat_message(chat_id=trade['group_id'], message_id=trade['qr_msg_id'])
        except: pass

        msg = f"""<b>✅ GIAO DỊCH {code} ĐÃ NHẬN ĐỦ TIỀN</b>
━━━━━━━━━━━━━━━━━━━━
📦 <b>Sản phẩm:</b> {trade['product_name']}
💰 <b>Số tiền nhận:</b> {amount_received:,} VND
🛡 <b>Trạng thái:</b> BOT ĐANG GIỮ TIỀN AN TOÀN

👤 <b>Người mua:</b> {trade['buyer_name']}
👤 <b>Người bán:</b> {trade['seller_name']}
━━━━━━━━━━━━━━━━━━━━
🚀 <b>YÊU CẦU:</b> Người bán tiến hành giao hàng. Sau khi xong, người mua bấm nút xác nhận dưới đây.
{CONFIG['aml_note']}"""
        
        btn = [[InlineKeyboardButton("✅ TÔI ĐÃ NHẬN ĐỦ HÀNG", callback_data=f"done_{code}")]]
        sent = await tg_app.bot.send_message(chat_id=trade['group_id'], text=msg, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(btn))
        db.update_trade(code, status_msg_id=sent.message_id)
        try: await tg_app.bot.pin_chat_message(chat_id=trade['group_id'], message_id=sent.message_id)
        except: pass
    else:
        missing = total_needed - amount_received
        txt = f"""<b>⚠️ CẢNH BÁO: CHUYỂN THIẾU TIỀN</b>
━━━━━━━━━━━━━━━━━━━━
🆔 <b>Mã đơn:</b> <code>{code}</code>
💰 <b>Cần thanh toán:</b> {total_needed:,} VND
📥 <b>Thực nhận từ bill:</b> {amount_received:,} VND
❌ <b>CÒN THIẾU:</b> <code>{missing:,}</code> VND

<i>Vui lòng chuyển thêm đúng số tiền thiếu với nội dung chuyển khoản là <code>{code}</code></i>"""
        await tg_app.bot.send_message(chat_id=trade['group_id'], text=txt, parse_mode=ParseMode.HTML)

# ==========================================================
#                      INTERFACE & COMMANDS
# ==========================================================
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # === TÍNH NĂNG MỚI: Tự động lưu Group để sau này Broadcast ===
    if update.effective_chat.type in ["group", "supergroup"]:
        db.add_group(update.effective_chat.id, update.effective_chat.title)

    bot_info = await context.bot.get_me()
    keyboard = [
        [InlineKeyboardButton("➕ Thêm Bot Vào Nhóm", url=f"https://t.me/{bot_info.username}?startgroup=true")],
        [InlineKeyboardButton("📖 Hướng Dẫn", callback_data="ui_help"), InlineKeyboardButton("📊 Thống Kê", callback_data="ui_stats")],
        [InlineKeyboardButton("👨‍💻 Liên Hệ Admin", url=f"https://t.me/{CONFIG['admin_handle'][1:]}")]
    ]
    
    txt = f"""<b>⚡ HỆ THỐNG TRUNG GIAN TỰ ĐỘNG 4.0</b>
━━━━━━━━━━━━━━━━━━━━
Chào mừng bạn đến với nền tảng Giao Dịch an toàn.

<b>💎 TÍNH NĂNG:</b>
• 🛡 <b>An Toàn:</b> Bot giữ tiền trung gian minh bạch.
• ⚡ <b>Tốc Độ:</b> Xác thực Bank tự động 100%.
• 🚫 <b>Phòng Chống:</b> Hệ thống quét tiền bẩn & lừa đảo.

{CONFIG['aml_note']}"""
    
    if update.callback_query:
        await update.callback_query.edit_message_text(txt, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text(txt, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML)

async def cmd_taogdtg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type == "private":
        return await update.message.reply_text("❌ Lệnh này chỉ dùng trong Nhóm Giao Dịch!")
    
    # === TÍNH NĂNG MỚI: Tự động lưu thông tin Group ===
    db.add_group(update.effective_chat.id, update.effective_chat.title)

    # === TÍNH NĂNG MỚI: Kiểm tra Blacklist Anti-Scam ===
    ban_reason = db.is_blacklisted(update.effective_user.id)
    if ban_reason:
        return await update.message.reply_text(f"⛔ <b>TÀI KHOẢN BỊ KHÓA</b>\nBạn nằm trong danh sách đen của hệ thống.\nLý do: <i>{ban_reason}</i>", parse_mode=ParseMode.HTML)

    try:
        parts = [p.strip() for p in update.message.text.replace("/taogdtg", "").split("|")]
        if len(parts) < 3: raise ValueError
        
        amount = int(re.sub(r"\D", "", parts[0]))
        product = parts[1]
        seller = parts[2] 
        
        if amount < 1000:
            return await update.message.reply_text("❌ Số tiền tối thiểu là 1,000 VND!")

        code = f"GD{int(datetime.now().timestamp())}"
        fee = max(CONFIG['fee_min'], int(amount * CONFIG['fee_percent']))
        total = amount + fee

        db.create_trade({
            "code": code, "group_id": update.effective_chat.id, "group_name": update.effective_chat.title,
            "buyer_id": update.effective_user.id, "buyer_name": update.effective_user.full_name,
            "buyer_user": f"@{update.effective_user.username}", "seller_name": seller,
            "amount": amount, "fee": fee, "total_pay": total, "product_name": product
        })

        qr = f"https://img.vietqr.io/image/{CONFIG['bank_bin']}-{CONFIG['bank_stk']}-compact2.png?amount={total}&addInfo={code}&accountName={CONFIG['bank_owner'].replace(' ', '%20')}"
        txt = f"""<b>🤝 ĐƠN GIAO DỊCH MỚI: {code}</b>
━━━━━━━━━━━━━━━━━━━━
📦 <b>Sản phẩm:</b> {product}
👤 <b>Người Bán:</b> {seller}
👤 <b>Người Mua:</b> {update.effective_user.full_name}
━━━━━━━━━━━━━━━━━━━━
💵 <b>Tiền hàng:</b> {amount:,} VND
⚙️ <b>Phí GD:</b> {fee:,} VND
💳 <b>TỔNG THANH TOÁN:</b> <code>{total:,}</code> VND
📝 <b>Nội dung:</b> <code>{code}</code>

{CONFIG['aml_note']}"""

        kb = [[InlineKeyboardButton("🔄 Lấy Lại Mã QR", callback_data=f"getqr_{code}"), InlineKeyboardButton("❌ Hủy Đơn", callback_data=f"cancel_{code}")]]
        msg = await update.message.reply_photo(photo=qr, caption=txt, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))
        db.update_trade(code, qr_msg_id=msg.message_id)
        try: await msg.pin() 
        except: pass
    except:
        await update.message.reply_text("❌ <b>Sai cú pháp!</b>\nSử dụng: <code>/taogdtg Tiền | Sản phẩm | @Seller</code>", parse_mode=ParseMode.HTML)

async def cmd_bank(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) < 2: 
        return await update.message.reply_text("❌ Cú pháp: <code>/bank [MãGD] [STK Bank Tên]</code>", parse_mode=ParseMode.HTML)
    
    code, info = context.args[0].upper(), " ".join(context.args[1:])
    trade = db.get_trade(code)
    
    if not trade:
        return await update.message.reply_text("❌ Không tìm thấy mã giao dịch này!")

    curr_user = f"@{update.effective_user.username}"
    if curr_user.lower() != trade['seller_name'].lower():
        return await update.message.reply_text(f"⛔ Quyền hạn: Chỉ người bán (<b>{trade['seller_name']}</b>) mới có quyền rút tiền đơn này!", parse_mode=ParseMode.HTML)

    if trade['status'] == Status.BUYER_DONE:
        db.update_trade(code, status=Status.PAYOUT_WAIT, seller_bank_info=info)
        kb = [[InlineKeyboardButton("✅ XÁC NHẬN ĐÃ BANK", callback_data=f"adminpayout_{code}")]]
        await context.bot.send_message(CONFIG['admin_id'], f"🏛 <b>YÊU CẦU RÚT TIỀN: {code}</b>\n💰 Tiền: {trade['amount']:,} VND\n💳 STK: {info}\n👥 Seller: {trade['seller_name']}\n📂 Nhóm: {trade['group_name']}", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))
        await update.message.reply_text("✅ <b>Yêu cầu thành công!</b>\nAdmin đang thực hiện chuyển khoản cho bạn.", parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text(f"❌ Trạng thái đơn không hợp lệ để rút tiền! (Hiện tại: {trade['status']})")

async def cmd_hoantien(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.photo:
        return await update.message.reply_text(
            "❌ <b>Yêu cầu hoàn tiền thất bại!</b>\nBạn bắt buộc phải gửi hình ảnh làm bằng chứng tranh chấp.\n\n"
            "📸 <b>Cách làm:</b> Gửi một bức ảnh bằng chứng và chèn vào phần *Caption* cú pháp sau:\n"
            "<code>/hoantien [MãGD] [STK Nhận Tiền] [Lý do hoàn]</code>", 
            parse_mode=ParseMode.HTML
        )
    
    caption = update.message.caption or ""
    parts = caption.split()
    
    if len(parts) < 4:
        return await update.message.reply_text("❌ Cú pháp caption không đúng!\nVí dụ: <code>/hoantien GD12345 0987654321 MBBank Người bán lừa đảo</code>", parse_mode=ParseMode.HTML)
    
    code = parts[1].upper()
    info = parts[2]
    reason = " ".join(parts[3:])
    
    trade = db.get_trade(code)
    
    if not trade:
        return await update.message.reply_text("❌ Đơn không tồn tại!")

    is_admin = update.effective_user.id == CONFIG['admin_id']
    is_buyer = update.effective_user.id == trade['buyer_id']
    
    if not (is_admin or is_buyer):
        return await update.message.reply_text("⛔ Chỉ người mua hoặc Admin mới có quyền yêu cầu hoàn tiền!")

    if trade['status'] in [Status.HOLDING, Status.BUYER_DONE, Status.PAYOUT_WAIT]:
        db.update_trade(code, status=Status.REFUND_WAIT, seller_bank_info=info)
        
        kb = [
            [InlineKeyboardButton("🔄 DUYỆT HOÀN TIỀN", callback_data=f"adminrefund_{code}")],
            [InlineKeyboardButton("❌ TỪ CHỐI & GIỮ TIỀN", callback_data=f"rejectrefund_{code}")]
        ]
        
        admin_msg = f"""🚨 <b>YÊU CẦU HOÀN TIỀN CÓ TRANH CHẤP: {code}</b>
━━━━━━━━━━━━━━━━━━━━
💰 <b>Số tiền cần hoàn:</b> {trade['total_pay']:,} VND
💳 <b>STK Nhận:</b> {info}
📝 <b>Lý do khiếu nại:</b> {reason}
👤 <b>Người khiếu nại:</b> {trade['buyer_name']}
📂 <b>Nhóm GD:</b> {trade['group_name']}
📸 <i>Bằng chứng được đính kèm bên trên.</i>"""
        
        await context.bot.send_photo(
            chat_id=CONFIG['admin_id'], 
            photo=update.message.photo[-1].file_id, 
            caption=admin_msg, 
            parse_mode=ParseMode.HTML, 
            reply_markup=InlineKeyboardMarkup(kb)
        )
        
        await update.message.reply_text("✅ <b>Đã gửi yêu cầu hoàn tiền kèm bằng chứng!</b>\nAdmin sẽ xem xét hình ảnh và lý do để đưa ra quyết định.", parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text(f"❌ Trạng thái đơn không thể hoàn tiền! ({trade['status']})")

async def cmd_check(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args: return await update.message.reply_text("❌ Vui lòng nhập mã đơn!")
    code = context.args[0].upper()
    trade = db.get_trade(code)
    if not trade: return await update.message.reply_text("❌ Đơn không tồn tại!")
    
    txt = f"""<b>🔍 THÔNG TIN ĐƠN: {code}</b>
━━━━━━━━━━━━━━━━━━━━
📦 <b>SP:</b> {trade['product_name']}
💵 <b>Số tiền:</b> {trade['amount']:,} VND
🛡 <b>Trạng thái:</b> <code>{trade['status']}</code>
⏰ <b>Ngày tạo:</b> {trade['created_at']}
👤 <b>Bán:</b> {trade['seller_name']}
👤 <b>Mua:</b> {trade['buyer_name']}"""
    await update.message.reply_text(txt, parse_mode=ParseMode.HTML)

async def cmd_huy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args: return await update.message.reply_text("❌ Vui lòng nhập mã đơn cần hủy!")
    code = context.args[0].upper()
    trade = db.get_trade(code)
    if not trade: return await update.message.reply_text("❌ Đơn không tồn tại!")
    
    user_id = update.effective_user.id
    curr_user = f"@{update.effective_user.username}".lower()
    
    if user_id == trade['buyer_id'] or curr_user == trade['seller_name'].lower():
        if trade['status'] == Status.PENDING:
            db.update_trade(code, status=Status.CANCELLED)
            await update.message.reply_text(f"✅ Đã hủy giao dịch {code} thành công!")
        else:
            await update.message.reply_text("❌ Chỉ có thể hủy khi đơn đang ở trạng thái Chờ Thanh Toán!")
    else:
        await update.message.reply_text("⛔ Bạn không có quyền hủy đơn này!")

async def cmd_thongke(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != CONFIG['admin_id']: return
    s = db.get_stats()
    txt = f"""<b>📊 THỐNG KÊ HỆ THỐNG</b>
━━━━━━━━━━━━━━━━━━━━
✅ <b>Đơn thành công:</b> {s['total_count'] or 0} đơn
💰 <b>Tổng tiền hàng:</b> {s['total_amount'] or 0:,} VND
💎 <b>Phí thu được:</b> {s['total_fee'] or 0:,} VND"""
    await update.message.reply_text(txt, parse_mode=ParseMode.HTML)

async def cmd_lichsu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    curr_user = f"@{update.effective_user.username}"
    
    with db.conn:
        res = db.conn.execute(
            "SELECT code, product_name, amount, status FROM trades WHERE buyer_id = ? OR seller_name = ? ORDER BY id DESC LIMIT 5", 
            (user_id, curr_user)
        ).fetchall()
    
    if not res:
        return await update.message.reply_text("📭 Bạn chưa có giao dịch nào gần đây trên hệ thống.")
        
    txt = "<b>🕒 5 GIAO DỊCH GẦN NHẤT CỦA BẠN:</b>\n━━━━━━━━━━━━━━━━━━━━\n"
    for r in res:
        txt += f"🔸 <b>{r['code']}</b> | {r['product_name']}\n💵 {r['amount']:,} VND - 📌 <code>{r['status']}</code>\n\n"
    
    await update.message.reply_text(txt, parse_mode=ParseMode.HTML)

async def cmd_cskh(update: Update, context: ContextTypes.DEFAULT_TYPE):
    txt = f"""🎧 <b>HỖ TRỢ KHÁCH HÀNG 24/7</b>
━━━━━━━━━━━━━━━━━━━━
Nếu bạn gặp vấn đề với giao dịch, nạp sai tiền, hoặc có khiếu nại tranh chấp, vui lòng liên hệ Admin qua kênh sau:

👨‍💻 <b>Admin:</b> {CONFIG['admin_handle']}

<i>Lưu ý: Để được hỗ trợ nhanh nhất, vui lòng cung cấp kèm [Mã GD] và [Hình ảnh bill/bằng chứng] khi nhắn tin cho Admin.</i>"""
    await update.message.reply_text(txt, parse_mode=ParseMode.HTML)

# ==========================================================
#         TÍNH NĂNG NÂNG CẤP LÊN TẦM CAO MỚI (ADD-ONS)
# ==========================================================
async def cmd_top(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Bảng xếp hạng mức độ uy tín"""
    top_users = db.get_top_buyers()
    if not top_users:
        return await update.message.reply_text("📊 Hệ thống chưa có đủ dữ liệu để xếp hạng.")
    
    txt = "🏆 <b>BẢNG XẾP HẠNG KHÁCH HÀNG VIP (THEO VOLUME)</b>\n━━━━━━━━━━━━━━━━━━━━\n"
    medals = ["🥇", "🥈", "🥉", "🏅", "🎖"]
    for i, user in enumerate(top_users):
        txt += f"{medals[i]} <b>{user['buyer_name']}</b>\n└ <i>{user['count']} giao dịch</i> | 💰 <b>{user['total']:,} VND</b>\n\n"
    
    await update.message.reply_text(txt, parse_mode=ParseMode.HTML)

async def cmd_ban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin chặn kẻ lừa đảo"""
    if update.effective_user.id != CONFIG['admin_id']: return
    if len(context.args) < 2:
        return await update.message.reply_text("❌ Cú pháp: <code>/ban [User_ID] [Lý do]</code>", parse_mode=ParseMode.HTML)
    
    target_id = int(context.args[0])
    reason = " ".join(context.args[1:])
    db.add_blacklist(target_id, reason)
    await update.message.reply_text(f"🛑 Đã cho <b>{target_id}</b> vào sổ đen.\nLý do: <i>{reason}</i>", parse_mode=ParseMode.HTML)

async def cmd_unban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin mở khóa"""
    if update.effective_user.id != CONFIG['admin_id']: return
    if not context.args:
        return await update.message.reply_text("❌ Cú pháp: <code>/unban [User_ID]</code>", parse_mode=ParseMode.HTML)
    
    target_id = int(context.args[0])
    db.remove_blacklist(target_id)
    await update.message.reply_text(f"✅ Đã gỡ Blacklist cho ID: <b>{target_id}</b>", parse_mode=ParseMode.HTML)

async def cmd_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Phát sóng tin nhắn tới mọi Group bot đang có mặt (Tính năng Kéo Mem)"""
    if update.effective_user.id != CONFIG['admin_id']: return
    if not context.args:
        return await update.message.reply_text("❌ Cú pháp: <code>/broadcast [Nội dung thông báo]</code>", parse_mode=ParseMode.HTML)
    
    msg = update.message.text.replace("/broadcast", "").strip()
    groups = db.get_all_groups()
    success = 0
    
    for g in groups:
        try:
            await context.bot.send_message(g['chat_id'], f"📢 <b>THÔNG BÁO TỪ HỆ THỐNG:</b>\n━━━━━━━━━━━━━━━━━━━━\n{msg}", parse_mode=ParseMode.HTML)
            success += 1
            await asyncio.sleep(0.5) # Tránh bị Telegram Rate Limit
        except: pass
        
    await update.message.reply_text(f"🚀 <b>Broadcast hoàn tất!</b>\nĐã gửi thành công tới {success}/{len(groups)} nhóm.", parse_mode=ParseMode.HTML)

# ==========================================================
#                      CALLBACKS
# ==========================================================
async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data
    user_id = update.effective_user.id
    username = f"@{update.effective_user.username}"

    if data == "ui_help":
        txt = """<b>📖 HƯỚNG DẪN QUY TRÌNH GD</b>
━━━━━━━━━━━━━━━━━━━━
1️⃣ <b>Tạo đơn:</b> Dùng <code>/taogdtg Tiền | SP | @Seller</code>
2️⃣ <b>Thanh toán:</b> Người mua Quét mã QR chuyển tiền cho Bot.
3️⃣ <b>Giao hàng:</b> Bot nhận tiền -> Báo người bán giao hàng.
4️⃣ <b>Xác nhận:</b> Người mua nhận xong bấm <b>[Đã nhận hàng]</b>.
5️⃣ <b>Rút tiền:</b> Người bán dùng <code>/bank</code> để nhận tiền về STK.
🆘 <b>Hoàn tiền:</b> Nếu có tranh chấp gửi ảnh kèm cap <code>/hoantien</code>."""
        await query.edit_message_text(txt, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Quay Lại", callback_data="ui_back")]]))

    elif data == "ui_back":
        await cmd_start(update, context)

    elif data.startswith("getqr_"):
        code = data.split("_")[1]
        trade = db.get_trade(code)
        if trade:
            qr = f"https://img.vietqr.io/image/{CONFIG['bank_bin']}-{CONFIG['bank_stk']}-compact2.png?amount={trade['total_pay']}&addInfo={code}&accountName={CONFIG['bank_owner'].replace(' ', '%20')}"
            await query.message.reply_photo(photo=qr, caption=f"🔄 Mã QR của đơn <b>{code}</b>", parse_mode=ParseMode.HTML)
            await query.answer()

    elif data.startswith("cancel_"):
        code = data.split("_")[1]
        trade = db.get_trade(code)
        if trade and (user_id == trade['buyer_id'] or username.lower() == trade['seller_name'].lower()):
            if trade['status'] == Status.PENDING:
                db.update_trade(code, status=Status.CANCELLED)
                await query.edit_message_caption("❌ Giao dịch này đã được hủy bởi người trong cuộc.")
            else: await query.answer("❌ Không thể hủy đơn này!", show_alert=True)
        else: await query.answer("⛔ Bạn không có quyền!", show_alert=True)

    elif data.startswith("done_"):
        code = data.split("_")[1]
        trade = db.get_trade(code)
        if not trade: return await query.answer("❌ Đơn không tồn tại!")
        if user_id != trade['buyer_id']:
            return await query.answer("⛔ Chỉ người mua mới được xác nhận nhận hàng!", show_alert=True)
        
        if trade['status'] == Status.HOLDING:
            db.update_trade(code, status=Status.BUYER_DONE)
            await query.answer("✅ Đã xác nhận! Chờ người bán rút tiền.", show_alert=True)
            txt = f"<b>📦 GIAO DỊCH {code} HOÀN TẤT</b>\n\nNgười bán {trade['seller_name']} vui lòng rút tiền bằng cú pháp:\n<code>/bank {code} [STK Bank Tên]</code>"
            if query.message.photo: await query.edit_message_caption(caption=txt, parse_mode=ParseMode.HTML)
            else: await query.edit_message_text(text=txt, parse_mode=ParseMode.HTML)
        else:
            await query.answer("⚠️ Trạng thái đơn không hợp lệ!", show_alert=True)

    elif data.startswith("adminpayout_"):
        if user_id != CONFIG['admin_id']: return await query.answer("⛔ Bạn không có quyền!", show_alert=True)
        code = data.split("_")[1]
        trade = db.get_trade(code)
        if trade['status'] == Status.PAYOUT_WAIT:
            db.update_trade(code, status=Status.COMPLETED)
            await query.answer("✅ Đã đánh dấu hoàn tất rút tiền!")
            
            txt_admin = f"✅ <b>ĐÃ GIẢI NGÂN THÀNH CÔNG ĐƠN {code}</b> cho người bán."
            if query.message.photo: await query.edit_message_caption(caption=txt_admin, parse_mode=ParseMode.HTML)
            else: await query.edit_message_text(text=txt_admin, parse_mode=ParseMode.HTML)
            
            await context.bot.send_message(trade['group_id'], f"<b>✅ GIẢI NGÂN THÀNH CÔNG: Giao dịch {code}</b>\nAdmin đã chuyển tiền cho người bán {trade['seller_name']}. Cảm ơn các bạn đã sử dụng dịch vụ!", parse_mode=ParseMode.HTML)
            
            # === TÍNH NĂNG MỚI: Log các đơn thành công ra Channel Truyền Thông ===
            if CONFIG.get("log_channel"):
                log_txt = f"""🎉 <b>GIAO DỊCH THÀNH CÔNG</b> 🎉
━━━━━━━━━━━━━━━━━━━━
📦 <b>Sản phẩm:</b> {trade['product_name']}
💵 <b>Trị giá:</b> {trade['amount']:,} VND
🤝 <b>Bên bán:</b> {trade['seller_name']}

🛡 <i>Giao dịch trung gian uy tín, tự động 100% qua Bot!</i>"""
                try: await context.bot.send_message(CONFIG["log_channel"], log_txt, parse_mode=ParseMode.HTML)
                except Exception as e: logger.error(f"Lỗi gửi log channel: {e}")
                
        else:
            await query.answer("⚠️ Đơn không ở trạng thái chờ rút tiền!", show_alert=True)

    elif data.startswith("adminrefund_"):
        if user_id != CONFIG['admin_id']: return await query.answer("⛔ Bạn không có quyền!", show_alert=True)
        code = data.split("_")[1]
        trade = db.get_trade(code)
        if trade['status'] == Status.REFUND_WAIT:
            db.update_trade(code, status=Status.REFUNDED)
            await query.answer("✅ Đã đánh dấu hoàn tiền cho người mua!")
            
            txt_admin = f"✅ <b>ĐÃ HOÀN TIỀN THÀNH CÔNG ĐƠN {code}</b> cho người mua."
            if query.message.photo: await query.edit_message_caption(caption=txt_admin, parse_mode=ParseMode.HTML)
            else: await query.edit_message_text(text=txt_admin, parse_mode=ParseMode.HTML)
            
            await context.bot.send_message(trade['group_id'], f"<b>↩️ HOÀN TIỀN THÀNH CÔNG: Giao dịch {code}</b>\nAdmin đã giải quyết khiếu nại và hoàn tiền cho người mua.\nTrạng thái: Đã kết thúc.", parse_mode=ParseMode.HTML)
        else:
            await query.answer("⚠️ Đơn này không ở trạng thái chờ hoàn tiền!", show_alert=True)

    elif data.startswith("rejectrefund_"):
        if user_id != CONFIG['admin_id']: return await query.answer("⛔ Bạn không có quyền!", show_alert=True)
        code = data.split("_")[1]
        trade = db.get_trade(code)
        if trade['status'] == Status.REFUND_WAIT:
            db.update_trade(code, status=Status.HOLDING) 
            await query.answer("✅ Đã từ chối hoàn tiền!")
            
            txt_admin = f"❌ <b>ĐÃ TỪ CHỐI YÊU CẦU HOÀN TIỀN ĐƠN {code}</b>"
            if query.message.photo: await query.edit_message_caption(caption=txt_admin, parse_mode=ParseMode.HTML)
            else: await query.edit_message_text(text=txt_admin, parse_mode=ParseMode.HTML)
            
            await context.bot.send_message(trade['group_id'], f"<b>❌ TỪ CHỐI KHIẾU NẠI: Giao dịch {code}</b>\nAdmin đã xem xét bằng chứng và từ chối yêu cầu hoàn tiền. Tiền hiện vẫn đang được Bot giam giữ an toàn.", parse_mode=ParseMode.HTML)
        else:
            await query.answer("⚠️ Đơn này không ở trạng thái chờ hoàn tiền!", show_alert=True)

# ==========================================================
#                      RUNNER
# ==========================================================
async def main_runner():
    # Đăng ký các Handler
    tg_app.add_handler(CommandHandler("start", cmd_start))
    tg_app.add_handler(CommandHandler("taogdtg", cmd_taogdtg))
    tg_app.add_handler(CommandHandler("bank", cmd_bank))
    tg_app.add_handler(CommandHandler("hoantien", cmd_hoantien)) 
    tg_app.add_handler(CommandHandler("check", cmd_check))
    tg_app.add_handler(CommandHandler("huy", cmd_huy))
    tg_app.add_handler(CommandHandler("thongke", cmd_thongke))
    tg_app.add_handler(CommandHandler("lichsu", cmd_lichsu)) 
    tg_app.add_handler(CommandHandler("cskh", cmd_cskh))     
    
    # === ĐĂNG KÝ HANDLER NÂNG CẤP ===
    tg_app.add_handler(CommandHandler("top", cmd_top))
    tg_app.add_handler(CommandHandler("ban", cmd_ban))
    tg_app.add_handler(CommandHandler("unban", cmd_unban))
    tg_app.add_handler(CommandHandler("broadcast", cmd_broadcast))

    tg_app.add_handler(CallbackQueryHandler(callback_handler))
    
    # Khởi tạo bot
    await tg_app.initialize()
    await tg_app.start()
    
    # Chạy Polling cho Telegram trong background
    asyncio.create_task(tg_app.updater.start_polling())
    # Chạy task keep-alive để bot không bị treo
    asyncio.create_task(keep_alive())
    
    logger.info("🤖 Bot Telegram is running...")
    
    # Chạy Webhook Server (FastAPI)
    port = int(os.environ.get("PORT", 8080)) 
    config = uvicorn.Config(app, host="0.0.0.0", port=port, loop="asyncio")
    server = uvicorn.Server(config)
    
    logger.info(f"🌐 Webhook Server running on port {port}")
    await server.serve()

if __name__ == "__main__":
    try:
        asyncio.run(main_runner())
    except KeyboardInterrupt:
        pass
