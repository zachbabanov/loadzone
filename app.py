import json
import threading
import os
import smtplib
import ssl
import sqlite3
from email.message import EmailMessage
from datetime import datetime, timedelta
from flask import Flask, jsonify, request, send_from_directory
from flask_socketio import SocketIO
from apscheduler.schedulers.background import BackgroundScheduler

DB_FILE = 'data.sqlite'
LEGACY_JSON = 'data.json'

app = Flask(__name__, static_folder='static', template_folder='templates')
socketio = SocketIO(app, cors_allowed_origins='*')
scheduler = BackgroundScheduler()
scheduler.start()

db_lock = threading.Lock()


def send_email_notification(to_email: str, subject: str, body: str):
    """
    Отправляет письмо на to_email в отдельном потоке.
    Настройки берутся из переменных окружения:
      SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS, SMTP_FROM (опционально)
    Если переменные не заданы — функция тихо завершится.
    """
    host = os.environ.get('SMTP_HOST')
    port = os.environ.get('SMTP_PORT')
    user = os.environ.get('SMTP_USER')
    password = os.environ.get('SMTP_PASS')
    sender = os.environ.get('SMTP_FROM') or user

    if not (host and port and user and password and sender and to_email):
        app.logger.debug("SMTP credentials incomplete; skipping email to %s", to_email)
        return

    def _send():
        try:
            port_int = int(port)
        except Exception:
            port_int = 465

        msg = EmailMessage()
        msg['Subject'] = subject
        msg['From'] = sender
        msg['To'] = to_email
        msg.set_content(body)

        try:
            if port_int == 465:
                context = ssl.create_default_context()
                with smtplib.SMTP_SSL(host, port_int, context=context) as server:
                    server.login(user, password)
                    server.send_message(msg)
            else:
                with smtplib.SMTP(host, port_int) as server:
                    server.ehlo()
                    server.starttls(context=ssl.create_default_context())
                    server.ehlo()
                    server.login(user, password)
                    server.send_message(msg)
            app.logger.debug("Email sent to %s (subject=%s)", to_email, subject)
        except Exception as exc:
            app.logger.exception("Failed to send email to %s: %s", to_email, str(exc))

    t = threading.Thread(target=_send, daemon=True)
    t.start()


def get_conn():
    """Return a new sqlite3 connection. Use row_factory to get dict-like rows."""
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db_and_migrate():
    """
    Create tables if not exist. If legacy JSON exists and DB is empty, migrate it.
    This function is safe to call multiple times.
    """
    with db_lock:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("PRAGMA journal_mode=WAL;")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS vms (
                id TEXT PRIMARY KEY,
                group_id INTEGER,
                booked_by TEXT,
                expires_at TEXT,
                external_ip TEXT,
                internal_ip TEXT
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS groups (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS group_vms (
                group_id INTEGER NOT NULL,
                vm_id TEXT NOT NULL,
                PRIMARY KEY (group_id, vm_id)
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                email TEXT PRIMARY KEY,
                created TEXT
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS bookings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_email TEXT,
                vm_id TEXT,
                start TEXT,
                end TEXT,
                action TEXT
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                vm_id TEXT NOT NULL,
                email TEXT NOT NULL,
                position INTEGER NOT NULL
            );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_queue_vm ON queue(vm_id, position);")
        conn.commit()

        cur.execute("SELECT COUNT(*) as cnt FROM vms")
        cnt = cur.fetchone()['cnt']
        if cnt == 0 and os.path.exists(LEGACY_JSON):
            try:
                with open(LEGACY_JSON, 'r', encoding='utf-8') as f:
                    data = json.load(f)
            except Exception:
                data = None

            if data:
                groups = data.get('groups', [])
                for g in groups:
                    cur.execute("INSERT OR IGNORE INTO groups(id, name) VALUES (?, ?)",
                                (g.get('id'), g.get('name')))
                    for vm_id in g.get('vm_ids', []):
                        cur.execute("INSERT OR IGNORE INTO group_vms(group_id, vm_id) VALUES (?, ?)",
                                    (g.get('id'), vm_id))

                for vm in data.get('vms', []):
                    conn.execute("""
                        INSERT OR REPLACE INTO vms(id, group_id, booked_by, expires_at, external_ip, internal_ip)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (
                        vm.get('id'),
                        vm.get('group'),
                        vm.get('booked_by'),
                        vm.get('expires_at'),
                        vm.get('external_ip') or vm.get('ExternalIP'),
                        vm.get('internal_ip') or vm.get('InternalIp'),
                    ))
                    if vm.get('group') is not None:
                        cur.execute("INSERT OR IGNORE INTO group_vms(group_id, vm_id) VALUES (?, ?)",
                                    (vm.get('group'), vm.get('id')))
                    q = vm.get('queue') or []
                    for pos, email in enumerate(q, start=1):
                        cur.execute("INSERT INTO queue(vm_id, email, position) VALUES (?, ?, ?)",
                                    (vm.get('id'), email, pos))

                users = data.get('users', {}) or {}
                for email, u in users.items():
                    cur.execute("INSERT OR REPLACE INTO users(email, created) VALUES (?, ?)",
                                (email, u.get('created')))
                    for b in u.get('bookings', []):
                        cur.execute("""
                            INSERT INTO bookings(user_email, vm_id, start, end, action)
                            VALUES (?, ?, ?, ?, ?)
                        """, (email, b.get('vm_id'), b.get('start'), b.get('end'), b.get('action')))

                conn.commit()
        conn.close()


init_db_and_migrate()


def vm_row_to_dict(row):
    return {
        'id': row['id'],
        'group': row['group_id'],
        'booked_by': row['booked_by'],
        'expires_at': row['expires_at'],
        'queue': get_queue_for_vm(row['id']),
        'external_ip': row['external_ip'],
        'internal_ip': row['internal_ip'],
    }


def get_all_vms():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM vms ORDER BY id")
    rows = cur.fetchall()
    vms = [vm_row_to_dict(r) for r in rows]
    conn.close()
    return vms


def get_vm(vm_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM vms WHERE id = ?", (vm_id,))
    row = cur.fetchone()
    conn.close()
    return vm_row_to_dict(row) if row else None


def get_all_groups():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id, name FROM groups ORDER BY id")
    groups = []
    for r in cur.fetchall():
        cur2 = conn.cursor()
        cur2.execute("SELECT vm_id FROM group_vms WHERE group_id = ?", (r['id'],))
        vm_ids = [x['vm_id'] for x in cur2.fetchall()]
        groups.append({'id': r['id'], 'name': r['name'], 'vm_ids': vm_ids})
    conn.close()
    return groups


def get_queue_for_vm(vm_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT email FROM queue WHERE vm_id = ? ORDER BY position", (vm_id,))
    emails = [r['email'] for r in cur.fetchall()]
    conn.close()
    return emails


def set_queue_for_vm(vm_id, emails):
    """Replace queue for vm_id with given email list (emails = list)."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM queue WHERE vm_id = ?", (vm_id,))
    for pos, email in enumerate(emails, start=1):
        cur.execute("INSERT INTO queue(vm_id, email, position) VALUES (?, ?, ?)", (vm_id, email, pos))
    conn.commit()
    conn.close()


def user_exists(email):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM users WHERE email = ?", (email,))
    r = cur.fetchone()
    conn.close()
    return bool(r)


def ensure_user(email):
    if not email:
        return
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("INSERT OR IGNORE INTO users(email, created) VALUES (?, ?)", (email, datetime.now().isoformat()))
    conn.commit()
    conn.close()


def add_booking_record(user_email, vm_id, start=None, end=None, action=None):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("INSERT INTO bookings(user_email, vm_id, start, end, action) VALUES (?, ?, ?, ?, ?)",
                (user_email, vm_id, start or datetime.now().isoformat(), end, action))
    conn.commit()
    conn.close()


def get_user_bookings(email):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT vm_id, start, end, action FROM bookings WHERE user_email = ? ORDER BY start DESC", (email,))
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


@app.route('/')
def index():
    return send_from_directory('templates', 'index.html')


@app.route('/vms', methods=['GET'])
def list_vms():
    """
    Return vms + groups in the same format as before:
      { 'vms': [...], 'groups': [...] }
    Each vm includes queue[], external_ip/internal_ip fields.
    """
    vms = get_all_vms()
    groups = get_all_groups()
    for vm in vms:
        vm.setdefault('queue', [])
        vm.setdefault('external_ip', None)
        vm.setdefault('internal_ip', None)
    return jsonify({'vms': vms, 'groups': groups})


@app.route('/groups', methods=['GET'])
def list_groups():
    return jsonify(get_all_groups())


@app.route('/groups', methods=['POST'])
def create_group():
    req = request.get_json() or {}
    name = req.get('name')
    vm_ids = req.get('vm_ids', []) or []
    if not name:
        return jsonify({'error': 'Не указано название группы'}), 400

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id FROM groups WHERE LOWER(name) = LOWER(?)", (name,))
    if cur.fetchone():
        conn.close()
        return jsonify({'error': 'Группа с таким именем уже существует'}), 400

    cur.execute("INSERT INTO groups(name) VALUES (?)", (name,))
    group_id = cur.lastrowid

    for vm_id in vm_ids:
        cur.execute("INSERT OR IGNORE INTO group_vms(group_id, vm_id) VALUES (?, ?)", (group_id, vm_id))
        cur.execute("UPDATE vms SET group_id = ? WHERE id = ?", (group_id, vm_id))
    conn.commit()
    conn.close()
    socketio.emit('notification', {'msg': f"Создана группа «{name}»"})
    return jsonify({'id': group_id, 'name': name, 'vm_ids': vm_ids}), 201


@app.route('/groups/<int:group_id>/add-existing-vms', methods=['POST'])
def add_existing_vms_to_group(group_id):
    req = request.get_json() or {}
    vm_ids = req.get('vm_ids', []) or []
    if not isinstance(vm_ids, list) or not vm_ids:
        return jsonify({'error': 'vm_ids должен быть списком непустых идентификаторов'}), 400

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id FROM groups WHERE id = ?", (group_id,))
    if not cur.fetchone():
        conn.close()
        return jsonify({'error': 'Группа не найдена'}), 404

    added = []
    for vm_id in vm_ids:
        cur.execute("SELECT id FROM vms WHERE id = ?", (vm_id,))
        if cur.fetchone():
            cur.execute("UPDATE vms SET group_id = ? WHERE id = ?", (group_id, vm_id))
            cur.execute("INSERT OR IGNORE INTO group_vms(group_id, vm_id) VALUES (?, ?)", (group_id, vm_id))
            added.append(vm_id)
    conn.commit()
    cur.execute("SELECT id, name FROM groups WHERE id = ?", (group_id,))
    group_row = cur.fetchone()
    conn.close()

    if not added:
        return jsonify({'error': 'Ни одна VM не найдена для добавления'}), 404

    socketio.emit('notification', {'msg': f"Добавлено {len(added)} VM в группу {group_row['name']}"})
    return jsonify({'added': added, 'group': {'id': group_row['id'], 'name': group_row['name']}})


@app.route('/add-vm', methods=['POST'])
def add_vm():
    user_email = request.cookies.get('user_email')
    if not user_email:
        return jsonify({'error': 'Требуется авторизация'}), 401

    req = request.get_json() or {}
    vm_id = req.get('id')
    group_id = req.get('group_id')
    external_ip = req.get('external_ip') or None
    internal_ip = req.get('internal_ip') or None

    if not vm_id:
        return jsonify({'error': 'Не указан идентификатор VM'}), 400

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM vms WHERE id = ?", (vm_id,))
    if cur.fetchone():
        conn.close()
        return jsonify({'error': 'VM с таким идентификатором уже существует'}), 400

    group_exists = False
    if group_id is not None:
        cur.execute("SELECT id FROM groups WHERE id = ?", (group_id,))
        if cur.fetchone():
            group_exists = True

    cur.execute("""
        INSERT INTO vms(id, group_id, booked_by, expires_at, external_ip, internal_ip)
        VALUES (?, ?, NULL, NULL, ?, ?)
    """, (vm_id, group_id if group_exists else None, external_ip, internal_ip))

    if group_exists:
        cur.execute("INSERT OR IGNORE INTO group_vms(group_id, vm_id) VALUES (?, ?)", (group_id, vm_id))

    conn.commit()
    conn.close()

    socketio.emit('notification', {'msg': f"Добавлена новая VM: {vm_id}"})
    vm = get_vm(vm_id)
    return jsonify(vm), 201


@app.route('/edit-vm', methods=['POST'])
def edit_vm():
    """
    Edit VM: accepts JSON:
      { vm_id, group_id (optional key), external_ip (optional key), internal_ip (optional key) }
    Requires authorization.
    """
    user_email = request.cookies.get('user_email')
    if not user_email:
        return jsonify({'error': 'Требуется авторизация'}), 401

    req = request.get_json() or {}
    vm_id = req.get('vm_id')
    if not vm_id:
        return jsonify({'error': 'Не указан vm_id'}), 400

    group_key_present = 'group_id' in req
    new_group_id = req.get('group_id') if group_key_present else None

    external_key_present = 'external_ip' in req
    internal_key_present = 'internal_ip' in req

    external_ip = req.get('external_ip') if external_key_present else None
    internal_ip = req.get('internal_ip') if internal_key_present else None

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM vms WHERE id = ?", (vm_id,))
    vm_row = cur.fetchone()
    if not vm_row:
        conn.close()
        return jsonify({'error': 'VM не найдена'}), 404

    old_group = vm_row['group_id']

    if group_key_present:
        if new_group_id is None:
            cur.execute("UPDATE vms SET group_id = NULL WHERE id = ?", (vm_id,))
            cur.execute("DELETE FROM group_vms WHERE vm_id = ?", (vm_id,))
        else:
            cur.execute("SELECT id FROM groups WHERE id = ?", (new_group_id,))
            if not cur.fetchone():
                conn.close()
                return jsonify({'error': 'Группа не найдена'}), 404

            if old_group is not None and old_group != new_group_id:
                cur.execute("DELETE FROM group_vms WHERE group_id = ? AND vm_id = ?", (old_group, vm_id))

            cur.execute("UPDATE vms SET group_id = ? WHERE id = ?", (new_group_id, vm_id))
            cur.execute("INSERT OR IGNORE INTO group_vms(group_id, vm_id) VALUES (?, ?)", (new_group_id, vm_id))

    if external_key_present:
        cur.execute("UPDATE vms SET external_ip = ? WHERE id = ?", (external_ip, vm_id))
    if internal_key_present:
        cur.execute("UPDATE vms SET internal_ip = ? WHERE id = ?", (internal_ip, vm_id))

    conn.commit()
    conn.close()

    socketio.emit('notification', {'msg': f"VM {vm_id} изменена — обновлены параметры", 'target': None})
    vm = get_vm(vm_id)
    return jsonify(vm), 200


@app.route('/delete-vm', methods=['POST'])
def delete_vm():
    user_email = request.cookies.get('user_email')
    if not user_email:
        return jsonify({'error': 'Требуется авторизация'}), 401

    req = request.get_json() or {}
    vm_id = req.get('vm_id')
    if not vm_id:
        return jsonify({'error': 'Не указан vm_id'}), 400

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM vms WHERE id = ?", (vm_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return jsonify({'error': 'VM не найдена'}), 404

    if row['booked_by']:
        owner = row['booked_by']
        add_booking_record(owner, vm_id, start=datetime.now().isoformat(), end=None, action='deleted')

    remove_scheduled_jobs_for_vm(vm_id)

    cur.execute("DELETE FROM vms WHERE id = ?", (vm_id,))
    cur.execute("DELETE FROM queue WHERE vm_id = ?", (vm_id,))
    cur.execute("DELETE FROM group_vms WHERE vm_id = ?", (vm_id,))
    conn.commit()
    conn.close()

    socketio.emit('notification', {'msg': f"VM {vm_id} удалена"})
    return jsonify({'status': 'ok'})


@app.route('/remove-vm-from-group', methods=['POST'])
def remove_vm_from_group():
    req = request.get_json() or {}
    vm_id = req.get('vm_id')
    group_id = req.get('group_id')
    if not vm_id:
        return jsonify({'error': 'Не указан vm_id'}), 400

    conn = get_conn()
    cur = conn.cursor()
    updated = False

    if group_id is not None:
        cur.execute("SELECT vm_id FROM group_vms WHERE group_id = ? AND vm_id = ?", (group_id, vm_id))
        if cur.fetchone():
            cur.execute("DELETE FROM group_vms WHERE group_id = ? AND vm_id = ?", (group_id, vm_id))
            cur.execute("UPDATE vms SET group_id = NULL WHERE id = ?", (vm_id,))
            updated = True
    else:
        cur.execute("SELECT group_id FROM group_vms WHERE vm_id = ?", (vm_id,))
        row = cur.fetchone()
        if row:
            cur.execute("DELETE FROM group_vms WHERE vm_id = ?", (vm_id,))
            cur.execute("UPDATE vms SET group_id = NULL WHERE id = ?", (vm_id,))
            updated = True

    conn.commit()
    conn.close()

    if not updated:
        return jsonify({'error': 'VM или группа не найдены / VM не состоит в группе'}), 404

    socketio.emit('notification', {'msg': f"VM {vm_id} исключена из группы"})
    return jsonify({'status': 'ok'})


@app.route('/auth', methods=['POST'])
def auth():
    email = (request.json.get('email', '') or '').strip().lower()
    if not email or '@' not in email:
        return jsonify({'error': 'Некорректный email'}), 400

    ensure_user(email)
    add_booking_record(email, None, start=datetime.now().isoformat(), end=None, action='login')
    resp = jsonify({'status': 'ok'})
    resp.set_cookie('user_email', email, max_age=60*60*24*365)
    return resp


@app.route('/logout', methods=['POST'])
def logout():
    resp = jsonify({'status': 'logged out'})
    resp.set_cookie('user_email', '', expires=0)
    return resp


@app.route('/book', methods=['POST'])
def book_vm():
    user_email = request.cookies.get('user_email')
    if not user_email:
        return jsonify({'error': 'Требуется авторизация'}), 401

    req = request.get_json() or {}
    vm_id = req.get('vm_id')
    hours = min(max(int(req.get('hours', 24)), 1), 24)

    if not vm_id:
        return jsonify({'error': 'Не указан vm_id'}), 400

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM vms WHERE id = ?", (vm_id,))
    vm_row = cur.fetchone()
    if not vm_row:
        conn.close()
        return jsonify({'error': 'VM не найдена'}), 404

    if vm_row['booked_by']:
        conn.close()
        return jsonify({'error': 'VM уже забронирована'}), 400

    expires = datetime.now() + timedelta(hours=hours)
    cur.execute("UPDATE vms SET booked_by = ?, expires_at = ? WHERE id = ?", (user_email, expires.isoformat(), vm_id))

    add_booking_record(user_email, vm_id, start=datetime.now().isoformat(), end=expires.isoformat(), action='book')

    conn.commit()
    conn.close()
    socketio.emit('notification', {
        'msg': f"{user_email} забронировал VM {vm_id} до {expires.strftime('%Y-%m-%d %H:%M:%S')}"
    })
    schedule_jobs()
    return jsonify(get_vm(vm_id))


@app.route('/renew', methods=['POST'])
def renew_vm():
    user_email = request.cookies.get('user_email')
    if not user_email:
        return jsonify({'error': 'Требуется авторизация'}), 401

    req = request.get_json() or {}
    vm_id = req.get('vm_id')
    hours = min(max(int(req.get('hours', 24)), 1), 24)

    if not vm_id:
        return jsonify({'error': 'Не указан vm_id'}), 400

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM vms WHERE id = ?", (vm_id,))
    vm_row = cur.fetchone()
    if not vm_row:
        conn.close()
        return jsonify({'error': 'VM не найдена'}), 404

    if vm_row['booked_by'] != user_email:
        conn.close()
        return jsonify({'error': 'Вы не бронировали эту VM'}), 403

    try:
        expires_old = datetime.fromisoformat(vm_row['expires_at']) if vm_row['expires_at'] else datetime.now()
    except Exception:
        expires_old = datetime.now()

    new_expires = expires_old + timedelta(hours=hours)
    cur.execute("UPDATE vms SET expires_at = ? WHERE id = ?", (new_expires.isoformat(), vm_id))

    add_booking_record(user_email, vm_id, start=datetime.now().isoformat(), end=new_expires.isoformat(), action='renew')
    conn.commit()
    conn.close()

    socketio.emit('notification', {
        'msg': f"{user_email} продлил бронь VM {vm_id} до {new_expires.strftime('%Y-%m-%d %H:%M:%S')}"
    })
    schedule_jobs()
    return jsonify(get_vm(vm_id))


@app.route('/me', methods=['GET'])
def get_me():
    user_email = request.cookies.get('user_email')
    if not user_email:
        return jsonify({'authenticated': False}), 200
    if not user_exists(user_email):
        return jsonify({'authenticated': False}), 200

    bookings = get_user_bookings(user_email)
    return jsonify({'authenticated': True, 'email': user_email, 'bookings': bookings}), 200


@app.route('/queue/join', methods=['POST'])
def join_queue():
    user_email = request.cookies.get('user_email')
    if not user_email:
        return jsonify({'error': 'Требуется авторизация'}), 401

    req = request.get_json() or {}
    vm_id = req.get('vm_id')
    if not vm_id:
        return jsonify({'error': 'Не указан vm_id'}), 400

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM vms WHERE id = ?", (vm_id,))
    if not cur.fetchone():
        conn.close()
        return jsonify({'error': 'VM не найдена'}), 404

    cur.execute("SELECT email FROM queue WHERE vm_id = ? ORDER BY position", (vm_id,))
    existing = [r['email'] for r in cur.fetchall()]

    if user_email in existing:
        position = existing.index(user_email) + 1
        conn.close()
        return jsonify({'status': 'already_in_queue', 'position': position}), 200

    if len(existing) >= 10:
        conn.close()
        return jsonify({'error': 'Очередь заполнена (максимум 10)'}), 400

    cur.execute("SELECT booked_by FROM vms WHERE id = ?", (vm_id,))
    owner = cur.fetchone()['booked_by']
    if owner == user_email:
        conn.close()
        return jsonify({'error': 'Вы уже владеете этой VM'}), 400

    next_pos = (len(existing) + 1)
    cur.execute("INSERT INTO queue(vm_id, email, position) VALUES (?, ?, ?)", (vm_id, user_email, next_pos))
    conn.commit()
    conn.close()

    if owner:
        socketio.emit('notification', {'msg': f"{user_email} встал(а) в очередь на VM {vm_id}", 'target': owner})
        send_email_notification(
            to_email=owner,
            subject=f"[LoadZone] На вашу VM {vm_id} появилась очередь",
            body=f"Пользователь {user_email} встал(а) в очередь на VM {vm_id}."
        )
    position = next_pos
    return jsonify({'status': 'ok', 'position': position}), 200


@app.route('/queue/leave', methods=['POST'])
def leave_queue():
    user_email = request.cookies.get('user_email')
    if not user_email:
        return jsonify({'error': 'Требуется авторизация'}), 401

    req = request.get_json() or {}
    vm_id = req.get('vm_id')
    if not vm_id:
        return jsonify({'error': 'Не указан vm_id'}), 400

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id, position FROM queue WHERE vm_id = ? AND email = ? ORDER BY position", (vm_id, user_email))
    row = cur.fetchone()
    if not row:
        conn.close()
        return jsonify({'error': 'Вы не в очереди'}), 400

    cur.execute("DELETE FROM queue WHERE id = ?", (row['id'],))
    cur.execute("SELECT id, email FROM queue WHERE vm_id = ? ORDER BY position", (vm_id,))
    remaining = cur.fetchall()
    for pos, r in enumerate(remaining, start=1):
        cur.execute("UPDATE queue SET position = ? WHERE id = ?", (pos, r['id']))
    conn.commit()
    conn.close()
    return jsonify({'status': 'ok'}), 200


@app.route('/cancel', methods=['POST'])
def cancel_booking():
    user_email = request.cookies.get('user_email')
    if not user_email:
        return jsonify({'error': 'Требуется авторизация'}), 401
    req = request.get_json() or {}
    vm_id = req.get('vm_id')
    if not vm_id:
        return jsonify({'error': 'Не указан vm_id'}), 400

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT booked_by FROM vms WHERE id = ?", (vm_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return jsonify({'error': 'VM не найдена'}), 404

    if row['booked_by'] != user_email:
        conn.close()
        return jsonify({'error': 'Вы не бронировали эту VM'}), 403

    cur.execute("UPDATE vms SET booked_by = NULL, expires_at = NULL WHERE id = ?", (vm_id,))
    add_booking_record(user_email, vm_id, start=datetime.now().isoformat(), end=None, action='cancel')

    conn.commit()
    conn.close()
    remove_scheduled_jobs_for_vm(vm_id)
    socketio.emit('notification', {'msg': f"{user_email} отменил бронь VM {vm_id}"})
    schedule_jobs()
    return jsonify({'status': 'ok'})


@app.route('/delete-group', methods=['POST'])
def delete_group():
    req = request.get_json() or {}
    group_id = req.get('group_id')
    if group_id is None:
        return jsonify({'error': 'Не указан group_id'}), 400

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id FROM groups WHERE id = ?", (group_id,))
    if not cur.fetchone():
        conn.close()
        return jsonify({'error': 'Группа не найдена'}), 404

    cur.execute("SELECT vm_id FROM group_vms WHERE group_id = ?", (group_id,))
    vm_ids = [r['vm_id'] for r in cur.fetchall()]
    for vm_id in vm_ids:
        cur.execute("UPDATE vms SET group_id = NULL WHERE id = ?", (vm_id,))
    cur.execute("DELETE FROM group_vms WHERE group_id = ?", (group_id,))
    cur.execute("DELETE FROM groups WHERE id = ?", (group_id,))
    conn.commit()
    conn.close()
    socketio.emit('notification', {'msg': f"Группа {group_id} удалена"})
    return jsonify({'status': 'ok'})


def remove_notify_release_jobs():
    try:
        for job in scheduler.get_jobs():
            jid = job.id or ""
            if jid.startswith("notify_") or jid.startswith("release_"):
                try:
                    scheduler.remove_job(jid)
                except Exception:
                    app.logger.debug("Failed to remove job %s", jid, exc_info=True)
    except Exception:
        app.logger.debug("Could not iterate scheduler jobs", exc_info=True)


def release_vm(vm_id):
    """
    Release VM (DB): clear booked_by/expires_at, notify queue first, save bookings history.
    """
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM vms WHERE id = ?", (vm_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return
    user_email = row['booked_by']
    if user_email and user_exists(user_email):
        add_booking_record(user_email, vm_id, start=datetime.now().isoformat(), end=None, action='release')

    cur.execute("UPDATE vms SET booked_by = NULL, expires_at = NULL WHERE id = ?", (vm_id,))
    cur.execute("SELECT email FROM queue WHERE vm_id = ? ORDER BY position LIMIT 1", (vm_id,))
    first = cur.fetchone()
    conn.commit()
    conn.close()
    remove_scheduled_jobs_for_vm(vm_id)
    if first:
        first_email = first['email']
        socketio.emit('notification', {'msg': f"VM {vm_id} освобождена — сейчас вы можете её забронировать", 'target': first_email})
        send_email_notification(
            to_email=first,
            subject=f"[LoadZone] VM {vm_id} освобождена",
            body=f"VM {vm_id} освобождена — вы первый в очереди и можете её забронировать."
        )
    socketio.emit('notification', {'msg': f"VM {vm_id} освобождена"})


def schedule_jobs():
    """
    Schedule notify and release jobs based on vms/expires_at/queue.
    """
    with db_lock:
        try:
            remove_notify_release_jobs()
        except Exception:
            app.logger.exception("Failed to remove old notify/release jobs")

        vms = get_all_vms()
        now = datetime.now()
        for vm in vms:
            if vm.get('booked_by') and vm.get('expires_at'):
                try:
                    expires = datetime.fromisoformat(vm['expires_at'])
                except Exception:
                    continue
                notify_time = expires - timedelta(hours=1)
                if notify_time > now:
                    queue = vm.get('queue') or []
                    if queue:
                        first_email = queue[0]

                        def make_notify_first(vm_id=vm['id'], first=first_email):
                            def _fn():
                                try:
                                    socketio.emit('notification', {
                                        'msg': f"VM {vm_id}: через час бронь истечёт — вы следующий в очереди.",
                                        'target': first
                                    })
                                    send_email_notification(
                                        to_email=first,
                                        subject=f"[LoadZone] Вы следующий в очереди VM {vm_id}",
                                        body=f"VM {vm_id}: текущая бронь истекает через час — вы следующий в очереди."
                                    )
                                except Exception:
                                    app.logger.exception("notify-first job failed for VM %s", vm_id)
                            return _fn

                        try:
                            scheduler.add_job(func=make_notify_first(), trigger='date', run_date=notify_time, id=f"notify_{vm['id']}", replace_existing=True)
                        except Exception:
                            app.logger.exception("Failed to schedule notify job for vm %s", vm.get('id'))
                    else:
                        try:
                            scheduler.add_job(func=lambda vm_id=vm['id']: socketio.emit('notification', {'msg': f"Бронь VM {vm_id} истекает через час!"}),
                                              trigger='date', run_date=notify_time, id=f"notify_{vm['id']}", replace_existing=True)
                        except Exception:
                            pass

                if expires > now:
                    try:
                        scheduler.add_job(func=lambda vm_id=vm['id']: release_vm(vm_id),
                                          trigger='date', run_date=expires, id=f"release_{vm['id']}", replace_existing=True)
                    except Exception:
                        pass


def remove_scheduled_jobs_for_vm(vm_id):
    for jid in [f"notify_{vm_id}", f"release_{vm_id}"]:
        try:
            scheduler.remove_job(jid)
        except Exception:
            pass


def _parse_iso(s):
    if not s:
        return None
    try:
        return datetime.fromisoformat(s)
    except Exception:
        try:
            from datetime import datetime as dt
            return dt.strptime(s[:19], "%Y-%m-%dT%H:%M:%S")
        except Exception:
            return None


def purge_old_history():
    """
    Similar rules as original: remove old booking/cancel/release entries older than 1 hour, and prune pairs.
    We'll load bookings grouped by user, compute which to keep, then rewrite bookings table for each user.
    """
    now = datetime.now()
    cutoff = now - timedelta(hours=1)
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id, user_email, vm_id, start, end, action FROM bookings ORDER BY user_email, vm_id, start")
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()

    from collections import defaultdict
    users = defaultdict(lambda: defaultdict(list))
    for r in rows:
        users[r['user_email']][r['vm_id']].append(r)

    keep_records = []
    for email, per_vm in users.items():
        for vm_id, events in per_vm.items():
            parsed = []
            for ev in events:
                ev2 = dict(ev)
                ev2['_start_dt'] = _parse_iso(ev.get('start'))
                ev2['_end_dt'] = _parse_iso(ev.get('end')) if ev.get('end') else None
                parsed.append(ev2)
            ev_list_sorted = sorted(parsed, key=lambda x: (x.get('_start_dt') or datetime.fromtimestamp(0)))
            keep_ids = set()
            for ev in ev_list_sorted:
                action = ev.get('action')
                s = ev.get('_start_dt')
                e = ev.get('_end_dt')
                if action in ('cancel', 'release'):
                    if s and s < cutoff:
                        continue
                    else:
                        keep_ids.add(ev['id'])
                elif action == 'book':
                    if e and e < cutoff:
                        continue
                    else:
                        keep_ids.add(ev['id'])
                else:
                    if s and s < cutoff:
                        continue
                    keep_ids.add(ev['id'])
            for ev in ev_list_sorted:
                if ev.get('action') in ('cancel', 'release'):
                    cancel_time = ev.get('_start_dt')
                    if cancel_time and cancel_time < cutoff:
                        prev_books = [b for b in ev_list_sorted if b.get('action') == 'book' and (b.get('_start_dt') or datetime.fromtimestamp(0)) <= cancel_time]
                        if prev_books:
                            for pb in prev_books:
                                if pb['id'] in keep_ids:
                                    keep_ids.discard(pb['id'])
                            if ev['id'] in keep_ids:
                                keep_ids.discard(ev['id'])
            for ev in ev_list_sorted:
                if ev['id'] in keep_ids:
                    keep_records.append({
                        'id': ev['id'],
                        'user_email': ev['user_email'],
                        'vm_id': ev['vm_id'],
                        'start': ev['start'],
                        'end': ev['end'],
                        'action': ev['action']
                    })

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM bookings")
    for r in keep_records:
        cur.execute("INSERT INTO bookings(id, user_email, vm_id, start, end, action) VALUES (?, ?, ?, ?, ?, ?)",
                    (r['id'], r['user_email'], r['vm_id'], r['start'], r['end'], r['action']))
    conn.commit()
    conn.close()
    if keep_records:
        app.logger.info("purge_old_history: cleaned outdated booking records")


try:
    scheduler.add_job(func=purge_old_history, trigger='interval', hours=1, id='purge_old_history_job', replace_existing=True)
except Exception:
    app.logger.exception("Failed to schedule purge_old_history_job", exc_info=True)

try:
    purge_old_history()
except Exception:
    app.logger.exception("Initial purge_old_history failed")


@socketio.on('connect')
def on_connect():
    schedule_jobs()


if __name__ == '__main__':
    socketio.run(app, host='0.0.0.0', port=5000)
