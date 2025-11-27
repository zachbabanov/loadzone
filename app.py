# app.py — обновлённый
try:
    import eventlet
    eventlet.monkey_patch()
    _HAS_EVENTLET = True
except Exception:
    _HAS_EVENTLET = False

import os
import threading
import time
import smtplib
import ssl
from email.message import EmailMessage
from datetime import datetime, timedelta
from functools import wraps
import logging

from flask import Flask, jsonify, request, send_from_directory
from flask_socketio import SocketIO
from apscheduler.schedulers.background import BackgroundScheduler

from sqlalchemy import (
    create_engine, MetaData, Table, Column, Integer, String, Text,
    select, insert, update, delete, func, and_
)
from sqlalchemy.exc import OperationalError, DatabaseError, InterfaceError, PendingRollbackError
from sqlalchemy.orm import scoped_session, sessionmaker

# --- Config from env ---
MYSQL_USER = os.environ.get('MYSQL_USER')
MYSQL_PASS = os.environ.get('MYSQL_PASS')
MYSQL_HOST = os.environ.get('MYSQL_HOST', '127.0.0.1')
MYSQL_PORT = os.environ.get('MYSQL_PORT', '3306')
MYSQL_DB = os.environ.get('MYSQL_DB')

if not (MYSQL_USER and MYSQL_PASS and MYSQL_DB):
    raise RuntimeError("Please set MYSQL_USER, MYSQL_PASS and MYSQL_DB environment variables")

DB_URL = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASS}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}?charset=utf8mb4"

POOL_SIZE = int(os.environ.get('DB_POOL_SIZE', 5))
MAX_OVERFLOW = int(os.environ.get('DB_MAX_OVERFLOW', 10))
POOL_TIMEOUT = int(os.environ.get('DB_POOL_TIMEOUT', 30))

SOCKET_MESSAGE_QUEUE = os.environ.get('SOCKET_MESSAGE_QUEUE')

# --- Flask / SocketIO init ---
app = Flask(__name__, static_folder='static', template_folder='templates')

handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('[%(asctime)s] %(levelname)s in %(module)s: %(message)s'))
if not app.logger.handlers:
    app.logger.addHandler(handler)
app.logger.setLevel(logging.INFO)

# Enable engineio_logger for better diagnostics (can be set False after debugging)
_engineio_logger = os.environ.get('ENGINEIO_LOGGER', '1') != '0'

if _HAS_EVENTLET:
    if SOCKET_MESSAGE_QUEUE:
        app.logger.info("SocketIO: using eventlet with message_queue=%s", SOCKET_MESSAGE_QUEUE)
        socketio = SocketIO(app, cors_allowed_origins='*', async_mode='eventlet',
                            logger=True, engineio_logger=_engineio_logger, manage_session=False,
                            message_queue=SOCKET_MESSAGE_QUEUE)
    else:
        app.logger.info("SocketIO: using eventlet without message_queue (single-process recommended)")
        socketio = SocketIO(app, cors_allowed_origins='*', async_mode='eventlet',
                            logger=True, engineio_logger=_engineio_logger, manage_session=False)
else:
    if SOCKET_MESSAGE_QUEUE:
        app.logger.info("SocketIO: using threading with message_queue=%s", SOCKET_MESSAGE_QUEUE)
        socketio = SocketIO(app, cors_allowed_origins='*', async_mode='threading',
                            logger=True, engineio_logger=_engineio_logger, manage_session=False,
                            message_queue=SOCKET_MESSAGE_QUEUE)
    else:
        app.logger.warning("eventlet not available; fallback to threading (single-process recommended).")
        socketio = SocketIO(app, cors_allowed_origins='*', async_mode='threading',
                            logger=True, engineio_logger=_engineio_logger, manage_session=False)

# --- Scheduler ---
scheduler = BackgroundScheduler()
scheduler.start()

# --- DB setup ---
engine = create_engine(
    DB_URL,
    pool_size=POOL_SIZE,
    max_overflow=MAX_OVERFLOW,
    pool_timeout=POOL_TIMEOUT,
    pool_pre_ping=True,
    future=True
)
Session = scoped_session(sessionmaker(bind=engine, expire_on_commit=False))

metadata = MetaData()

vms = Table(
    'vms', metadata,
    Column('id', String(255), primary_key=True),
    Column('group_id', Integer, nullable=True),
    Column('booked_by', String(255), nullable=True),
    Column('expires_at', Text, nullable=True),
    Column('external_ip', String(255), nullable=True),
    Column('internal_ip', String(255), nullable=True),
    mysql_engine='InnoDB',
    mysql_charset='utf8mb4'
)

groups = Table(
    'groups', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('name', String(255), unique=True, nullable=False),
    mysql_engine='InnoDB',
    mysql_charset='utf8mb4'
)

group_vms = Table(
    'group_vms', metadata,
    Column('group_id', Integer, nullable=False),
    Column('vm_id', String(255), nullable=False),
    mysql_engine='InnoDB',
    mysql_charset='utf8mb4'
)

users = Table(
    'users', metadata,
    Column('email', String(255), primary_key=True),
    Column('created', Text, nullable=True),
    mysql_engine='InnoDB',
    mysql_charset='utf8mb4'
)

bookings = Table(
    'bookings', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('user_email', String(255), nullable=True),
    Column('vm_id', String(255), nullable=True),
    Column('start', Text, nullable=True),
    Column('end', Text, nullable=True),
    Column('action', String(50), nullable=True),
    mysql_engine='InnoDB',
    mysql_charset='utf8mb4'
)

queue = Table(
    'queue', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('vm_id', String(255), nullable=False),
    Column('email', String(255), nullable=False),
    Column('position', Integer, nullable=False),
    mysql_engine='InnoDB',
    mysql_charset='utf8mb4'
)

with engine.begin() as conn:
    metadata.create_all(conn)

db_lock = threading.Lock()

# --- Helpers ---
def db_retry(max_retries=5, initial_delay=0.05, backoff=2.0):
    """
    Retry transient DB errors (including PendingRollbackError).
    """
    def decorator(fn):
        @wraps(fn)
        def wrapped(*args, **kwargs):
            delay = initial_delay
            last_exc = None
            for attempt in range(max_retries):
                try:
                    return fn(*args, **kwargs)
                except (OperationalError, InterfaceError, DatabaseError, PendingRollbackError) as exc:
                    last_exc = exc
                    app.logger.debug("DB transient error on attempt %d for %s: %s", attempt + 1, fn.__name__, repr(exc))
                    time.sleep(delay)
                    delay *= backoff
            app.logger.exception("DB operation failed after %d retries: %s", max_retries, fn.__name__, exc_info=last_exc)
            raise last_exc
        return wrapped
    return decorator

def _safe_exec(session, thunk):
    """
    Execute thunk() which normally calls session.execute(...).
    On PendingRollbackError, perform session.rollback() and retry once.
    thunk must be a zero-arg callable.
    """
    try:
        return thunk()
    except PendingRollbackError:
        try:
            session.rollback()
        except Exception:
            app.logger.debug("session.rollback() failed in _safe_exec", exc_info=True)
        return thunk()

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

# --- Email sending (background, non-blocking) ---
def send_email_notification(to_email: str, subject: str, body: str):
    """
    Send email in a background worker:
      - if eventlet is present, use eventlet.spawn_n (non-blocking greenlet)
      - otherwise use threading.Thread
    """
    host = os.environ.get('SMTP_HOST')
    port = os.environ.get('SMTP_PORT')
    user = os.environ.get('SMTP_USER')
    password = os.environ.get('SMTP_PASS')
    sender = os.environ.get('SMTP_FROM') or user

    masked_user = (user.split('@')[0] + "@...") if user and "@" in user else user

    if not (host and port and user and password and sender and to_email):
        app.logger.info("SMTP credentials incomplete or missing recipient; skipping email to %r (host=%r, port=%r, user=%r, sender=%r).",
                         to_email, host, port, masked_user, sender)
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

        app.logger.info("Attempting to send email to %s via %s:%s (user=%s)", to_email, host, port_int, masked_user)
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
            app.logger.info("Email successfully sent to %s (subject=%s)", to_email, subject)
        except Exception as exc:
            app.logger.exception("Failed to send email to %s: %s", to_email, str(exc))

    try:
        if _HAS_EVENTLET:
            eventlet.spawn_n(_send)
        else:
            t = threading.Thread(target=_send, daemon=True)
            t.start()
    except Exception:
        try:
            t = threading.Thread(target=_send, daemon=True)
            t.start()
        except Exception:
            app.logger.exception("Failed to start background sender for email to %s", to_email)

# --- Row/DB helpers ---
@db_retry()
def vm_row_to_dict(row_mapping, session=None):
    """Convert RowMapping (mappings() result) to dict used by frontend."""
    if not row_mapping:
        return None
    vm_id = row_mapping['id']
    if session is None:
        s = Session()
        try:
            q_emails = _safe_exec(s, lambda: s.execute(select(queue.c.email).where(queue.c.vm_id == vm_id).order_by(queue.c.position)).scalars().all())
        finally:
            s.close()
    else:
        q_emails = _safe_exec(session, lambda: session.execute(select(queue.c.email).where(queue.c.vm_id == vm_id).order_by(queue.c.position)).scalars().all())
    return {
        'id': row_mapping['id'],
        'group': row_mapping['group_id'],
        'booked_by': row_mapping['booked_by'],
        'expires_at': row_mapping['expires_at'],
        'queue': q_emails,
        'external_ip': row_mapping.get('external_ip') if hasattr(row_mapping, 'get') else row_mapping['external_ip'],
        'internal_ip': row_mapping.get('internal_ip') if hasattr(row_mapping, 'get') else row_mapping['internal_ip'],
    }

@db_retry()
def get_all_vms():
    session = Session()
    try:
        rows = _safe_exec(session, lambda: session.execute(select(vms).order_by(vms.c.id)).mappings().all())
        result = []
        for row in rows:
            result.append(vm_row_to_dict(row, session=session))
        return result
    finally:
        session.close()

@db_retry()
def get_vm(vm_id):
    session = Session()
    try:
        row = _safe_exec(session, lambda: session.execute(select(vms).where(vms.c.id == vm_id)).mappings().fetchone())
        return vm_row_to_dict(row, session=session) if row else None
    finally:
        session.close()

@db_retry()
def get_all_groups():
    session = Session()
    try:
        g_rows = _safe_exec(session, lambda: session.execute(select(groups).order_by(groups.c.id)).mappings().all())
        out = []
        for gr in g_rows:
            vm_ids = _safe_exec(session, lambda: session.execute(select(group_vms.c.vm_id).where(group_vms.c.group_id == gr['id'])).scalars().all())
            out.append({'id': gr['id'], 'name': gr['name'], 'vm_ids': vm_ids})
        return out
    finally:
        session.close()

@db_retry()
def get_queue_for_vm(vm_id):
    session = Session()
    try:
        emails = _safe_exec(session, lambda: session.execute(select(queue.c.email).where(queue.c.vm_id == vm_id).order_by(queue.c.position)).scalars().all())
        return emails
    finally:
        session.close()

@db_retry()
def set_queue_for_vm(vm_id, emails):
    session = Session()
    try:
        with session.begin():
            session.execute(delete(queue).where(queue.c.vm_id == vm_id))
            for pos, email in enumerate(emails, start=1):
                session.execute(insert(queue).values(vm_id=vm_id, email=email, position=pos))
    finally:
        session.close()

@db_retry()
def user_exists(email):
    session = Session()
    try:
        r = _safe_exec(session, lambda: session.execute(select(users.c.email).where(users.c.email == email)).scalar())
        return bool(r)
    finally:
        session.close()

@db_retry()
def ensure_user(email):
    if not email:
        return
    session = Session()
    try:
        with session.begin():
            session.execute(insert(users).prefix_with('IGNORE').values(email=email, created=datetime.now().isoformat()))
    finally:
        session.close()

@db_retry()
def add_booking_record(user_email, vm_id, start=None, end=None, action=None):
    session = Session()
    try:
        with session.begin():
            session.execute(insert(bookings).values(
                user_email=user_email, vm_id=vm_id, start=start or datetime.now().isoformat(), end=end, action=action
            ))
    finally:
        session.close()

@db_retry()
def get_user_bookings(email):
    session = Session()
    try:
        rows = _safe_exec(session, lambda: session.execute(
            select(bookings.c.vm_id, bookings.c.start, bookings.c.end, bookings.c.action)
            .where(bookings.c.user_email == email)
            .order_by(bookings.c.start.desc())
        ).all())
        return [dict(vm_id=r[0], start=r[1], end=r[2], action=r[3]) for r in rows]
    finally:
        session.close()

# --- HTTP routes ---
@app.route('/')
def index():
    return send_from_directory('templates', 'index.html')

@app.route('/vms', methods=['GET'])
def list_vms():
    vms_list = get_all_vms()
    groups_list = get_all_groups()
    for vm in vms_list:
        vm.setdefault('queue', [])
        vm.setdefault('external_ip', None)
        vm.setdefault('internal_ip', None)
    return jsonify({'vms': vms_list, 'groups': groups_list})

@app.route('/groups', methods=['GET'])
def list_groups_route():
    return jsonify(get_all_groups())

@app.route('/groups', methods=['POST'])
def create_group():
    req = request.get_json() or {}
    name = req.get('name')
    vm_ids = req.get('vm_ids', []) or []
    if not name:
        return jsonify({'error': 'Не указано название группы'}), 400

    session = Session()
    try:
        existing = _safe_exec(session, lambda: session.execute(select(groups.c.id).where(func.lower(groups.c.name) == name.lower())).scalar())
        if existing:
            return jsonify({'error': 'Группа с таким именем уже существует'}), 400
        with session.begin():
            session.execute(insert(groups).values(name=name))
            group_id = _safe_exec(session, lambda: session.execute(select(groups.c.id).where(groups.c.name == name)).scalar())
            for vm_id in vm_ids:
                session.execute(update(vms).where(vms.c.id == vm_id).values(group_id=group_id))
                session.execute(insert(group_vms).prefix_with('IGNORE').values(group_id=group_id, vm_id=vm_id))
    finally:
        session.close()

    socketio.emit('notification', {'msg': f"Создана группа «{name}»"})
    return jsonify({'id': group_id, 'name': name, 'vm_ids': vm_ids}), 201

@app.route('/groups/<int:group_id>/add-existing-vms', methods=['POST'])
def add_existing_vms_to_group(group_id):
    req = request.get_json() or {}
    vm_ids = req.get('vm_ids', []) or []
    if not isinstance(vm_ids, list) or not vm_ids:
        return jsonify({'error': 'vm_ids должен быть списком непустых идентификаторов'}), 400

    session = Session()
    try:
        if not _safe_exec(session, lambda: session.execute(select(groups.c.id).where(groups.c.id == group_id)).scalar()):
            return jsonify({'error': 'Группа не найдена'}), 404
        added = []
        with session.begin():
            for vm_id in vm_ids:
                if _safe_exec(session, lambda: session.execute(select(vms.c.id).where(vms.c.id == vm_id)).scalar()):
                    session.execute(update(vms).where(vms.c.id == vm_id).values(group_id=group_id))
                    session.execute(insert(group_vms).prefix_with('IGNORE').values(group_id=group_id, vm_id=vm_id))
                    added.append(vm_id)
        gr = _safe_exec(session, lambda: session.execute(select(groups.c.id, groups.c.name).where(groups.c.id == group_id)).mappings().fetchone())
    finally:
        session.close()

    if not added:
        return jsonify({'error': 'Ни одна VM не найдена для добавления'}), 404

    socketio.emit('notification', {'msg': f"Добавлено {len(added)} VM в группу {gr['name']}"})
    return jsonify({'added': added, 'group': {'id': gr['id'], 'name': gr['name']}})

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

    session = Session()
    try:
        if _safe_exec(session, lambda: session.execute(select(vms.c.id).where(vms.c.id == vm_id)).scalar()):
            return jsonify({'error': 'VM с таким идентификатором уже существует'}), 400

        group_exists = None
        if group_id is not None:
            if _safe_exec(session, lambda: session.execute(select(groups.c.id).where(groups.c.id == group_id)).scalar()):
                group_exists = group_id

        with session.begin():
            session.execute(insert(vms).values(
                id=vm_id,
                group_id=group_exists,
                booked_by=None,
                expires_at=None,
                external_ip=external_ip,
                internal_ip=internal_ip
            ))
            if group_exists:
                session.execute(insert(group_vms).prefix_with('IGNORE').values(group_id=group_id, vm_id=vm_id))
    finally:
        session.close()

    socketio.emit('notification', {'msg': f"Добавлена новая VM: {vm_id}"})
    vm = get_vm(vm_id)
    return jsonify(vm), 201

@app.route('/edit-vm', methods=['POST'])
def edit_vm():
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

    session = Session()
    try:
        row = _safe_exec(session, lambda: session.execute(select(vms).where(vms.c.id == vm_id)).mappings().fetchone())
        if not row:
            return jsonify({'error': 'VM не найдена'}), 404

        old_group = row['group_id']

        with session.begin():
            if group_key_present:
                if new_group_id is None:
                    session.execute(update(vms).where(vms.c.id == vm_id).values(group_id=None))
                    session.execute(delete(group_vms).where(group_vms.c.vm_id == vm_id))
                else:
                    if not _safe_exec(session, lambda: session.execute(select(groups.c.id).where(groups.c.id == new_group_id)).scalar()):
                        return jsonify({'error': 'Группа не найдена'}), 404
                    if old_group is not None and old_group != new_group_id:
                        session.execute(delete(group_vms).where((group_vms.c.group_id == old_group) & (group_vms.c.vm_id == vm_id)))
                    session.execute(update(vms).where(vms.c.id == vm_id).values(group_id=new_group_id))
                    session.execute(insert(group_vms).prefix_with('IGNORE').values(group_id=new_group_id, vm_id=vm_id))

            if external_key_present:
                session.execute(update(vms).where(vms.c.id == vm_id).values(external_ip=external_ip))
            if internal_key_present:
                session.execute(update(vms).where(vms.c.id == vm_id).values(internal_ip=internal_ip))
    finally:
        session.close()

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

    session = Session()
    try:
        row = _safe_exec(session, lambda: session.execute(select(vms).where(vms.c.id == vm_id)).mappings().fetchone())
        if not row:
            return jsonify({'error': 'VM не найдена'}), 404
        if row['booked_by']:
            owner = row['booked_by']
            add_booking_record(owner, vm_id, start=datetime.now().isoformat(), end=None, action='deleted')

        remove_scheduled_jobs_for_vm(vm_id)

        with session.begin():
            session.execute(delete(vms).where(vms.c.id == vm_id))
            session.execute(delete(queue).where(queue.c.vm_id == vm_id))
            session.execute(delete(group_vms).where(group_vms.c.vm_id == vm_id))
    finally:
        session.close()

    socketio.emit('notification', {'msg': f"VM {vm_id} удалена"})
    return jsonify({'status': 'ok'})

@app.route('/remove-vm-from-group', methods=['POST'])
def remove_vm_from_group():
    req = request.get_json() or {}
    vm_id = req.get('vm_id')
    group_id = req.get('group_id')
    if not vm_id:
        return jsonify({'error': 'Не указан vm_id'}), 400

    session = Session()
    try:
        updated = False
        with session.begin():
            if group_id is not None:
                found = _safe_exec(session, lambda: session.execute(select(group_vms.c.vm_id).where((group_vms.c.group_id == group_id) & (group_vms.c.vm_id == vm_id))).scalar())
                if found:
                    session.execute(delete(group_vms).where((group_vms.c.group_id == group_id) & (group_vms.c.vm_id == vm_id)))
                    session.execute(update(vms).where(vms.c.id == vm_id).values(group_id=None))
                    updated = True
            else:
                found = _safe_exec(session, lambda: session.execute(select(group_vms.c.group_id).where(group_vms.c.vm_id == vm_id)).scalar())
                if found:
                    session.execute(delete(group_vms).where(group_vms.c.vm_id == vm_id))
                    session.execute(update(vms).where(vms.c.id == vm_id).values(group_id=None))
                    updated = True
    finally:
        session.close()

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
    try:
        hours = min(max(int(req.get('hours', 24)), 1), 24)
    except Exception:
        hours = 24
    if not vm_id:
        return jsonify({'error': 'Не указан vm_id'}), 400

    session = Session()
    expires = None
    try:
        with session.begin():
            row = _safe_exec(session, lambda: session.execute(select(vms).where(vms.c.id == vm_id).with_for_update()).mappings().fetchone())
            if not row:
                return jsonify({'error': 'VM не найдена'}), 404
            if row['booked_by']:
                return jsonify({'error': 'VM уже забронирована'}), 400

            expires = datetime.now() + timedelta(hours=hours)
            session.execute(update(vms).where(vms.c.id == vm_id).values(booked_by=user_email, expires_at=expires.isoformat()))
            session.execute(insert(bookings).values(
                user_email=user_email, vm_id=vm_id, start=datetime.now().isoformat(), end=expires.isoformat(), action='book'
            ))
    finally:
        session.close()

    socketio.emit('notification', {'msg': f"{user_email} забронировал VM {vm_id} до {expires.strftime('%Y-%m-%d %H:%M:%S')}"})
    schedule_jobs()
    return jsonify(get_vm(vm_id))

@app.route('/renew', methods=['POST'])
def renew_vm():
    user_email = request.cookies.get('user_email')
    if not user_email:
        return jsonify({'error': 'Требуется авторизация'}), 401
    req = request.get_json() or {}
    vm_id = req.get('vm_id')
    try:
        hours = min(max(int(req.get('hours', 24)), 1), 24)
    except Exception:
        hours = 24
    if not vm_id:
        return jsonify({'error': 'Не указан vm_id'}), 400

    session = Session()
    try:
        with session.begin():
            row = _safe_exec(session, lambda: session.execute(select(vms).where(vms.c.id == vm_id).with_for_update()).mappings().fetchone())
            if not row:
                return jsonify({'error': 'VM не найдена'}), 404
            if row['booked_by'] != user_email:
                return jsonify({'error': 'Вы не бронировали эту VM'}), 403
            try:
                expires_old = datetime.fromisoformat(row['expires_at']) if row['expires_at'] else datetime.now()
            except Exception:
                expires_old = datetime.now()
            new_expires = expires_old + timedelta(hours=hours)
            session.execute(update(vms).where(vms.c.id == vm_id).values(expires_at=new_expires.isoformat()))
            session.execute(insert(bookings).values(user_email=user_email, vm_id=vm_id, start=datetime.now().isoformat(), end=new_expires.isoformat(), action='renew'))
    finally:
        session.close()

    socketio.emit('notification', {'msg': f"{user_email} продлил бронь VM {vm_id} до {new_expires.strftime('%Y-%m-%d %H:%M:%S')}"})
    schedule_jobs()
    return jsonify(get_vm(vm_id))

@app.route('/me', methods=['GET'])
def get_me():
    user_email = request.cookies.get('user_email')
    if not user_email:
        return jsonify({'authenticated': False}), 200
    if not user_exists(user_email):
        return jsonify({'authenticated': False}), 200
    bookings_data = get_user_bookings(user_email)
    return jsonify({'authenticated': True, 'email': user_email, 'bookings': bookings_data}), 200

@app.route('/queue/join', methods=['POST'])
def join_queue():
    """
    All DB actions done in transaction to avoid race conditions.
    """
    user_email = request.cookies.get('user_email')
    if not user_email:
        return jsonify({'error': 'Требуется авторизация'}), 401

    req = request.get_json() or {}
    vm_id = req.get('vm_id')
    if not vm_id:
        return jsonify({'error': 'Не указан vm_id'}), 400

    owner = None
    next_pos = None

    session = Session()
    try:
        try:
            with session.begin():
                vm_row = _safe_exec(session, lambda: session.execute(
                    select(vms.c.id, vms.c.booked_by).where(vms.c.id == vm_id).with_for_update()
                ).mappings().fetchone())
                if not vm_row:
                    return jsonify({'error': 'VM не найдена'}), 404

                existing_emails = _safe_exec(session, lambda: session.execute(
                    select(queue.c.email).where(queue.c.vm_id == vm_id).order_by(queue.c.position).with_for_update()
                ).scalars().all())

                if user_email in existing_emails:
                    position = existing_emails.index(user_email) + 1
                    return jsonify({'status': 'already_in_queue', 'position': position}), 200

                if len(existing_emails) >= 10:
                    return jsonify({'error': 'Очередь заполнена (максимум 10)'}), 400

                owner = vm_row['booked_by']
                if owner == user_email:
                    return jsonify({'error': 'Вы уже владеете этой VM'}), 400

                next_pos = len(existing_emails) + 1
                session.execute(insert(queue).values(vm_id=vm_id, email=user_email, position=next_pos))
        except (OperationalError, InterfaceError, DatabaseError, PendingRollbackError) as exc:
            app.logger.exception("DB error in join_queue for vm %s user %s", vm_id, user_email)
            try:
                session.rollback()
            except Exception:
                pass
            return jsonify({'error': 'Внутренняя ошибка базы данных'}), 500
    finally:
        session.close()

    if owner:
        app.logger.info("join_queue: notifying owner=%s about new queue entry by %s for vm=%s", owner, user_email, vm_id)
        try:
            socketio.emit('notification', {'msg': f"{user_email} встал(а) в очередь на VM {vm_id}", 'target': owner})
        except Exception:
            app.logger.exception("Failed to emit socket notification in join_queue")
        try:
            send_email_notification(to_email=owner, subject=f"[LoadZone] На вашу VM {vm_id} появилась очередь", body=f"Пользователь {user_email} встал(а) в очередь на VM {vm_id}.")
            app.logger.info("join_queue: send_email_notification invoked for owner=%s", owner)
        except Exception:
            app.logger.exception("join_queue: send_email_notification raised exception for owner=%s", owner)

    return jsonify({'status': 'ok', 'position': next_pos}), 200

@app.route('/queue/leave', methods=['POST'])
def leave_queue():
    user_email = request.cookies.get('user_email')
    if not user_email:
        return jsonify({'error': 'Требуется авторизация'}), 401
    req = request.get_json() or {}
    vm_id = req.get('vm_id')
    if not vm_id:
        return jsonify({'error': 'Не указан vm_id'}), 400

    session = Session()
    try:
        try:
            with session.begin():
                row = _safe_exec(session, lambda: session.execute(
                    select(queue.c.id, queue.c.position).where((queue.c.vm_id == vm_id) & (queue.c.email == user_email)).with_for_update()
                ).mappings().fetchone())
                if not row:
                    return jsonify({'error': 'Вы не в очереди'}), 400
                session.execute(delete(queue).where(queue.c.id == row['id']))
                remaining = _safe_exec(session, lambda: session.execute(
                    select(queue.c.id, queue.c.email).where(queue.c.vm_id == vm_id).order_by(queue.c.position).with_for_update()
                ).mappings().all())
                for pos, r in enumerate(remaining, start=1):
                    session.execute(update(queue).where(queue.c.id == r['id']).values(position=pos))
        except (OperationalError, InterfaceError, DatabaseError, PendingRollbackError) as exc:
            app.logger.exception("DB error in leave_queue for vm %s user %s", vm_id, user_email)
            try:
                session.rollback()
            except Exception:
                pass
            return jsonify({'error': 'Внутренняя ошибка базы данных'}), 500
    finally:
        session.close()

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

    session = Session()
    try:
        with session.begin():
            row = _safe_exec(session, lambda: session.execute(select(vms.c.booked_by).where(vms.c.id == vm_id).with_for_update()).mappings().fetchone())
            if not row:
                return jsonify({'error': 'VM не найдена'}), 404
            if row['booked_by'] != user_email:
                return jsonify({'error': 'Вы не бронировали эту VM'}), 403
            session.execute(update(vms).where(vms.c.id == vm_id).values(booked_by=None, expires_at=None))
            session.execute(insert(bookings).values(user_email=user_email, vm_id=vm_id, start=datetime.now().isoformat(), end=None, action='cancel'))
    finally:
        session.close()

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

    session = Session()
    try:
        if not _safe_exec(session, lambda: session.execute(select(groups.c.id).where(groups.c.id == group_id)).scalar()):
            return jsonify({'error': 'Группа не найдена'}), 404
        with session.begin():
            vm_ids = _safe_exec(session, lambda: session.execute(select(group_vms.c.vm_id).where(group_vms.c.group_id == group_id)).scalars().all())
            for vm_id in vm_ids:
                session.execute(update(vms).where(vms.c.id == vm_id).values(group_id=None))
            session.execute(delete(group_vms).where(group_vms.c.group_id == group_id))
            session.execute(delete(groups).where(groups.c.id == group_id))
    finally:
        session.close()

    socketio.emit('notification', {'msg': f"Группа {group_id} удалена"})
    return jsonify({'status': 'ok'})

# --- Release / scheduler helpers ---
def remove_notify_release_jobs():
    try:
        for job in scheduler.get_jobs():
            jid = job.id or ""
            if jid.startswith('notify_') or jid.startswith('release_'):
                try:
                    scheduler.remove_job(jid)
                except Exception:
                    app.logger.debug("Failed to remove job %s", jid, exc_info=True)
    except Exception:
        app.logger.debug("Could not iterate scheduler jobs", exc_info=True)

def release_vm(vm_id):
    """
    Release VM: clear booked_by/expires_at, notify first in queue and add booking record.
    This function opens and closes its own DB session.
    """
    session = Session()
    first = None
    try:
        with session.begin():
            row = _safe_exec(session, lambda: session.execute(select(vms).where(vms.c.id == vm_id).with_for_update()).mappings().fetchone())
            if not row:
                return
            user_email = row['booked_by']
            if user_email and user_exists(user_email):
                session.execute(insert(bookings).values(user_email=user_email, vm_id=vm_id, start=datetime.now().isoformat(), end=None, action='release'))
            session.execute(update(vms).where(vms.c.id == vm_id).values(booked_by=None, expires_at=None))
            first = _safe_exec(session, lambda: session.execute(select(queue.c.email).where(queue.c.vm_id == vm_id).order_by(queue.c.position).limit(1)).scalars().first())
            # If first obtained, remove it from queue and renumber — do that in separate transaction to be safe
            if first:
                session.execute(delete(queue).where((queue.c.vm_id == vm_id) & (queue.c.email == first)))
                remaining = _safe_exec(session, lambda: session.execute(select(queue.c.id, queue.c.email).where(queue.c.vm_id == vm_id).order_by(queue.c.position)).mappings().all())
                for pos, r in enumerate(remaining, start=1):
                    session.execute(update(queue).where(queue.c.id == r['id']).values(position=pos))
    finally:
        session.close()

    # Remove old scheduled jobs for this VM (noop if not present)
    remove_scheduled_jobs_for_vm(vm_id)

    if first:
        socketio.emit('notification', {'msg': f"VM {vm_id} освобождена — сейчас вы можете её забронировать", 'target': first})
        try:
            send_email_notification(to_email=first, subject=f"[LoadZone] VM {vm_id} освобождена", body=f"VM {vm_id} освобождена — вы первый в очереди и можете её забронировать.")
            app.logger.info("release_vm: send_email_notification invoked for %s", first)
        except Exception:
            app.logger.exception("release_vm: failed to call send_email_notification for %s", first)

    socketio.emit('notification', {'msg': f"VM {vm_id} освобождена"})

def schedule_jobs():
    """
    Cancel existing notify/release jobs and schedule new ones according to current VM expirations.
    Uses db_lock to avoid races when scheduler runs concurrently with request handlers.
    """
    with db_lock:
        try:
            remove_notify_release_jobs()
        except Exception:
            app.logger.exception("Failed to remove old notify/release jobs")

        vms_list = get_all_vms()
        now = datetime.now()
        for vm in vms_list:
            try:
                if vm.get('booked_by') and vm.get('expires_at'):
                    expires = _parse_iso(vm['expires_at'])
                    if not expires:
                        continue
                    notify_time = expires - timedelta(hours=1)
                    # schedule notify (1 hour before) if in future
                    if notify_time > now:
                        queue_emails = vm.get('queue') or []
                        if queue_emails:
                            first = queue_emails[0]
                            def notify_first_callable(vm_id=vm['id'], target=first):
                                def _fn():
                                    try:
                                        socketio.emit('notification', {'msg': f"VM {vm_id}: через час бронь истечёт — вы следующий в очереди.", 'target': target})
                                        send_email_notification(to_email=target, subject=f"[LoadZone] Вы следующий в очереди VM {vm_id}", body=f"VM {vm_id}: текущая бронь истекает через час — вы следующий в очереди.")
                                    except Exception:
                                        app.logger.exception("notify-first job failed for VM %s", vm_id)
                                return _fn
                            try:
                                scheduler.add_job(func=notify_first_callable(), trigger='date', run_date=notify_time, id=f"notify_{vm['id']}", replace_existing=True)
                            except Exception:
                                app.logger.exception("Failed to schedule notify job for vm %s", vm.get('id'))
                        else:
                            try:
                                scheduler.add_job(func=lambda vm_id=vm['id']: socketio.emit('notification', {'msg': f"Бронь VM {vm_id} истекает через час!"}), trigger='date', run_date=notify_time, id=f"notify_{vm['id']}", replace_existing=True)
                            except Exception:
                                app.logger.exception("Failed to schedule generic notify for vm %s", vm.get('id'))

                    # schedule release at exact expiry if in future
                    if expires > now:
                        try:
                            scheduler.add_job(func=lambda vm_id=vm['id']: release_vm(vm_id), trigger='date', run_date=expires, id=f"release_{vm['id']}", replace_existing=True)
                        except Exception:
                            app.logger.exception("Failed to schedule release job for vm %s", vm.get('id'))
            except Exception:
                app.logger.exception("schedule_jobs: problem scheduling for vm %s", vm.get('id'))

def remove_scheduled_jobs_for_vm(vm_id):
    for jid in [f"notify_{vm_id}", f"release_{vm_id}"]:
        try:
            scheduler.remove_job(jid)
        except Exception:
            pass

# --- New robust periodic monitor ---
@db_retry()
def check_and_release_expired_vms():
    """
    Periodic job that scans for VMs with expires_at <= now and booked_by IS NOT NULL,
    and calls release_vm(vm_id) for each expired VM.

    Important: this function creates and closes its own DB session and does NOT
    reuse session objects across scheduler runs/threads.
    """
    now = datetime.now()
    session = Session()
    try:
        # Select vms that have a booked_by and a non-null expires_at
        rows = _safe_exec(session, lambda: session.execute(
            select(vms.c.id, vms.c.expires_at, vms.c.booked_by)
            .where(and_(vms.c.booked_by != None, vms.c.expires_at != None))
        ).mappings().all())
    finally:
        session.close()

    if not rows:
        return

    expired = []
    for r in rows:
        vm_id = r['id']
        expires_at = _parse_iso(r['expires_at'])
        if not expires_at:
            continue
        if expires_at <= now:
            expired.append(vm_id)

    if not expired:
        return

    app.logger.info("check_and_release_expired_vms: found %d expired VMs: %s", len(expired), expired)

    # Release each expired VM — release_vm uses its own session
    for vm_id in expired:
        try:
            app.logger.info("check_and_release_expired_vms: releasing VM %s", vm_id)
            release_vm(vm_id)
        except Exception:
            app.logger.exception("check_and_release_expired_vms: failed to release vm %s", vm_id)

# --- History purge (unchanged logic but kept safe) ---
def purge_old_history():
    now = datetime.now()
    cutoff = now - timedelta(hours=1)
    session = Session()
    try:
        rows = _safe_exec(session, lambda: session.execute(
            select(bookings.c.id, bookings.c.user_email, bookings.c.vm_id, bookings.c.start, bookings.c.end, bookings.c.action)
            .order_by(bookings.c.user_email, bookings.c.vm_id, bookings.c.start)
        ).all())
    finally:
        session.close()

    from collections import defaultdict
    users_map = defaultdict(lambda: defaultdict(list))
    for r in rows:
        users_map[r[1]][r[2]].append({'id': r[0], 'user_email': r[1], 'vm_id': r[2], 'start': r[3], 'end': r[4], 'action': r[5]})

    keep_records = []
    for email, per_vm in users_map.items():
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
                    keep_records.append(ev)

    session = Session()
    try:
        with session.begin():
            session.execute(delete(bookings))
            for r in keep_records:
                session.execute(insert(bookings).values(id=r['id'], user_email=r['user_email'], vm_id=r['vm_id'], start=r['start'], end=r['end'], action=r['action']))
    finally:
        session.close()

    if keep_records:
        app.logger.info("purge_old_history: cleaned outdated booking records")

# --- Schedule periodic jobs at startup ---
try:
    scheduler.add_job(func=purge_old_history, trigger='interval', hours=1, id='purge_old_history_job', replace_existing=True)
except Exception:
    app.logger.exception("Failed to schedule purge_old_history_job", exc_info=True)

try:
    scheduler.add_job(func=check_and_release_expired_vms, trigger='interval', seconds=60, id='check_and_release_expired_vms', replace_existing=True)
    app.logger.info("Scheduled periodic monitor check_and_release_expired_vms every 60s")
except Exception:
    app.logger.exception("Failed to schedule check_and_release_expired_vms", exc_info=True)

# initial runs
try:
    purge_old_history()
except Exception:
    app.logger.exception("Initial purge_old_history failed")

try:
    check_and_release_expired_vms()
except Exception:
    app.logger.exception("Initial check_and_release_expired_vms failed")

# --- SocketIO connect hook (keeps schedule in sync when clients connect) ---
@socketio.on('connect')
def on_connect(auth=None):
    try:
        app.logger.debug("Socket connect, scheduling jobs")
        schedule_jobs()
    except Exception:
        app.logger.exception("schedule_jobs failed on connect")

# --- Run ---
if __name__ == '__main__':
    if _HAS_EVENTLET:
        socketio.run(app, host='0.0.0.0', port=5000)
    else:
        socketio.run(app, host='0.0.0.0', port=5000)
