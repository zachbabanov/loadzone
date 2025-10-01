import json
import threading
import os
import smtplib
import ssl
from email.message import EmailMessage
from datetime import datetime, timedelta
from flask import Flask, jsonify, request, send_from_directory, make_response
from flask_socketio import SocketIO
from apscheduler.schedulers.background import BackgroundScheduler

app = Flask(__name__, static_folder='static', template_folder='templates')
socketio = SocketIO(app, cors_allowed_origins='*')
scheduler = BackgroundScheduler()
scheduler.start()

data_file = 'data.json'
lock = threading.Lock()


def load_data():
    with lock:
        with open(data_file, 'r', encoding='utf-8') as f:
            return json.load(f)


def save_data(data):
    with lock:
        with open(data_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, default=str, indent=2)


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


def schedule_jobs():
    data = load_data()
    now = datetime.now()
    try:
        scheduler.remove_all_jobs()
    except Exception:
        pass

    for vm in data['vms']:
        if vm.get('booked_by') and vm.get('expires_at'):
            try:
                expires = datetime.fromisoformat(vm['expires_at'])
            except Exception:
                continue

            notify_time = expires - timedelta(hours=1)
            if notify_time > now:
                queue = vm.get('queue', []) or []
                if queue:
                    first_email = queue[0]

                    def make_notify_first(vm_id=vm['id'], first=first_email):
                        def _fn():
                            try:
                                socketio.emit(
                                    'notification',
                                    {
                                        'msg': f"VM {vm_id}: через час бронь истечёт — вы следующий в очереди.",
                                        'target': first
                                    }
                                )
                                send_email_notification(
                                    to_email=first,
                                    subject=f"[LoadZone] Вы следующий в очереди VM {vm_id}",
                                    body=f"VM {vm_id}: текущая бронь истекает через час — вы следующий в очереди."
                                )
                            except Exception:
                                app.logger.exception("notify-first job failed for VM %s", vm_id)
                        return _fn

                    try:
                        scheduler.add_job(
                            func=make_notify_first(),
                            trigger='date',
                            run_date=notify_time,
                            id=f"notify_{vm['id']}"
                        )
                    except Exception:
                        app.logger.exception("Failed to schedule notify job for vm %s", vm.get('id'))

                else:
                    try:
                        scheduler.add_job(
                            func=lambda vm_id=vm['id']: socketio.emit(
                                'notification',
                                {'msg': f"Бронь VM {vm_id} истекает через час!"}
                            ),
                            trigger='date',
                            run_date=notify_time,
                            id=f"notify_{vm['id']}"
                        )
                    except Exception:
                        pass

            if expires > now:
                try:
                    scheduler.add_job(
                        func=lambda vm_id=vm['id']: release_vm(vm_id),
                        trigger='date',
                        run_date=expires,
                        id=f"release_{vm['id']}"
                    )
                except Exception:
                    pass


def remove_scheduled_jobs_for_vm(vm_id):
    for jid in [f"notify_{vm_id}", f"release_{vm_id}"]:
        try:
            scheduler.remove_job(jid)
        except Exception:
            pass


def release_vm(vm_id):
    data = load_data()
    for vm in data['vms']:
        if vm['id'] == vm_id:
            user_email = vm.get('booked_by')
            if user_email and user_email in data.get('users', {}):
                user = data['users'][user_email]
                user.setdefault('bookings', []).append({
                    'vm_id': vm_id,
                    'start': datetime.now().isoformat(),
                    'action': 'release'
                })

            vm['booked_by'] = None
            vm['expires_at'] = None

            queue = vm.get('queue', []) or []
            if queue:
                first = queue[0]
                socketio.emit('notification', {'msg': f"VM {vm_id} освобождена — сейчас вы можете её забронировать", 'target': first})
                send_email_notification(
                    to_email=first,
                    subject=f"[LoadZone] VM {vm_id} освобождена",
                    body=f"VM {vm_id} освобождена — вы первый в очереди и можете её забронировать."
                )
            save_data(data)
            remove_scheduled_jobs_for_vm(vm_id)
            socketio.emit('notification', {'msg': f"VM {vm_id} освобождена"})
            break


@app.route('/')
def index():
    return send_from_directory('templates', 'index.html')


@app.route('/vms', methods=['GET'])
def list_vms():
    data = load_data()
    for vm in data['vms']:
        vm.setdefault('queue', [])
    return jsonify({
        'vms': data['vms'],
        'groups': data.get('groups', [])
    })


@app.route('/groups', methods=['GET'])
def list_groups():
    data = load_data()
    return jsonify(data.get('groups', []))


@app.route('/groups', methods=['POST'])
def create_group():
    req = request.get_json()
    name = req.get('name')
    vm_ids = req.get('vm_ids', [])

    if not name:
        return jsonify({'error': 'Не указано название группы'}), 400

    data = load_data()

    for group in data.get('groups', []):
        if group['name'].lower() == name.lower():
            return jsonify({'error': 'Группа с таким именем уже существует'}), 400

    new_id = max((g['id'] for g in data.get('groups', [])), default=0) + 1
    group = {'id': new_id, 'name': name, 'vm_ids': vm_ids}
    data.setdefault('groups', []).append(group)

    for vm in data['vms']:
        if vm['id'] in vm_ids:
            vm['group'] = new_id

    save_data(data)
    socketio.emit('notification', {'msg': f"Создана группа «{name}»"})
    return jsonify(group), 201


@app.route('/add-vm', methods=['POST'])
def add_vm():
    user_email = request.cookies.get('user_email')
    if not user_email:
        return jsonify({'error': 'Требуется авторизация'}), 401

    req = request.get_json()
    vm_id = req.get('id')
    group_id = req.get('group_id')

    if not vm_id:
        return jsonify({'error': 'Не указан идентификатор VM'}), 400

    data = load_data()

    for vm in data['vms']:
        if vm['id'] == vm_id:
            return jsonify({'error': 'VM с таким идентификатором уже существует'}), 400

    group_exists = False
    if group_id is not None:
        for group in data.get('groups', []):
            if group['id'] == group_id:
                group_exists = True
                if vm_id not in group['vm_ids']:
                    group['vm_ids'].append(vm_id)
                break

    new_vm = {
        'id': vm_id,
        'group': group_id if group_exists else None,
        'booked_by': None,
        'expires_at': None,
        'queue': []
    }

    data['vms'].append(new_vm)
    save_data(data)

    socketio.emit('notification', {'msg': f"Добавлена новая VM: {vm_id}"})
    return jsonify(new_vm), 201


@app.route('/auth', methods=['POST'])
def auth():
    email = request.json.get('email', '').strip().lower()
    if not email or '@' not in email:
        return jsonify({'error': 'Некорректный email'}), 400

    data = load_data()
    users = data.setdefault('users', {})

    if email not in users:
        users[email] = {
            'created': datetime.now().isoformat(),
            'bookings': []
        }
        save_data(data)

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

    req = request.get_json()
    vm_id = req.get('vm_id')
    hours = min(max(int(req.get('hours', 24)), 1), 24)

    data = load_data()
    user = data['users'].get(user_email)
    if not user:
        return jsonify({'error': 'Пользователь не найден'}), 400

    for vm in data['vms']:
        if vm['id'] == vm_id:
            if vm.get('booked_by'):
                return jsonify({'error': 'VM уже забронирована'}), 400

            expires = datetime.now() + timedelta(hours=hours)
            vm['booked_by'] = user_email
            vm['expires_at'] = expires.isoformat()

            user.setdefault('bookings', []).append({
                'vm_id': vm_id,
                'start': datetime.now().isoformat(),
                'end': expires.isoformat(),
                'action': 'book'
            })

            save_data(data)
            socketio.emit('notification', {
                'msg': f"{user_email} забронировал VM {vm_id} до {expires.strftime('%Y-%m-%d %H:%M:%S UTC')}"
            })
            schedule_jobs()
            return jsonify(vm)

    return jsonify({'error': 'VM не найдена'}), 404


@app.route('/renew', methods=['POST'])
def renew_vm():
    user_email = request.cookies.get('user_email')
    if not user_email:
        return jsonify({'error': 'Требуется авторизация'}), 401

    req = request.get_json()
    vm_id = req.get('vm_id')
    hours = min(max(int(req.get('hours', 24)), 1), 24)

    data = load_data()
    user = data['users'].get(user_email)
    if not user:
        return jsonify({'error': 'Пользователь не найден'}), 400

    for vm in data['vms']:
        if vm['id'] == vm_id:
            if vm.get('booked_by') != user_email:
                return jsonify({'error': 'Вы не бронировали эту VM'}), 403

            expires = datetime.fromisoformat(vm['expires_at']) + timedelta(hours=hours)
            vm['expires_at'] = expires.isoformat()

            user.setdefault('bookings', []).append({
                'vm_id': vm_id,
                'start': datetime.now().isoformat(),
                'end': expires.isoformat(),
                'action': 'renew'
            })

            save_data(data)
            socketio.emit('notification', {
                'msg': f"{user_email} продлил бронь VM {vm_id} до {expires.strftime('%Y-%m-%d %H:%M:%S UTC')}"
            })
            schedule_jobs()
            return jsonify(vm)

    return jsonify({'error': 'VM не найдена'}), 404


@app.route('/me', methods=['GET'])
def get_me():
    user_email = request.cookies.get('user_email')
    if not user_email:
        return jsonify({'authenticated': False}), 200

    data = load_data()
    user = data.get('users', {}).get(user_email)
    if not user:
        return jsonify({'authenticated': False}), 200

    return jsonify({
        'authenticated': True,
        'email': user_email,
        'bookings': user.get('bookings', [])
    }), 200


@app.route('/delete-vm', methods=['POST'])
def delete_vm():
    user_email = request.cookies.get('user_email')
    if not user_email:
        return jsonify({'error': 'Требуется авторизация'}), 401

    req = request.get_json()
    vm_id = req.get('vm_id')
    if not vm_id:
        return jsonify({'error': 'Не указан vm_id'}), 400

    data = load_data()
    vm_found = False
    new_vms = []
    for vm in data['vms']:
        if vm['id'] == vm_id:
            vm_found = True
            if vm.get('booked_by') and vm['booked_by'] in data.get('users', {}):
                user = data['users'][vm['booked_by']]
                user.setdefault('bookings', []).append({
                    'vm_id': vm_id,
                    'start': datetime.now().isoformat(),
                    'action': 'deleted'
                })
            remove_scheduled_jobs_for_vm(vm_id)
            continue
        new_vms.append(vm)

    if not vm_found:
        return jsonify({'error': 'VM не найдена'}), 404

    data['vms'] = new_vms

    for group in data.get('groups', []):
        if vm_id in group.get('vm_ids', []):
            group['vm_ids'] = [x for x in group['vm_ids'] if x != vm_id]

    save_data(data)
    socketio.emit('notification', {'msg': f"VM {vm_id} удалена"})
    return jsonify({'status': 'ok'})


@app.route('/delete-group', methods=['POST'])
def delete_group():
    req = request.get_json()
    group_id = req.get('group_id')
    if group_id is None:
        return jsonify({'error': 'Не указан group_id'}), 400

    data = load_data()
    groups = data.get('groups', [])
    new_groups = []
    found = False
    for g in groups:
        if g['id'] == group_id:
            found = True
            for vm in data['vms']:
                if vm.get('group') == group_id:
                    vm['group'] = None
            continue
        new_groups.append(g)

    if not found:
        return jsonify({'error': 'Группа не найдена'}), 404

    data['groups'] = new_groups
    save_data(data)
    socketio.emit('notification', {'msg': f"Группа {group_id} удалена"})
    return jsonify({'status': 'ok'})


@app.route('/remove-vm-from-group', methods=['POST'])
def remove_vm_from_group():
    req = request.get_json()
    vm_id = req.get('vm_id')
    group_id = req.get('group_id')
    if not vm_id:
        return jsonify({'error': 'Не указан vm_id'}), 400

    data = load_data()
    updated = False

    if group_id is not None:
        for group in data.get('groups', []):
            if group['id'] == group_id and vm_id in group.get('vm_ids', []):
                group['vm_ids'] = [x for x in group['vm_ids'] if x != vm_id]
                updated = True
                break
    else:
        for group in data.get('groups', []):
            if vm_id in group.get('vm_ids', []):
                group['vm_ids'] = [x for x in group['vm_ids'] if x != vm_id]
                updated = True

    for vm in data['vms']:
        if vm['id'] == vm_id:
            vm['group'] = None
            updated = True
            break

    if not updated:
        return jsonify({'error': 'VM или группа не найдены / VM не состоит в группе'}), 404

    save_data(data)
    socketio.emit('notification', {'msg': f"VM {vm_id} исключена из группы"})
    return jsonify({'status': 'ok'})


@app.route('/cancel', methods=['POST'])
def cancel_booking():
    user_email = request.cookies.get('user_email')
    if not user_email:
        return jsonify({'error': 'Требуется авторизация'}), 401

    req = request.get_json()
    vm_id = req.get('vm_id')
    if not vm_id:
        return jsonify({'error': 'Не указан vm_id'}), 400

    data = load_data()
    user = data.get('users', {}).get(user_email)
    if not user:
        return jsonify({'error': 'Пользователь не найден'}), 400

    for vm in data['vms']:
        if vm['id'] == vm_id:
            if vm.get('booked_by') != user_email:
                return jsonify({'error': 'Вы не бронировали эту VM'}), 403

            vm['booked_by'] = None
            vm['expires_at'] = None
            user.setdefault('bookings', []).append({
                'vm_id': vm_id,
                'start': datetime.now().isoformat(),
                'action': 'cancel'
            })
            save_data(data)
            remove_scheduled_jobs_for_vm(vm_id)
            socketio.emit('notification', {'msg': f"{user_email} отменил бронь VM {vm_id}"})
            schedule_jobs()
            return jsonify({'status': 'ok'})

    return jsonify({'error': 'VM не найдена'}), 404


@app.route('/groups/<int:group_id>/add-existing-vms', methods=['POST'])
def add_existing_vms_to_group(group_id):
    req = request.get_json()
    vm_ids = req.get('vm_ids', [])
    if not isinstance(vm_ids, list) or not vm_ids:
        return jsonify({'error': 'vm_ids должен быть списком непустых идентификаторов'}), 400

    data = load_data()
    group = None
    for g in data.get('groups', []):
        if g['id'] == group_id:
            group = g
            break

    if not group:
        return jsonify({'error': 'Группа не найдена'}), 404

    added = []
    for vm_id in vm_ids:
        for vm in data['vms']:
            if vm['id'] == vm_id:
                if vm.get('group') != group_id:
                    vm['group'] = group_id
                if vm_id not in group.get('vm_ids', []):
                    group.setdefault('vm_ids', []).append(vm_id)
                added.append(vm_id)
                break

    if not added:
        return jsonify({'error': 'Ни одна VM не найдена для добавления'}), 404

    save_data(data)
    socketio.emit('notification', {'msg': f"Добавлено {len(added)} VM в группу {group['name']}"})
    return jsonify({'added': added, 'group': group})


@app.route('/queue/join', methods=['POST'])
def join_queue():
    user_email = request.cookies.get('user_email')
    if not user_email:
        return jsonify({'error': 'Требуется авторизация'}), 401

    req = request.get_json()
    vm_id = req.get('vm_id')
    if not vm_id:
        return jsonify({'error': 'Не указан vm_id'}), 400

    data = load_data()
    vm = next((v for v in data['vms'] if v['id'] == vm_id), None)
    if not vm:
        return jsonify({'error': 'VM не найдена'}), 404

    vm.setdefault('queue', [])
    queue = vm['queue']

    if user_email in queue:
        position = queue.index(user_email) + 1
        return jsonify({'status': 'already_in_queue', 'position': position}), 200

    if len(queue) >= 10:
        return jsonify({'error': 'Очередь заполнена (максимум 10)'}), 400

    if vm.get('booked_by') == user_email:
        return jsonify({'error': 'Вы уже владеете этой VM'}), 400

    queue.append(user_email)
    save_data(data)

    owner = vm.get('booked_by')
    if owner:
        msg = f"{user_email} встал(а) в очередь на VM {vm_id}"
        socketio.emit('notification', {'msg': msg, 'target': owner})
        send_email_notification(
            to_email=owner,
            subject=f"[LoadZone] На вашу VM {vm_id} появилась очередь",
            body=f"Пользователь {user_email} встал(а) в очередь на VM {vm_id}."
        )

    position = queue.index(user_email) + 1
    return jsonify({'status': 'ok', 'position': position}), 200


@app.route('/queue/leave', methods=['POST'])
def leave_queue():
    user_email = request.cookies.get('user_email')
    if not user_email:
        return jsonify({'error': 'Требуется авторизация'}), 401

    req = request.get_json()
    vm_id = req.get('vm_id')
    if not vm_id:
        return jsonify({'error': 'Не указан vm_id'}), 400

    data = load_data()
    vm = next((v for v in data['vms'] if v['id'] == vm_id), None)
    if not vm:
        return jsonify({'error': 'VM не найдена'}), 404

    vm.setdefault('queue', [])
    queue = vm['queue']
    if user_email in queue:
        queue = [x for x in queue if x != user_email]
        vm['queue'] = queue
        save_data(data)
        return jsonify({'status': 'ok'}), 200
    else:
        return jsonify({'error': 'Вы не в очереди'}), 400


@socketio.on('connect')
def on_connect():
    schedule_jobs()


if __name__ == '__main__':
    socketio.run(app, host='0.0.0.0', port=5000)
