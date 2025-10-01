import json
import threading
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
MAX_QUEUE_SIZE = 10


def load_data():
    with lock:
        with open(data_file, 'r', encoding='utf-8') as f:
            return json.load(f)


def save_data(data):
    with lock:
        with open(data_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, default=str, indent=2)


def schedule_jobs():
    """
    Планируем уведомления и освобождения VM.
    Для каждой забронированной VM:
      - уведомление за 1 час (как раньше)
      - если очередь не пустая — дополнительное уведомление первому в очереди за 1 час
      - задача на полную очистку/освобождение в момент expires
    """
    data = load_data()
    now = datetime.utcnow()
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
                try:
                    scheduler.add_job(
                        func=lambda vm_id=vm['id']: socketio.emit(
                            'notification',
                            {'msg': f"Бронь VM {vm_id} истекает через час!", 'vm': vm_id}
                        ),
                        trigger='date',
                        run_date=notify_time,
                        id=f"notify_{vm['id']}"
                    )
                except Exception:
                    pass

                try:
                    if vm.get('queue'):
                        next_email = vm['queue'][0]
                        scheduler.add_job(
                            func=lambda vm_id=vm['id'], email=next_email: socketio.emit(
                                'notification',
                                {'msg': f"Вы следующий в очереди на VM {vm_id}", 'target': email, 'vm': vm_id}
                            ),
                            trigger='date',
                            run_date=notify_time,
                            id=f"queue_next_{vm['id']}"
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
    for jid in [f"notify_{vm_id}", f"release_{vm_id}", f"queue_next_{vm_id}"]:
        try:
            scheduler.remove_job(jid)
        except Exception:
            pass


def release_vm(vm_id):
    """
    Освобождает VM: снимает бронь, удаляет задачи. Если очередь непустая, уведомляет первого в очереди, что VM доступна.
    (Автоматического назначения/бронирования следующему не делаю — этого не просили.)
    """
    data = load_data()
    modified = False
    for vm in data['vms']:
        if vm['id'] == vm_id:
            user_email = vm.get('booked_by')
            if user_email and user_email in data.get('users', {}):
                user = data['users'][user_email]
                user.setdefault('bookings', []).append({
                    'vm_id': vm_id,
                    'start': datetime.utcnow().isoformat(),
                    'action': 'release'
                })

            vm['booked_by'] = None
            vm['expires_at'] = None
            modified = True

            remove_scheduled_jobs_for_vm(vm_id)

            q = vm.get('queue') or []
            if q:
                next_email = q.pop(0)
                save_data(data)
                socketio.emit('notification', {
                    'msg': f"VM {vm_id} стала доступна. Вы первый в очереди.",
                    'target': next_email,
                    'vm': vm_id
                })
                schedule_jobs()
                return

            break

    if modified:
        save_data(data)
        socketio.emit('notification', {'msg': f"VM {vm_id} освобождена"})


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
            'created': datetime.utcnow().isoformat(),
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

            expires = datetime.utcnow() + timedelta(hours=hours)
            vm['booked_by'] = user_email
            vm['expires_at'] = expires.isoformat()
            vm.setdefault('queue', [])

            user.setdefault('bookings', []).append({
                'vm_id': vm_id,
                'start': datetime.utcnow().isoformat(),
                'end': expires.isoformat(),
                'action': 'book'
            })

            save_data(data)
            socketio.emit('notification', {
                'msg': f"{user_email} забронировал VM {vm_id} до {expires.strftime('%Y-%m-%d %H:%M:%S UTC')}",
                'vm': vm_id
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
                'start': datetime.utcnow().isoformat(),
                'end': expires.isoformat(),
                'action': 'renew'
            })

            save_data(data)
            socketio.emit('notification', {
                'msg': f"{user_email} продлил бронь VM {vm_id} до {expires.strftime('%Y-%m-%d %H:%M:%S UTC')}",
                'vm': vm_id
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
                    'start': datetime.utcnow().isoformat(),
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
                'start': datetime.utcnow().isoformat(),
                'action': 'cancel'
            })
            save_data(data)
            remove_scheduled_jobs_for_vm(vm_id)
            socketio.emit('notification', {'msg': f"{user_email} отменил бронь VM {vm_id}", 'vm': vm_id})
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
                vm.setdefault('queue', [])
                added.append(vm_id)
                break

    if not added:
        return jsonify({'error': 'Ни одна VM не найдена для добавления'}), 404

    save_data(data)
    socketio.emit('notification', {'msg': f"Добавлено {len(added)} VM в группу {group['name']}"})
    return jsonify({'added': added, 'group': group})


@app.route('/queue/join', methods=['POST'])
def queue_join():
    user_email = request.cookies.get('user_email')
    if not user_email:
        return jsonify({'error': 'Требуется авторизация'}), 401

    req = request.get_json()
    vm_id = req.get('vm_id')
    if not vm_id:
        return jsonify({'error': 'Не указан vm_id'}), 400

    data = load_data()
    vm = None
    for v in data['vms']:
        if v['id'] == vm_id:
            vm = v
            break

    if not vm:
        return jsonify({'error': 'VM не найдена'}), 404

    vm.setdefault('queue', [])
    if vm.get('booked_by') == user_email:
        return jsonify({'error': 'Вы уже бронируете эту VM'}), 400

    if user_email in vm['queue']:
        return jsonify({'error': 'Вы уже в очереди'}), 400

    if len(vm['queue']) >= MAX_QUEUE_SIZE:
        return jsonify({'error': f'Очередь переполнена (макс {MAX_QUEUE_SIZE})'}), 400

    vm['queue'].append(user_email)
    save_data(data)

    if vm.get('booked_by'):
        socketio.emit('notification', {
            'msg': f"На VM {vm_id} встала очередь ({len(vm['queue'])} чел.)",
            'target': vm.get('booked_by'),
            'vm': vm_id
        })

    socketio.emit('notification', {
        'msg': f"Вы в очереди на VM {vm_id} (позиция {len(vm['queue'])})",
        'target': user_email,
        'vm': vm_id
    })

    schedule_jobs()

    return jsonify({'status': 'ok', 'position': len(vm['queue']), 'queue': vm['queue']})


@app.route('/queue/leave', methods=['POST'])
def queue_leave():
    user_email = request.cookies.get('user_email')
    if not user_email:
        return jsonify({'error': 'Требуется авторизация'}), 401

    req = request.get_json() or {}
    vm_id = req.get('vm_id')

    data = load_data()
    modified = False
    removed_positions = {}

    for vm in data['vms']:
        if vm_id and vm['id'] != vm_id:
            continue
        q = vm.setdefault('queue', [])
        if user_email in q:
            q[:] = [e for e in q if e != user_email]
            removed_positions[vm['id']] = True
            modified = True

    if not modified:
        return jsonify({'error': 'Вы не состоите в очереди'}), 400

    save_data(data)
    schedule_jobs()
    socketio.emit('notification', {'msg': f"{user_email} вышел(ла) из очереди", 'target': user_email})
    return jsonify({'status': 'ok', 'removed': list(removed_positions.keys())})


@socketio.on('connect')
def on_connect():
    schedule_jobs()


if __name__ == '__main__':
    socketio.run(app, host='0.0.0.0', port=5000)
