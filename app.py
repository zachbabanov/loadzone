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

def load_data():
    with lock:
        with open(data_file, 'r', encoding='utf-8') as f:
            return json.load(f)

def save_data(data):
    with lock:
        with open(data_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, default=str, indent=2)

def schedule_jobs():
    data = load_data()
    now = datetime.utcnow()
    scheduler.remove_all_jobs()
    for vm in data['vms']:
        if vm['booked_by'] and vm['expires_at']:
            expires = datetime.fromisoformat(vm['expires_at'])
            notify_time = expires - timedelta(hours=1)
            if notify_time > now:
                scheduler.add_job(
                    func=lambda vm_id=vm['id']: socketio.emit(
                        'notification',
                        {'msg': f"Бронь VM {vm_id} истекает через час!"}
                    ),
                    trigger='date',
                    run_date=notify_time,
                    id=f"notify_{vm['id']}"
                )
            if expires > now:
                scheduler.add_job(
                    func=lambda vm_id=vm['id']: release_vm(vm_id),
                    trigger='date',
                    run_date=expires,
                    id=f"release_{vm['id']}"
                )

def release_vm(vm_id):
    data = load_data()
    for vm in data['vms']:
        if vm['id'] == vm_id:
            user_email = vm['booked_by']
            if user_email and user_email in data.get('users', {}):
                user = data['users'][user_email]
                user['bookings'].append({
                    'vm_id': vm_id,
                    'start': datetime.utcnow().isoformat(),
                    'action': 'release'
                })
            
            vm['booked_by'] = None
            vm['expires_at'] = None
            save_data(data)
            socketio.emit('notification', {'msg': f"VM {vm_id} освобождена"})
            break

@app.route('/')
def index():
    return send_from_directory('templates', 'index.html')

@app.route('/vms', methods=['GET'])
def list_vms():
    data = load_data()
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
        'expires_at': None
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
            if vm['booked_by']:
                return jsonify({'error': 'VM уже забронирована'}), 400
            
            expires = datetime.utcnow() + timedelta(hours=hours)
            vm['booked_by'] = user_email
            vm['expires_at'] = expires.isoformat()
            
            user['bookings'].append({
                'vm_id': vm_id,
                'start': datetime.utcnow().isoformat(),
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
            
            user['bookings'].append({
                'vm_id': vm_id,
                'start': datetime.utcnow().isoformat(),
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

@socketio.on('connect')
def on_connect():
    schedule_jobs()

if __name__ == '__main__':
    socketio.run(app, host='0.0.0.0', port=5000)
