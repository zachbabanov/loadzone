# LoadZone: Система управления виртуальными машинами

**LoadZone** — веб-приложение для управления виртуальными машинами (VM): бронирование, группировка, администрирование и уведомления в реальном времени.

## Кратко о проекте

* Веб-интерфейс (чистый HTML/CSS/JS) с адаптивной вёрсткой и поддержкой светлой/тёмной темы.
* Бэкенд на Flask с WebSocket-уведомлениями (Flask-SocketIO) и планировщиком задач (APScheduler).
* Простое хранение состояния в JSON-файле (`data.json`).

---

## Архитектура

### Бэкенд (Python)

* **Фреймворк**: Flask  
* **Реалтайм**: Flask-SocketIO  
* **Планировщик**: APScheduler (автоосвобождение VM, уведомления)  
* **Хранилище**: `data.json` (простая файловая БД)

### Фронтенд

* HTML/CSS/Vanilla JS
* Модальные окна приложения для всех операций (создание VM/группы, бронирование, добавление VM в группу и т.д.)
* WebSocket (socket.io-client) — получение уведомлений в реальном времени

---

## Модель данных

```json
{
  "vms": [
    {
      "id": "string",            // уникальный идентификатор VM, например "server-01"
      "group": "number|null",    // id группы или null
      "booked_by": "string|null",// email пользователя или null
      "expires_at": "ISO|null"   // время освобождения или null
    }
  ],
  "groups": [
    {
      "id": "number",           // ID группы
      "name": "string",         // название группы
      "vm_ids": ["string"]      // список id VM в группе
    }
  ],
  "users": {
    "user@example.com": {
      "created": "ISO",
      "bookings": [
        {
          "vm_id": "string",
          "start": "ISO",
          "end": "ISO|null",
          "action": "book|renew|release"
        }
      ]
    }
  }
}
```

---

## API / Взаимодействие (frontend ↔ backend)

> Ниже перечислены основные эндпоинты, которые использует фронтенд. (Реализация на сервере должна соответствовать этим контрактам.)

### Получение состояния

* `GET /vms`

```json
{ "vms": [...], "groups": [...] }
```

### Авторизация

* `POST /groups`

```json
{ "name": "Продакшн", "vm_ids": ["server-01","server-02"] }
```

* `GET /me`

```json
{ authenticated: true/false, email: "..."}
```

### Добавление VM

* `POST /add-vm`

```json
{ "id": "server-01", "group_id": 1|null }
```
### Создание группы

* `POST /groups`

```json
{ "name": "Продакшн", "vm_ids": ["server-01","server-02"] }
```

### Добавление уже существующих VM в группу

* `POST /groups/<group_id>/add-existing-vms`

```json
{ "vm_ids": ["server-03","server-07"] }
```

### Удаление VM

* `POST /delete-vm`

```json
{ "vm_id": "server-01" }
```


### Удаление группы

* `POST /delete-group`

```json
{ "group_id": 5 }
```

### Исключение VM из группы

* `POST /remove-vm-from-group`

```json
{ "vm_id": "server-03", "group_id": 5 (опционально) }
```

> Если group_id не передан — сервер исключает VM из её текущей группы.

### Бронирование / Продление

* `POST /book`

```json
{ "vm_id": "server-02", "hours": 4 }
```

* `POST /renew`

```json
{ "vm_id": "server-02", "hours": 2 }
```

### Отмена бронирования

* `POST /cancel`

```json
{ "vm_id": "server-02" }
```

### Уведомления (WebSocket)

* Сервер отправляет уведомления через `socket.io`: событие `notification` с объектом `{ msg: "..." }`

---

## Правила очистки истории

Чтобы `data.json` не разрастался записями, реализована автоматическая очистка старых событий в
`user.*.bookings`:

* Если запись имеет `action == cancel` или `release` и ее `start` старее 1 часа - запись удаляется
* Если запись имеет `action == book` и есть `end` (при `end` старее одного часа) - запись удаляется
* Если для одного `vm_id` у пользователя есть пара `book -> cancel` или `book -> release` и время
`cancel`/`release` старее 1 часа - удаляются обе записи
* 
Задача выполняется каждый час и записывает изменения в `data.json` только при необходимости (если
изменения были)

---

## Безопасность и почта (SMTP)

Для отправки писем используется модуль `smtplib`. Все SMTP-учетные данные получаются из 
переменных окружения:

  * `SMTP_HOST` - хост SMTP сервера 
  * `SMTP_PORT` - порт (обычно `465` для SSL или `587` для STARTTLS)
  * `SMTP_USER` - логин
  * `SMTP_PASS` - пароль
  * `SMTP_FROM` - (опционально) адрес отправителя; по умолчанию используется `SMTP_USER`

*Важно:** не храните логины/пароли в репозитории. Используйте:

  * переменные окружения в CI/сервере
  * Docker secrets
  * Vault или другой менеджер

Отправка писем запускается в отдельном потоке, чтобы не блокировать основной поток сервера

---

## Установка и запуск

### Требования

* Python 3.7+
* Рекомендуется виртуальное окружение

### Установка зависимостей

1. Создайте виртуальное окружение (опционально)

```bash
python -m venv venv
source venv/bin/activate   # Linux / macOS
# или
venv\Scripts\activate      # Windows
```

2. Установите библиотеки, которые понадобятся для `socketio`

```bash
pip install wheel netifaces
```

3. Установите зависимости из файла `requirements.txt`

```bash
pip install -r requirements.txt
```

### Переменные окружения

```bash
export SMTP_HOST="smtp.example.com"
export SMTP_PORT="465"
export SMTP_USER="loadzone-notify@example.com"
export SMTP_PASS="секрет"
export SMTP_FROM="loadzone-notify@example.com"
```

### Запуск

```bash
pip install -r requirements.txt
```

Откройте в браузере: `http://localhost:5000`
