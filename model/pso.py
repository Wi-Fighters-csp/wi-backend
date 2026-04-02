import os
import sqlite3
from datetime import datetime, timezone
import json

import jwt
from flask import current_app, request
from sqlalchemy import func
from werkzeug.security import check_password_hash, generate_password_hash

from __init__ import app
from model.user import User


class PSOUser:
    def __init__(self, record):
        self.id = record['id']
        self.uid = record['uid']
        self.name = record['name']
        self.email = record['email']
        self.role = record['role']
        self.password_hash = record['password_hash']

    def is_admin(self):
        return str(self.role).strip().lower() in {'admin', 'superadmin'}

    def is_teacher(self):
        return str(self.role).strip().lower() == 'teacher'

    def read(self):
        return {
            'id': self.id,
            'uid': self.uid,
            'name': self.name,
            'email': self.email,
            'role': self.role
        }


class PSOAuthService:
    @staticmethod
    def database_path():
        volumes_dir = os.path.join(app.instance_path, 'volumes')
        os.makedirs(volumes_dir, exist_ok=True)
        return os.path.join(volumes_dir, 'powayorchestra.db')

    @staticmethod
    def get_connection():
        connection = sqlite3.connect(PSOAuthService.database_path())
        connection.row_factory = sqlite3.Row
        connection.execute('PRAGMA foreign_keys = ON')
        return connection

    @staticmethod
    def default_email_for_uid(uid):
        return f'{uid}@powayorchestra.local'

    @staticmethod
    def shared_user_email(user):
        email = str(getattr(user, 'email', '') or '').strip().lower()
        if len(email) >= 3 and '@' in email and email != '?':
            return email
        return PSOAuthService.default_email_for_uid(user.uid)

    @staticmethod
    def normalize_practice_time(practice_time):
        try:
            return max(0, int(practice_time or 0))
        except (TypeError, ValueError):
            return None

    @staticmethod
    def normalize_role(role, default='user'):
        normalized_role = str(role or default).strip().lower()
        if normalized_role not in {'user', 'admin', 'superadmin', 'teacher'}:
            return default
        return normalized_role

    @staticmethod
    def normalize_member_card_text(value, default=''):
        return str(value or default).strip()

    @staticmethod
    def normalize_progression_xp(value):
        try:
            return max(0, int(value or 0))
        except (TypeError, ValueError):
            return 0

    @staticmethod
    def normalize_completed_quests(value):
        if not isinstance(value, list):
            return []

        normalized = []
        seen = set()
        for entry in value:
            quest_id = str(entry or '').strip()
            if not quest_id or quest_id in seen:
                continue
            seen.add(quest_id)
            normalized.append(quest_id)
        return normalized

    @staticmethod
    def normalize_progression_metrics(value):
        if not isinstance(value, dict):
            return {}

        normalized = {}
        for key, metric_value in value.items():
            metric_name = str(key or '').strip()
            if not metric_name:
                continue

            if isinstance(metric_value, bool):
                normalized[metric_name] = metric_value
            elif isinstance(metric_value, (int, float)):
                normalized[metric_name] = metric_value
            elif isinstance(metric_value, str):
                normalized[metric_name] = metric_value.strip()
            elif metric_value is None:
                normalized[metric_name] = None
        return normalized

    @staticmethod
    def progression_timestamp():
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def progression_payload(record=None):
        if record is None:
            return {
                'xp': 0,
                'completedQuests': [],
                'metrics': {},
                'lastUpdatedAt': None,
            }

        try:
            completed_quests = json.loads(record['completed_quests'] or '[]')
        except (TypeError, ValueError, json.JSONDecodeError):
            completed_quests = []

        try:
            metrics = json.loads(record['metrics'] or '{}')
        except (TypeError, ValueError, json.JSONDecodeError):
            metrics = {}

        return {
            'xp': PSOAuthService.normalize_progression_xp(record['xp']),
            'completedQuests': PSOAuthService.normalize_completed_quests(completed_quests),
            'metrics': PSOAuthService.normalize_progression_metrics(metrics),
            'lastUpdatedAt': record['last_updated_at'],
        }

    @staticmethod
    def merge_progression(existing_progression, body):
        payload = body.get('progression') if isinstance(body.get('progression'), dict) else body
        if not isinstance(payload, dict):
            payload = {}

        merged = {
            'xp': existing_progression['xp'],
            'completedQuests': list(existing_progression['completedQuests']),
            'metrics': dict(existing_progression['metrics']),
            'lastUpdatedAt': existing_progression['lastUpdatedAt'],
        }

        if 'xp' in payload:
            merged['xp'] = PSOAuthService.normalize_progression_xp(payload.get('xp'))
        if 'completedQuests' in payload:
            merged['completedQuests'] = PSOAuthService.normalize_completed_quests(payload.get('completedQuests'))
        if 'metrics' in payload:
            merged['metrics'] = PSOAuthService.normalize_progression_metrics(payload.get('metrics'))

        merged['lastUpdatedAt'] = PSOAuthService.progression_timestamp()
        return merged

    @staticmethod
    def ensure_database():
        with PSOAuthService.get_connection() as connection:
            connection.execute(
                '''
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    uid TEXT NOT NULL UNIQUE,
                    name TEXT NOT NULL,
                    email TEXT NOT NULL UNIQUE,
                    password_hash TEXT NOT NULL,
                    role TEXT NOT NULL DEFAULT 'User',
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                '''
            )
            columns = {
                column['name'] for column in connection.execute('PRAGMA table_info(users)').fetchall()
            }
            if 'email' not in columns:
                connection.execute('ALTER TABLE users ADD COLUMN email TEXT')
                rows = connection.execute('SELECT id, uid FROM users WHERE email IS NULL OR TRIM(email) = ""').fetchall()
                for row in rows:
                    connection.execute(
                        'UPDATE users SET email = ? WHERE id = ?',
                        (PSOAuthService.default_email_for_uid(row['uid']), row['id'])
                    )

            connection.execute(
                'CREATE UNIQUE INDEX IF NOT EXISTS idx_pso_users_email ON users(email)'
            )
            connection.execute(
                '''
                CREATE TABLE IF NOT EXISTS orchestra_members (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL UNIQUE,
                    uid TEXT NOT NULL UNIQUE,
                    name TEXT NOT NULL,
                    email TEXT NOT NULL,
                    instrument TEXT NOT NULL,
                    section TEXT NOT NULL,
                    practice_time INTEGER NOT NULL DEFAULT 0,
                    approved_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    approved_by INTEGER,
                    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
                    FOREIGN KEY (approved_by) REFERENCES users(id)
                )
                '''
            )
            member_columns = {
                column['name'] for column in connection.execute('PRAGMA table_info(orchestra_members)').fetchall()
            }
            if 'approved_at' not in member_columns:
                connection.execute('ALTER TABLE orchestra_members ADD COLUMN approved_at TEXT')
                connection.execute(
                    'UPDATE orchestra_members SET approved_at = COALESCE(registered_at, CURRENT_TIMESTAMP) '
                    'WHERE approved_at IS NULL OR TRIM(approved_at) = ""'
                )
            if 'approved_by' not in member_columns:
                connection.execute('ALTER TABLE orchestra_members ADD COLUMN approved_by INTEGER')

            connection.execute(
                '''
                CREATE TABLE IF NOT EXISTS member_requests (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    uid TEXT NOT NULL,
                    name TEXT NOT NULL,
                    email TEXT NOT NULL,
                    instrument TEXT NOT NULL,
                    section TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    submitted_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    reviewed_at TEXT,
                    reviewed_by INTEGER,
                    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
                    FOREIGN KEY (reviewed_by) REFERENCES users(id)
                )
                '''
            )
            connection.execute(
                '''
                CREATE TABLE IF NOT EXISTS member_cards (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    owner_uid TEXT NOT NULL,
                    owner_name TEXT NOT NULL,
                    family TEXT NOT NULL,
                    section_id TEXT NOT NULL,
                    instrument_title TEXT NOT NULL DEFAULT '',
                    image_url TEXT NOT NULL DEFAULT '',
                    bio TEXT NOT NULL DEFAULT '',
                    created_by_uid TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (owner_uid) REFERENCES users(uid) ON DELETE CASCADE,
                    FOREIGN KEY (created_by_uid) REFERENCES users(uid) ON DELETE CASCADE
                )
                '''
            )
            card_columns = {
                column['name'] for column in connection.execute('PRAGMA table_info(member_cards)').fetchall()
            }
            if 'instrument_title' not in card_columns:
                connection.execute("ALTER TABLE member_cards ADD COLUMN instrument_title TEXT NOT NULL DEFAULT ''")
            if 'image_url' not in card_columns:
                connection.execute("ALTER TABLE member_cards ADD COLUMN image_url TEXT NOT NULL DEFAULT ''")
            if 'bio' not in card_columns:
                connection.execute("ALTER TABLE member_cards ADD COLUMN bio TEXT NOT NULL DEFAULT ''")
            if 'created_by_uid' not in card_columns:
                connection.execute("ALTER TABLE member_cards ADD COLUMN created_by_uid TEXT NOT NULL DEFAULT ''")
            if 'created_at' not in card_columns:
                connection.execute("ALTER TABLE member_cards ADD COLUMN created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP")
            if 'updated_at' not in card_columns:
                connection.execute("ALTER TABLE member_cards ADD COLUMN updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP")

            connection.execute(
                'CREATE INDEX IF NOT EXISTS idx_member_cards_owner_uid ON member_cards(owner_uid)'
            )
            connection.execute(
                'CREATE INDEX IF NOT EXISTS idx_member_cards_family_section ON member_cards(family, section_id)'
            )
            connection.execute(
                'CREATE UNIQUE INDEX IF NOT EXISTS idx_member_cards_owner_section_unique ON member_cards(owner_uid, family, section_id)'
            )
            connection.execute(
                '''
                CREATE TABLE IF NOT EXISTS user_progression (
                    uid TEXT PRIMARY KEY,
                    xp INTEGER NOT NULL DEFAULT 0,
                    completed_quests TEXT NOT NULL DEFAULT '[]',
                    metrics TEXT NOT NULL DEFAULT '{}',
                    last_updated_at TEXT,
                    FOREIGN KEY (uid) REFERENCES users(uid) ON DELETE CASCADE
                )
                '''
            )
            connection.commit()

        PSOAuthService.seed_default_user()

    @staticmethod
    def seed_default_user():
        default_uid = os.environ.get('PSO_UID') or app.config.get('ADMIN_UID', 'admin')
        default_name = os.environ.get('PSO_NAME') or app.config.get('ADMIN_USER', 'PSO Admin')
        default_email = os.environ.get('PSO_EMAIL') or PSOAuthService.default_email_for_uid(default_uid)
        default_password = os.environ.get('PSO_PASSWORD') or app.config.get('ADMIN_PASSWORD') or app.config.get('DEFAULT_PASSWORD', 'password')
        default_role = PSOAuthService.normalize_role(os.environ.get('PSO_ROLE') or 'admin', default='admin')

        with PSOAuthService.get_connection() as connection:
            existing = connection.execute(
                'SELECT id, email FROM users WHERE uid = ?',
                (default_uid,)
            ).fetchone()
            if existing is None:
                connection.execute(
                    'INSERT INTO users (uid, name, email, password_hash, role) VALUES (?, ?, ?, ?, ?)',
                    (
                        default_uid,
                        default_name,
                        default_email,
                        generate_password_hash(default_password, 'pbkdf2:sha256', salt_length=10),
                        default_role,
                    )
                )
                connection.commit()
            elif existing['email'] is None or str(existing['email']).strip() == '':
                connection.execute(
                    'UPDATE users SET email = ? WHERE id = ?',
                    (default_email, existing['id'])
                )
                connection.commit()

    @staticmethod
    def find_user_by_email(email):
        PSOAuthService.ensure_database()
        with PSOAuthService.get_connection() as connection:
            record = connection.execute(
                'SELECT id, uid, name, email, password_hash, role FROM users WHERE email = ?',
                (email,)
            ).fetchone()

        if record is None:
            return None

        return PSOUser(record)

    @staticmethod
    def find_user_by_uid(uid):
        PSOAuthService.ensure_database()
        with PSOAuthService.get_connection() as connection:
            record = connection.execute(
                'SELECT id, uid, name, email, password_hash, role FROM users WHERE uid = ?',
                (uid,)
            ).fetchone()

        if record is None:
            return None

        return PSOUser(record)

    @staticmethod
    def find_user_by_name(name):
        PSOAuthService.ensure_database()
        normalized_name = str(name or '').strip().lower()
        if not normalized_name:
            return None

        with PSOAuthService.get_connection() as connection:
            record = connection.execute(
                'SELECT id, uid, name, email, password_hash, role FROM users WHERE LOWER(TRIM(name)) = ? LIMIT 1',
                (normalized_name,)
            ).fetchone()

        if record is None:
            return None

        return PSOUser(record)

    @staticmethod
    def find_user_by_identifier(identifier):
        normalized_identifier = str(identifier or '').strip()
        if not normalized_identifier:
            return None

        user = PSOAuthService.find_user_by_uid(normalized_identifier)
        if user is not None:
            return user

        user = PSOAuthService.find_user_by_email(normalized_identifier.lower())
        if user is not None:
            return user

        return PSOAuthService.find_user_by_name(normalized_identifier)

    @staticmethod
    def find_shared_user_by_identifier(identifier):
        normalized_identifier = str(identifier or '').strip()
        if not normalized_identifier:
            return None

        user = User.query.filter_by(_uid=normalized_identifier).first()
        if user is not None:
            return user

        normalized_email = normalized_identifier.lower()
        user = User.query.filter(func.lower(User._email) == normalized_email).first()
        if user is not None:
            return user

        return User.query.filter(func.lower(User._name) == normalized_identifier.lower()).first()

    @staticmethod
    def sync_shared_user(shared_user):
        PSOAuthService.ensure_database()

        normalized_uid = str(shared_user.uid or '').strip()
        normalized_name = str(shared_user.name or normalized_uid).strip() or normalized_uid
        normalized_email = PSOAuthService.shared_user_email(shared_user)
        normalized_role = PSOAuthService.normalize_role(getattr(shared_user, 'role', 'user'))

        existing_email_user = PSOAuthService.find_user_by_email(normalized_email)
        if existing_email_user is not None and existing_email_user.uid != normalized_uid:
            normalized_email = PSOAuthService.default_email_for_uid(normalized_uid)

        with PSOAuthService.get_connection() as connection:
            existing = connection.execute(
                'SELECT id FROM users WHERE uid = ?',
                (normalized_uid,)
            ).fetchone()

            if existing is None:
                connection.execute(
                    'INSERT INTO users (uid, name, email, password_hash, role) VALUES (?, ?, ?, ?, ?)',
                    (
                        normalized_uid,
                        normalized_name,
                        normalized_email,
                        shared_user._password,
                        normalized_role,
                    )
                )
            else:
                connection.execute(
                    'UPDATE users SET name = ?, email = ?, password_hash = ?, role = ? WHERE uid = ?',
                    (
                        normalized_name,
                        normalized_email,
                        shared_user._password,
                        normalized_role,
                        normalized_uid,
                    )
                )
            connection.commit()

        return PSOAuthService.find_user_by_uid(normalized_uid)

    @staticmethod
    def list_users():
        PSOAuthService.ensure_database()
        with PSOAuthService.get_connection() as connection:
            records = connection.execute(
                '''
                SELECT id, uid, name, email, role, created_at
                FROM users
                ORDER BY name COLLATE NOCASE ASC, uid COLLATE NOCASE ASC
                '''
            ).fetchall()

        return [dict(record) for record in records]

    @staticmethod
    def list_admins():
        PSOAuthService.ensure_database()
        with PSOAuthService.get_connection() as connection:
            records = connection.execute(
                '''
                SELECT id, uid, name, email, role, created_at
                FROM users
                WHERE LOWER(TRIM(role)) IN ('admin', 'superadmin')
                ORDER BY name COLLATE NOCASE ASC, uid COLLATE NOCASE ASC
                '''
            ).fetchall()

        return [dict(record) for record in records]

    @staticmethod
    def update_pso_user(uid, body):
        user = PSOAuthService.find_user_by_uid(uid)
        if user is None:
            return None, {'message': 'PSO user not found'}, 404

        updates = {}

        if 'name' in body:
            name = str(body.get('name') or '').strip()
            if len(name) < 2:
                return None, {'message': 'name must be at least 2 characters'}, 400
            updates['name'] = name

        if 'email' in body:
            email = str(body.get('email') or '').strip().lower()
            if len(email) < 3 or '@' not in email:
                return None, {'message': 'email is invalid'}, 400
            existing_user = PSOAuthService.find_user_by_email(email)
            if existing_user is not None and existing_user.uid != uid:
                return None, {'message': 'email already exists'}, 409
            updates['email'] = email

        if 'role' in body:
            updates['role'] = PSOAuthService.normalize_role(body.get('role'), default=user.role)

        if not updates:
            return user.read(), None, 200

        assignments = []
        parameters = []
        for key in ('name', 'email', 'role'):
            if key in updates:
                assignments.append(f'{key} = ?')
                parameters.append(updates[key])

        parameters.append(uid)

        with PSOAuthService.get_connection() as connection:
            connection.execute(
                f"UPDATE users SET {', '.join(assignments)} WHERE uid = ?",
                tuple(parameters)
            )
            connection.commit()

        updated_user = PSOAuthService.find_user_by_uid(uid)
        return updated_user.read(), None, 200

    @staticmethod
    def delete_pso_user(uid):
        user = PSOAuthService.find_user_by_uid(uid)
        if user is None:
            return False, {'message': 'PSO user not found'}, 404

        with PSOAuthService.get_connection() as connection:
            connection.execute('DELETE FROM users WHERE uid = ?', (uid,))
            connection.commit()

        return True, None, 200

    @staticmethod
    def create_user(name, uid, email, password, role='user'):
        PSOAuthService.ensure_database()

        if name is None or len(str(name).strip()) < 2:
            return None, {'message': 'Name is missing or less than 2 characters'}, 400

        if uid is None or len(str(uid).strip()) < 2:
            return None, {'message': 'User ID is missing or less than 2 characters'}, 400

        if email is None or len(str(email).strip()) < 3 or '@' not in str(email):
            return None, {'message': 'Email is missing or invalid'}, 400

        if password is None or len(str(password)) < 8:
            return None, {'message': 'Password must be at least 8 characters'}, 400

        normalized_name = str(name).strip()
        normalized_uid = str(uid).strip()
        normalized_email = str(email).strip().lower()
        normalized_role = PSOAuthService.normalize_role(role)

        existing_user = PSOAuthService.find_user_by_uid(normalized_uid)
        if existing_user is not None:
            return None, {'message': f'User {normalized_uid} already exists'}, 409

        existing_email = PSOAuthService.find_user_by_email(normalized_email)
        if existing_email is not None:
            return None, {'message': f'Email {normalized_email} already exists'}, 409

        with PSOAuthService.get_connection() as connection:
            cursor = connection.execute(
                'INSERT INTO users (uid, name, email, password_hash, role) VALUES (?, ?, ?, ?, ?)',
                (
                    normalized_uid,
                    normalized_name,
                    normalized_email,
                    generate_password_hash(password, 'pbkdf2:sha256', salt_length=10),
                    normalized_role,
                )
            )
            connection.commit()

            record = connection.execute(
                'SELECT id, uid, name, email, password_hash, role FROM users WHERE id = ?',
                (cursor.lastrowid,)
            ).fetchone()

        return PSOUser(record), None, 201

    @staticmethod
    def update_user_email(uid, email):
        normalized_email = str(email).strip().lower()
        existing_email_user = PSOAuthService.find_user_by_email(normalized_email)
        if existing_email_user is not None and existing_email_user.uid != uid:
            return False, {'message': f'Email {normalized_email} already exists'}, 409

        with PSOAuthService.get_connection() as connection:
            cursor = connection.execute(
                'UPDATE users SET email = ? WHERE uid = ?',
                (normalized_email, uid)
            )
            connection.commit()

        if cursor.rowcount == 0:
            return False, {'message': 'User not found'}, 404

        return True, None, None

    @staticmethod
    def authenticate(identifier, password):
        PSOAuthService.ensure_database()

        normalized_identifier = str(identifier or '').strip()
        current_app.logger.info('PSO auth attempt identifier=%s', normalized_identifier)

        if identifier is None or len(normalized_identifier) == 0:
            current_app.logger.warning('PSO auth failed: missing identifier')
            return None, {'message': 'User ID is missing'}, 401

        if password is None or len(str(password)) == 0:
            current_app.logger.warning('PSO auth failed for identifier=%s: missing password', normalized_identifier)
            return None, {'message': 'Password is missing'}, 401

        user = PSOAuthService.find_user_by_identifier(normalized_identifier)
        current_app.logger.info('PSO auth local user found=%s identifier=%s', user is not None, normalized_identifier)
        if user is not None and check_password_hash(user.password_hash, password):
            current_app.logger.info('PSO auth success via local PSO user identifier=%s uid=%s', normalized_identifier, user.uid)
            return user, None, None

        shared_user = PSOAuthService.find_shared_user_by_identifier(normalized_identifier)
        current_app.logger.info('PSO auth shared user found=%s identifier=%s', shared_user is not None, normalized_identifier)
        if shared_user is not None and shared_user.is_password(password):
            synced_user = PSOAuthService.sync_shared_user(shared_user)
            if synced_user is not None:
                current_app.logger.info(
                    'PSO auth success via shared user sync identifier=%s uid=%s',
                    normalized_identifier,
                    synced_user.uid,
                )
                return synced_user, None, None

            current_app.logger.warning(
                'PSO auth shared user sync failed identifier=%s shared_uid=%s',
                normalized_identifier,
                shared_user.uid,
            )

        if user is not None:
            current_app.logger.warning('PSO auth failed: local password mismatch identifier=%s uid=%s', normalized_identifier, user.uid)
        elif shared_user is not None:
            current_app.logger.warning('PSO auth failed: shared password mismatch identifier=%s uid=%s', normalized_identifier, shared_user.uid)
        else:
            current_app.logger.warning('PSO auth failed: no local or shared user for identifier=%s', normalized_identifier)

        return None, {
            'message': 'Invalid Poway Orchestra user ID or password. If you have not created a Poway account yet, sign up first.'
        }, 401

    @staticmethod
    def authenticate_request():
        PSOAuthService.ensure_database()

        token = request.cookies.get(current_app.config['JWT_TOKEN_NAME'])
        if not token:
            return None, {
                'message': 'Authentication required. No token found.',
                'data': None,
                'error': 'Unauthorized'
            }, 401

        try:
            data = jwt.decode(token, current_app.config['SECRET_KEY'], algorithms=['HS256'])
        except jwt.ExpiredSignatureError:
            return None, {
                'message': 'Token has expired!',
                'data': None,
                'error': 'Unauthorized'
            }, 401
        except jwt.InvalidTokenError:
            return None, {
                'message': 'Invalid token!',
                'data': None,
                'error': 'Unauthorized'
            }, 401

        user = PSOAuthService.find_user_by_uid(data.get('_uid'))
        if user is None:
            current_app.logger.info('PSO token fallback to shared user for uid=%s', data.get('_uid'))
            shared_user = User.query.filter_by(_uid=data.get('_uid')).first()
            if shared_user is None:
                current_app.logger.warning('PSO token auth failed: uid not found locally or shared uid=%s', data.get('_uid'))
                return None, {
                    'message': 'Invalid Authentication token!',
                    'data': None,
                    'error': 'Unauthorized'
                }, 401

            user = PSOAuthService.sync_shared_user(shared_user)
            if user is None:
                current_app.logger.warning('PSO token auth failed: shared user sync failed uid=%s', data.get('_uid'))
                return None, {
                    'message': 'Invalid Authentication token!',
                    'data': None,
                    'error': 'Unauthorized'
                }, 401

        return user, None, None

    @staticmethod
    def current_user_payload(user):
        payload = user.read()
        payload['is_member'] = PSOAuthService.is_member(user.uid)
        payload['member_request_status'] = PSOAuthService.get_member_request_status(user.uid)
        payload['is_admin'] = user.is_admin()
        payload['is_teacher'] = user.is_teacher()
        payload['can_access_admin_dashboard'] = user.is_admin()
        return payload

    @staticmethod
    def login_payload(user):
        return {
            'message': f'Authentication for {user.uid} successful',
            'user': {
                'uid': user.uid,
                'name': user.name,
                'email': user.email,
                'role': user.role,
                'is_member': PSOAuthService.is_member(user.uid),
                'member_request_status': PSOAuthService.get_member_request_status(user.uid),
                'can_access_admin_dashboard': user.is_admin()
            }
        }

    @staticmethod
    def signup_payload(user):
        return {
            'message': f'User {user.uid} created successfully',
            'user': {
                'id': user.id,
                'uid': user.uid,
                'name': user.name,
                'email': user.email,
                'role': user.role,
                'is_member': False,
                'member_request_status': 'none',
                'can_access_admin_dashboard': user.is_admin()
            }
        }

    @staticmethod
    def logout_payload(user):
        return {
            'message': 'Logout successful',
            'user': {
                'uid': user.uid,
                'name': user.name
            }
        }

    @staticmethod
    def issue_token(user):
        return jwt.encode(
            {'_uid': user.uid},
            current_app.config['SECRET_KEY'],
            algorithm='HS256'
        )

    @staticmethod
    def cookie_options(expired=False):
        is_production = os.environ.get('IS_PRODUCTION', 'false').lower() == 'true'
        options = {
            'max_age': 0 if expired else 43200,
            'secure': is_production,
            'httponly': True,
            'path': '/',
            'samesite': 'None' if is_production else 'Lax'
        }

        if is_production:
            options['domain'] = '.opencodingsociety.com'

        return options

    @staticmethod
    def attach_login_cookie(response, user):
        response.set_cookie(
            current_app.config['JWT_TOKEN_NAME'],
            PSOAuthService.issue_token(user),
            **PSOAuthService.cookie_options()
        )
        return response

    @staticmethod
    def clear_login_cookie(response):
        response.set_cookie(
            current_app.config['JWT_TOKEN_NAME'],
            '',
            **PSOAuthService.cookie_options(expired=True)
        )
        return response

    @staticmethod
    def get_member_by_uid(uid):
        PSOAuthService.ensure_database()
        with PSOAuthService.get_connection() as connection:
            record = connection.execute(
                '''
                SELECT om.id, om.user_id, om.uid, om.name, om.email, om.instrument,
                       om.section, om.practice_time, om.approved_at, om.approved_by, u.role
                FROM orchestra_members om
                JOIN users u ON u.id = om.user_id
                WHERE om.uid = ?
                ''',
                (uid,)
            ).fetchone()

        if record is None:
            return None

        return dict(record)

    @staticmethod
    def is_member(uid):
        return PSOAuthService.get_member_by_uid(uid) is not None

    @staticmethod
    def get_member_request_by_id(request_id):
        PSOAuthService.ensure_database()
        with PSOAuthService.get_connection() as connection:
            record = connection.execute(
                '''
                SELECT mr.id, mr.user_id, mr.uid, mr.name, mr.email, mr.instrument, mr.section,
                       mr.status, mr.submitted_at, mr.reviewed_at, mr.reviewed_by,
                       reviewer.uid AS reviewed_by_uid, reviewer.name AS reviewed_by_name
                FROM member_requests mr
                LEFT JOIN users reviewer ON reviewer.id = mr.reviewed_by
                WHERE mr.id = ?
                ''',
                (request_id,)
            ).fetchone()

        if record is None:
            return None

        return dict(record)

    @staticmethod
    def get_latest_member_request(uid):
        PSOAuthService.ensure_database()
        with PSOAuthService.get_connection() as connection:
            record = connection.execute(
                '''
                SELECT mr.id, mr.user_id, mr.uid, mr.name, mr.email, mr.instrument, mr.section,
                       mr.status, mr.submitted_at, mr.reviewed_at, mr.reviewed_by,
                       reviewer.uid AS reviewed_by_uid, reviewer.name AS reviewed_by_name
                FROM member_requests mr
                LEFT JOIN users reviewer ON reviewer.id = mr.reviewed_by
                WHERE mr.uid = ?
                ORDER BY mr.id DESC
                LIMIT 1
                ''',
                (uid,)
            ).fetchone()

        if record is None:
            return None

        return dict(record)

    @staticmethod
    def get_member_request_status(uid):
        if PSOAuthService.is_member(uid):
            return 'approved'
        latest_request = PSOAuthService.get_latest_member_request(uid)
        if latest_request is not None:
            return latest_request['status']
        return 'none'

    @staticmethod
    def member_profile_payload(uid):
        member = PSOAuthService.get_member_by_uid(uid)
        if member is None:
            return {
                'instrument': '',
                'section': '',
                'practice_time': 0,
            }

        return {
            'instrument': str(member.get('instrument') or '').strip(),
            'section': str(member.get('section') or '').strip(),
            'practice_time': PSOAuthService.normalize_practice_time(member.get('practice_time')) or 0,
        }

    @staticmethod
    def get_progression(uid):
        PSOAuthService.ensure_database()
        with PSOAuthService.get_connection() as connection:
            record = connection.execute(
                '''
                SELECT uid, xp, completed_quests, metrics, last_updated_at
                FROM user_progression
                WHERE uid = ?
                ''',
                (uid,)
            ).fetchone()

        return PSOAuthService.progression_payload(record)

    @staticmethod
    def save_progression(uid, body):
        PSOAuthService.ensure_database()

        existing_progression = PSOAuthService.get_progression(uid)
        updated_progression = PSOAuthService.merge_progression(existing_progression, body or {})

        with PSOAuthService.get_connection() as connection:
            connection.execute(
                '''
                INSERT INTO user_progression (uid, xp, completed_quests, metrics, last_updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(uid) DO UPDATE SET
                    xp = excluded.xp,
                    completed_quests = excluded.completed_quests,
                    metrics = excluded.metrics,
                    last_updated_at = excluded.last_updated_at
                ''',
                (
                    uid,
                    updated_progression['xp'],
                    json.dumps(updated_progression['completedQuests']),
                    json.dumps(updated_progression['metrics']),
                    updated_progression['lastUpdatedAt'],
                )
            )
            connection.commit()

        return updated_progression

    @staticmethod
    def member_card_payload(record):
        if record is None:
            return None

        return {
            'id': record['id'],
            'owner_uid': record['owner_uid'],
            'owner_name': record['owner_name'],
            'family': record['family'],
            'section_id': record['section_id'],
            'instrument_title': record['instrument_title'],
            'image_url': record['image_url'],
            'bio': record['bio'],
            'created_by_uid': record['created_by_uid'],
            'created_at': record['created_at'],
            'updated_at': record['updated_at'],
        }

    @staticmethod
    def get_member_card_by_id(card_id):
        PSOAuthService.ensure_database()
        with PSOAuthService.get_connection() as connection:
            record = connection.execute(
                '''
                SELECT id, owner_uid, owner_name, family, section_id, instrument_title,
                       image_url, bio, created_by_uid, created_at, updated_at
                FROM member_cards
                WHERE id = ?
                ''',
                (card_id,)
            ).fetchone()

        return PSOAuthService.member_card_payload(record)

    @staticmethod
    def list_member_cards(current_user=None, family=None, section_id=None):
        PSOAuthService.ensure_database()
        query = (
            'SELECT id, owner_uid, owner_name, family, section_id, instrument_title, '
            'image_url, bio, created_by_uid, created_at, updated_at '
            'FROM member_cards WHERE 1 = 1 '
        )
        parameters = []

        normalized_family = PSOAuthService.normalize_member_card_text(family)
        normalized_section = PSOAuthService.normalize_member_card_text(section_id)

        if normalized_family:
            query += 'AND family = ? '
            parameters.append(normalized_family)
        if normalized_section:
            query += 'AND section_id = ? '
            parameters.append(normalized_section)

        query += 'ORDER BY family COLLATE NOCASE ASC, section_id COLLATE NOCASE ASC, owner_name COLLATE NOCASE ASC, id ASC'

        with PSOAuthService.get_connection() as connection:
            records = connection.execute(query, tuple(parameters)).fetchall()

        return [PSOAuthService.member_card_payload(record) for record in records]

    @staticmethod
    def validate_member_card_owner(owner_uid):
        normalized_owner_uid = PSOAuthService.normalize_member_card_text(owner_uid)
        if len(normalized_owner_uid) < 2:
            return None, {'message': 'owner_uid is required'}, 400

        owner_user = PSOAuthService.find_user_by_uid(normalized_owner_uid)
        if owner_user is None:
            return None, {'message': 'Owner user not found'}, 404

        if not PSOAuthService.is_member(normalized_owner_uid):
            return None, {'message': 'Owner must be a registered member'}, 409

        return owner_user, None, None

    @staticmethod
    def find_member_card(owner_uid, family, section_id, exclude_id=None):
        PSOAuthService.ensure_database()
        query = (
            'SELECT id, owner_uid, owner_name, family, section_id, instrument_title, '
            'image_url, bio, created_by_uid, created_at, updated_at '
            'FROM member_cards WHERE owner_uid = ? AND family = ? AND section_id = ?'
        )
        parameters = [owner_uid, family, section_id]

        if exclude_id is not None:
            query += ' AND id != ?'
            parameters.append(exclude_id)

        with PSOAuthService.get_connection() as connection:
            record = connection.execute(query, tuple(parameters)).fetchone()

        return PSOAuthService.member_card_payload(record)

    @staticmethod
    def create_member_card(current_user, body):
        owner_uid = body.get('owner_uid') or body.get('ownerUid')
        owner_user, error_body, status_code = PSOAuthService.validate_member_card_owner(owner_uid)
        if error_body:
            return None, error_body, status_code

        family = PSOAuthService.normalize_member_card_text(body.get('family'))
        section_id = PSOAuthService.normalize_member_card_text(body.get('section_id') or body.get('sectionId'))
        owner_name = PSOAuthService.normalize_member_card_text(body.get('owner_name') or body.get('ownerName'), owner_user.name)
        instrument_title = PSOAuthService.normalize_member_card_text(body.get('instrument_title') or body.get('instrumentTitle'))
        image_url = PSOAuthService.normalize_member_card_text(body.get('image_url') or body.get('imageUrl'))
        bio = PSOAuthService.normalize_member_card_text(body.get('bio'))

        if not family:
            return None, {'message': 'family is required'}, 400
        if not section_id:
            return None, {'message': 'section_id is required'}, 400
        if not owner_name:
            return None, {'message': 'owner_name is required'}, 400
        if not instrument_title:
            return None, {'message': 'instrument_title is required'}, 400
        if PSOAuthService.find_member_card(owner_user.uid, family, section_id) is not None:
            return None, {'message': 'Member card already exists for this owner and section'}, 409

        with PSOAuthService.get_connection() as connection:
            cursor = connection.execute(
                '''
                INSERT INTO member_cards (
                    owner_uid, owner_name, family, section_id, instrument_title,
                    image_url, bio, created_by_uid
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    owner_user.uid,
                    owner_name,
                    family,
                    section_id,
                    instrument_title,
                    image_url,
                    bio,
                    current_user.uid,
                )
            )
            connection.commit()

        return PSOAuthService.get_member_card_by_id(cursor.lastrowid), None, 201

    @staticmethod
    def update_member_card(card_id, current_user, body):
        card = PSOAuthService.get_member_card_by_id(card_id)
        if card is None:
            return None, {'message': 'Member card not found'}, 404

        is_admin = current_user.is_admin()
        if not is_admin and card['owner_uid'] != current_user.uid:
            return None, {'message': 'Forbidden'}, 403

        updates = {}

        def update_if_present(target_key, *source_keys):
            for source_key in source_keys:
                if source_key in body:
                    updates[target_key] = body.get(source_key)
                    return

        update_if_present('owner_name', 'owner_name', 'ownerName')
        update_if_present('instrument_title', 'instrument_title', 'instrumentTitle')
        update_if_present('image_url', 'image_url', 'imageUrl')
        update_if_present('bio', 'bio')

        if is_admin:
            update_if_present('owner_uid', 'owner_uid', 'ownerUid')
            update_if_present('family', 'family')
            update_if_present('section_id', 'section_id', 'sectionId')

        if not updates:
            return card, None, 200

        normalized_updates = {}
        for key, value in updates.items():
            normalized_updates[key] = PSOAuthService.normalize_member_card_text(value)

        if 'owner_uid' in normalized_updates:
            owner_user, error_body, status_code = PSOAuthService.validate_member_card_owner(normalized_updates['owner_uid'])
            if error_body:
                return None, error_body, status_code
            normalized_updates['owner_uid'] = owner_user.uid
            if not normalized_updates.get('owner_name'):
                normalized_updates['owner_name'] = owner_user.name

        if 'owner_name' in normalized_updates and not normalized_updates['owner_name']:
            return None, {'message': 'owner_name is required'}, 400
        if is_admin and 'family' in normalized_updates and not normalized_updates['family']:
            return None, {'message': 'family is required'}, 400
        if is_admin and 'section_id' in normalized_updates and not normalized_updates['section_id']:
            return None, {'message': 'section_id is required'}, 400
        if 'instrument_title' in normalized_updates and not normalized_updates['instrument_title']:
            return None, {'message': 'instrument_title is required'}, 400

        next_owner_uid = normalized_updates.get('owner_uid', card['owner_uid'])
        next_family = normalized_updates.get('family', card['family'])
        next_section_id = normalized_updates.get('section_id', card['section_id'])
        if PSOAuthService.find_member_card(next_owner_uid, next_family, next_section_id, exclude_id=card_id) is not None:
            return None, {'message': 'Member card already exists for this owner and section'}, 409

        assignments = []
        parameters = []
        for key in ('owner_uid', 'owner_name', 'family', 'section_id', 'instrument_title', 'image_url', 'bio'):
            if key in normalized_updates:
                assignments.append(f'{key} = ?')
                parameters.append(normalized_updates[key])

        assignments.append('updated_at = CURRENT_TIMESTAMP')
        parameters.append(card_id)

        with PSOAuthService.get_connection() as connection:
            connection.execute(
                f"UPDATE member_cards SET {', '.join(assignments)} WHERE id = ?",
                tuple(parameters)
            )
            connection.commit()

        return PSOAuthService.get_member_card_by_id(card_id), None, 200

    @staticmethod
    def delete_member_card(card_id, current_user):
        card = PSOAuthService.get_member_card_by_id(card_id)
        if card is None:
            return False, {'message': 'Member card not found'}, 404

        if not current_user.is_admin() and card['owner_uid'] != current_user.uid:
            return False, {'message': 'Forbidden'}, 403

        with PSOAuthService.get_connection() as connection:
            connection.execute('DELETE FROM member_cards WHERE id = ?', (card_id,))
            connection.commit()

        return True, None, 200

    @staticmethod
    def submit_member_request(uid, name, email, instrument, section):
        if email is None or len(str(email).strip()) < 3 or '@' not in str(email):
            return None, {'message': 'Email is missing or invalid'}, 400

        if name is None or len(str(name).strip()) < 2:
            return None, {'message': 'Name is missing or less than 2 characters'}, 400

        if instrument is None or len(str(instrument).strip()) == 0:
            return None, {'message': 'Instrument is required'}, 400

        if section is None or len(str(section).strip()) == 0:
            return None, {'message': 'Section is required'}, 400

        user = PSOAuthService.find_user_by_uid(uid)
        if user is None:
            return None, {'message': 'User not found'}, 404

        if PSOAuthService.is_member(uid):
            return None, {'message': 'User is already registered as a member'}, 409

        latest_request = PSOAuthService.get_latest_member_request(uid)
        if latest_request is not None and latest_request['status'] == 'pending':
            return None, {'message': 'Member request is already pending'}, 409

        normalized_email = str(email).strip().lower()
        updated, error_body, status_code = PSOAuthService.update_user_email(uid, normalized_email)
        if not updated:
            return None, error_body, status_code

        user = PSOAuthService.find_user_by_uid(uid)

        with PSOAuthService.get_connection() as connection:
            cursor = connection.execute(
                '''
                INSERT INTO member_requests (user_id, uid, name, email, instrument, section, status)
                VALUES (?, ?, ?, ?, ?, ?, 'pending')
                ''',
                (
                    user.id,
                    user.uid,
                    str(name).strip(),
                    user.email,
                    str(instrument).strip(),
                    str(section).strip(),
                )
            )
            connection.commit()

        return PSOAuthService.get_member_request_by_id(cursor.lastrowid), None, 201

    @staticmethod
    def register_member(uid, email, instrument, section, practice_time):
        user = PSOAuthService.find_user_by_uid(uid)
        if user is None:
            return None, {'message': 'User not found'}, 404

        request_row, error_body, status_code = PSOAuthService.submit_member_request(
            uid,
            user.name,
            email,
            instrument,
            section
        )
        if error_body:
            return None, error_body, status_code

        return request_row, None, status_code

    @staticmethod
    def list_member_requests(status=None):
        PSOAuthService.ensure_database()
        query = (
            'SELECT mr.id, mr.user_id, mr.uid, mr.name, mr.email, mr.instrument, mr.section, '
            'mr.status, mr.submitted_at, mr.reviewed_at, mr.reviewed_by, '
            'reviewer.uid AS reviewed_by_uid, reviewer.name AS reviewed_by_name '
            'FROM member_requests mr '
            'LEFT JOIN users reviewer ON reviewer.id = mr.reviewed_by '
        )
        parameters = []
        if status and status != 'all':
            query += 'WHERE mr.status = ? '
            parameters.append(status)
        query += 'ORDER BY mr.id DESC'

        with PSOAuthService.get_connection() as connection:
            records = connection.execute(query, tuple(parameters)).fetchall()

        return [dict(record) for record in records]

    @staticmethod
    def approve_member_request(request_id, admin_user):
        request_row = PSOAuthService.get_member_request_by_id(request_id)
        if request_row is None:
            return None, {'message': 'Request not found'}, 404

        if request_row['status'] != 'pending':
            return None, {'message': 'Request already reviewed'}, 409

        if PSOAuthService.is_member(request_row['uid']):
            return None, {'message': 'User is already a member'}, 409

        with PSOAuthService.get_connection() as connection:
            connection.execute(
                '''
                INSERT INTO orchestra_members (user_id, uid, name, email, instrument, section, practice_time, approved_by)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    request_row['user_id'],
                    request_row['uid'],
                    request_row['name'],
                    request_row['email'],
                    request_row['instrument'],
                    request_row['section'],
                    0,
                    admin_user.id,
                )
            )
            connection.execute(
                '''
                UPDATE member_requests
                SET status = 'approved', reviewed_at = CURRENT_TIMESTAMP, reviewed_by = ?
                WHERE id = ?
                ''',
                (admin_user.id, request_id)
            )
            connection.commit()

        return PSOAuthService.get_member_request_by_id(request_id), None, 200

    @staticmethod
    def reject_member_request(request_id, admin_user):
        request_row = PSOAuthService.get_member_request_by_id(request_id)
        if request_row is None:
            return None, {'message': 'Request not found'}, 404

        if request_row['status'] != 'pending':
            return None, {'message': 'Request already reviewed'}, 409

        with PSOAuthService.get_connection() as connection:
            connection.execute(
                '''
                UPDATE member_requests
                SET status = 'rejected', reviewed_at = CURRENT_TIMESTAMP, reviewed_by = ?
                WHERE id = ?
                ''',
                (admin_user.id, request_id)
            )
            connection.commit()

        return PSOAuthService.get_member_request_by_id(request_id), None, 200

    @staticmethod
    def list_members():
        PSOAuthService.ensure_database()
        with PSOAuthService.get_connection() as connection:
            records = connection.execute(
                '''
                SELECT om.id, om.user_id, om.uid, om.name, om.email, om.instrument,
                       om.section, om.practice_time, om.approved_at, om.approved_by,
                       approver.uid AS approved_by_uid, approver.name AS approved_by_name,
                       u.role
                FROM orchestra_members om
                JOIN users u ON u.id = om.user_id
                LEFT JOIN users approver ON approver.id = om.approved_by
                ORDER BY om.id DESC
                '''
            ).fetchall()

        return [dict(record) for record in records]

    @staticmethod
    def update_member(uid, body):
        member = PSOAuthService.get_member_by_uid(uid)
        if member is None:
            return None, {'message': 'PSO member not found'}, 404

        updates = {}

        if 'name' in body:
            name = str(body.get('name') or '').strip()
            if len(name) < 2:
                return None, {'message': 'name must be at least 2 characters'}, 400
            updates['name'] = name

        if 'email' in body:
            email = str(body.get('email') or '').strip().lower()
            if len(email) < 3 or '@' not in email:
                return None, {'message': 'email is invalid'}, 400
            updates['email'] = email

        if 'instrument' in body:
            instrument = str(body.get('instrument') or '').strip()
            if not instrument:
                return None, {'message': 'instrument is required'}, 400
            updates['instrument'] = instrument

        if 'section' in body:
            section = str(body.get('section') or '').strip()
            if not section:
                return None, {'message': 'section is required'}, 400
            updates['section'] = section

        if 'practice_time' in body:
            practice_time = PSOAuthService.normalize_practice_time(body.get('practice_time'))
            if practice_time is None:
                return None, {'message': 'practice_time must be a non-negative integer'}, 400
            updates['practice_time'] = practice_time

        if not updates:
            return member, None, 200

        assignments = []
        parameters = []
        for key in ('name', 'email', 'instrument', 'section', 'practice_time'):
            if key in updates:
                assignments.append(f'{key} = ?')
                parameters.append(updates[key])

        parameters.append(uid)

        with PSOAuthService.get_connection() as connection:
            connection.execute(
                f"UPDATE orchestra_members SET {', '.join(assignments)} WHERE uid = ?",
                tuple(parameters)
            )
            if 'name' in updates or 'email' in updates:
                user_assignments = []
                user_parameters = []
                if 'name' in updates:
                    user_assignments.append('name = ?')
                    user_parameters.append(updates['name'])
                if 'email' in updates:
                    user_assignments.append('email = ?')
                    user_parameters.append(updates['email'])
                user_parameters.append(uid)
                connection.execute(
                    f"UPDATE users SET {', '.join(user_assignments)} WHERE uid = ?",
                    tuple(user_parameters)
                )
            connection.commit()

        return PSOAuthService.get_member_by_uid(uid), None, 200

    @staticmethod
    def delete_member(uid):
        member = PSOAuthService.get_member_by_uid(uid)
        if member is None:
            return False, {'message': 'PSO member not found'}, 404

        with PSOAuthService.get_connection() as connection:
            connection.execute('DELETE FROM orchestra_members WHERE uid = ?', (uid,))
            connection.commit()

        return True, None, 200

    @staticmethod
    def update_member_profile(uid, instrument, section, practice_time):
        if instrument is None or len(str(instrument).strip()) == 0:
            return False, {'message': 'Instrument is required'}, 400

        if section is None or len(str(section).strip()) == 0:
            return False, {'message': 'Section is required'}, 400

        normalized_practice_time = PSOAuthService.normalize_practice_time(practice_time)
        if normalized_practice_time is None:
            return False, {'message': 'Practice time must be a non-negative integer'}, 400

        with PSOAuthService.get_connection() as connection:
            cursor = connection.execute(
                '''
                UPDATE orchestra_members
                SET instrument = ?, section = ?, practice_time = ?
                WHERE uid = ?
                ''',
                (
                    str(instrument).strip(),
                    str(section).strip(),
                    normalized_practice_time,
                    uid,
                )
            )
            connection.commit()

        if cursor.rowcount == 0:
            return False, {'message': 'Member profile not found'}, 404

        return True, None, None


PSOAuthService.ensure_database()