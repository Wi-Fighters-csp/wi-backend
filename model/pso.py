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
    PROGRESSION_SCHEMA_VERSION = 2

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
    def progression_default_metrics():
        return {
            'uniquePages': [],
            'researchPages': [],
            'instrumentSearches': 0,
            'uniqueRecordingKeys': [],
            'recordingPlays': 0,
            'uniqueMusicianCards': [],
            'viewedConcertCalendar': 0,
            'viewedTicketsSection': 0,
            'gamesPlayed': 0,
            'uniqueGames': [],
        }

    @staticmethod
    def progression_default_state(last_updated_at=None):
        return {
            'schemaVersion': PSOAuthService.PROGRESSION_SCHEMA_VERSION,
            'xp': 0,
            'completedQuests': [],
            'metrics': PSOAuthService.progression_default_metrics(),
            'lastUpdatedAt': last_updated_at or PSOAuthService.progression_timestamp(),
        }

    @staticmethod
    def normalize_progression_metric_string_list(value, field_name):
        if not isinstance(value, list):
            return None, {'message': f'metrics.{field_name} must be an array of strings'}, 400

        normalized = []
        seen = set()
        for entry in value:
            if not isinstance(entry, str):
                return None, {'message': f'metrics.{field_name} must contain only strings'}, 400
            cleaned = entry.strip()
            if not cleaned or cleaned in seen:
                continue
            seen.add(cleaned)
            normalized.append(cleaned)

        return normalized, None, None

    @staticmethod
    def normalize_progression_metric_counter(value, field_name, clamp_to_binary=False):
        if isinstance(value, bool):
            normalized = int(value)
        elif isinstance(value, (int, float)):
            if int(value) != value:
                return None, {'message': f'metrics.{field_name} must be an integer value'}, 400
            normalized = int(value)
        else:
            return None, {'message': f'metrics.{field_name} must be a number'}, 400

        if normalized < 0:
            return None, {'message': f'metrics.{field_name} must be greater than or equal to 0'}, 400

        if clamp_to_binary:
            normalized = 1 if normalized > 0 else 0

        return normalized, None, None

    @staticmethod
    def normalize_progression_payload_v2(body):
        payload = body.get('progression') if isinstance(body.get('progression'), dict) else body
        if not isinstance(payload, dict):
            return None, {'message': 'Progression payload is required'}, 400

        schema_version = payload.get('schemaVersion')
        if schema_version != PSOAuthService.PROGRESSION_SCHEMA_VERSION:
            return None, {'message': f'schemaVersion must be {PSOAuthService.PROGRESSION_SCHEMA_VERSION}'}, 400

        xp_value = payload.get('xp')
        if isinstance(xp_value, bool) or not isinstance(xp_value, (int, float)):
            return None, {'message': 'xp must be a number greater than or equal to 0'}, 400
        if int(xp_value) != xp_value:
            return None, {'message': 'xp must be an integer value'}, 400
        normalized_xp = int(xp_value)
        if normalized_xp < 0:
            return None, {'message': 'xp must be greater than or equal to 0'}, 400

        completed_quests = payload.get('completedQuests')
        if not isinstance(completed_quests, list):
            return None, {'message': 'completedQuests must be an array of strings'}, 400
        normalized_completed_quests = []
        completed_seen = set()
        for entry in completed_quests:
            if not isinstance(entry, str):
                return None, {'message': 'completedQuests must contain only strings'}, 400
            quest_id = entry.strip()
            if not quest_id or quest_id in completed_seen:
                continue
            completed_seen.add(quest_id)
            normalized_completed_quests.append(quest_id)

        metrics = payload.get('metrics')
        if not isinstance(metrics, dict):
            return None, {'message': 'metrics must be an object'}, 400

        normalized_metrics = PSOAuthService.progression_default_metrics()

        normalized_metrics['uniquePages'], error_body, status_code = PSOAuthService.normalize_progression_metric_string_list(
            metrics.get('uniquePages', []),
            'uniquePages'
        )
        if error_body:
            return None, error_body, status_code

        normalized_metrics['researchPages'], error_body, status_code = PSOAuthService.normalize_progression_metric_string_list(
            metrics.get('researchPages', []),
            'researchPages'
        )
        if error_body:
            return None, error_body, status_code

        normalized_metrics['instrumentSearches'], error_body, status_code = PSOAuthService.normalize_progression_metric_counter(
            metrics.get('instrumentSearches', 0),
            'instrumentSearches'
        )
        if error_body:
            return None, error_body, status_code

        normalized_metrics['uniqueRecordingKeys'], error_body, status_code = PSOAuthService.normalize_progression_metric_string_list(
            metrics.get('uniqueRecordingKeys', []),
            'uniqueRecordingKeys'
        )
        if error_body:
            return None, error_body, status_code

        normalized_metrics['recordingPlays'], error_body, status_code = PSOAuthService.normalize_progression_metric_counter(
            metrics.get('recordingPlays', 0),
            'recordingPlays'
        )
        if error_body:
            return None, error_body, status_code

        normalized_metrics['uniqueMusicianCards'], error_body, status_code = PSOAuthService.normalize_progression_metric_string_list(
            metrics.get('uniqueMusicianCards', []),
            'uniqueMusicianCards'
        )
        if error_body:
            return None, error_body, status_code

        normalized_metrics['viewedConcertCalendar'], error_body, status_code = PSOAuthService.normalize_progression_metric_counter(
            metrics.get('viewedConcertCalendar', 0),
            'viewedConcertCalendar',
            clamp_to_binary=True,
        )
        if error_body:
            return None, error_body, status_code

        normalized_metrics['viewedTicketsSection'], error_body, status_code = PSOAuthService.normalize_progression_metric_counter(
            metrics.get('viewedTicketsSection', 0),
            'viewedTicketsSection',
            clamp_to_binary=True,
        )
        if error_body:
            return None, error_body, status_code

        normalized_metrics['gamesPlayed'], error_body, status_code = PSOAuthService.normalize_progression_metric_counter(
            metrics.get('gamesPlayed', 0),
            'gamesPlayed'
        )
        if error_body:
            return None, error_body, status_code

        normalized_metrics['uniqueGames'], error_body, status_code = PSOAuthService.normalize_progression_metric_string_list(
            metrics.get('uniqueGames', []),
            'uniqueGames'
        )
        if error_body:
            return None, error_body, status_code

        return {
            'schemaVersion': PSOAuthService.PROGRESSION_SCHEMA_VERSION,
            'xp': normalized_xp,
            'completedQuests': normalized_completed_quests,
            'metrics': normalized_metrics,
            'lastUpdatedAt': PSOAuthService.progression_timestamp(),
        }, None, None

    @staticmethod
    def progression_timestamp():
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def progression_payload(record=None):
        if record is None:
            return PSOAuthService.progression_default_state()

        try:
            completed_quests = json.loads(record['completed_quests'] or '[]')
        except (TypeError, ValueError, json.JSONDecodeError):
            completed_quests = []

        try:
            metrics = json.loads(record['metrics'] or '{}')
        except (TypeError, ValueError, json.JSONDecodeError):
            metrics = {}

        if int(record['schema_version'] or 0) != PSOAuthService.PROGRESSION_SCHEMA_VERSION:
            return PSOAuthService.progression_default_state()

        return {
            'schemaVersion': PSOAuthService.PROGRESSION_SCHEMA_VERSION,
            'xp': PSOAuthService.normalize_progression_xp(record['xp']),
            'completedQuests': PSOAuthService.normalize_completed_quests(completed_quests),
            'metrics': {
                **PSOAuthService.progression_default_metrics(),
                **PSOAuthService.normalize_progression_metrics(metrics),
            },
            'lastUpdatedAt': record['last_updated_at'] or PSOAuthService.progression_timestamp(),
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
                    phone TEXT,
                    experience TEXT,
                    background TEXT,
                    piece TEXT,
                    availability TEXT,
                    video_file TEXT,
                    video_link TEXT,
                    status TEXT NOT NULL DEFAULT 'pending',
                    submitted_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    reviewed_at TEXT,
                    reviewed_by INTEGER,
                    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
                    FOREIGN KEY (reviewed_by) REFERENCES users(id)
                )
                '''
            )

            request_columns = {
                column['name'] for column in connection.execute('PRAGMA table_info(member_requests)').fetchall()
            }
            if 'phone' not in request_columns:
                connection.execute("ALTER TABLE member_requests ADD COLUMN phone TEXT")
            if 'experience' not in request_columns:
                connection.execute("ALTER TABLE member_requests ADD COLUMN experience TEXT")
            if 'background' not in request_columns:
                connection.execute("ALTER TABLE member_requests ADD COLUMN background TEXT")
            if 'piece' not in request_columns:
                connection.execute("ALTER TABLE member_requests ADD COLUMN piece TEXT")
            if 'availability' not in request_columns:
                connection.execute("ALTER TABLE member_requests ADD COLUMN availability TEXT")
            if 'video_file' not in request_columns:
                connection.execute("ALTER TABLE member_requests ADD COLUMN video_file TEXT")
            if 'video_link' not in request_columns:
                connection.execute("ALTER TABLE member_requests ADD COLUMN video_link TEXT")

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
                    schema_version INTEGER NOT NULL DEFAULT 2,
                    xp INTEGER NOT NULL DEFAULT 0,
                    completed_quests TEXT NOT NULL DEFAULT '[]',
                    metrics TEXT NOT NULL DEFAULT '{}',
                    last_updated_at TEXT,
                    FOREIGN KEY (uid) REFERENCES users(uid) ON DELETE CASCADE
                )
                '''
            )
            progression_columns = {
                column['name'] for column in connection.execute('PRAGMA table_info(user_progression)').fetchall()
            }
            if 'schema_version' not in progression_columns:
                connection.execute(
                    'ALTER TABLE user_progression ADD COLUMN schema_version INTEGER NOT NULL DEFAULT 2'
                )

            # One-time migration reset for pre-v2 progression rows.
            connection.execute(
                '''
                UPDATE user_progression
                SET schema_version = ?,
                    xp = 0,
                    completed_quests = '[]',
                    metrics = ?,
                    last_updated_at = ?
                WHERE schema_version IS NULL OR schema_version != ?
                ''',
                (
                    PSOAuthService.PROGRESSION_SCHEMA_VERSION,
                    json.dumps(PSOAuthService.progression_default_metrics()),
                    PSOAuthService.progression_timestamp(),
                    PSOAuthService.PROGRESSION_SCHEMA_VERSION,
                )
            )

            # NEW: shared chat table for all devices/computers
            connection.execute(
                '''
                CREATE TABLE IF NOT EXISTS membership_chat_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    thread_uid TEXT NOT NULL,
                    sender_uid TEXT NOT NULL,
                    sender_name TEXT NOT NULL,
                    sender_role TEXT NOT NULL,
                    text TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (thread_uid) REFERENCES users(uid) ON DELETE CASCADE
                )
                '''
            )
            connection.execute(
                'CREATE INDEX IF NOT EXISTS idx_membership_chat_thread_uid ON membership_chat_messages(thread_uid)'
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

        return None, {
            'message': 'Invalid Poway Orchestra user ID or password. If you have not created a Poway account yet, sign up first.'
        }, 401

    @staticmethod
    def issue_token(user):
        payload = {
            'uid': user.uid,
            'role': user.role,
            'exp': datetime.now(timezone.utc).timestamp() + 60 * 60 * 24 * 7
        }
        return jwt.encode(payload, current_app.config['SECRET_KEY'], algorithm='HS256')

    @staticmethod
    def cookie_options(expired=False):
        options = {
            'httponly': True,
            'secure': False,
            'samesite': 'Lax',
            'path': '/',
        }
        if expired:
            options['expires'] = 0
        else:
            options['max_age'] = 60 * 60 * 24 * 7
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
            payload = jwt.decode(token, current_app.config['SECRET_KEY'], algorithms=['HS256'])
        except Exception:
            return None, {
                'message': 'Authentication required. Invalid token.',
                'data': None,
                'error': 'Unauthorized'
            }, 401

        user = PSOAuthService.find_user_by_uid(payload.get('uid'))
        if user is None:
            return None, {
                'message': 'Authentication required. User not found.',
                'data': None,
                'error': 'Unauthorized'
            }, 401

        return user, None, None

    @staticmethod
    def signup_payload(user):
        return {
            'message': 'Signup successful',
            'uid': user.uid,
            'name': user.name,
            'email': user.email,
            'role': user.role,
            'is_admin': user.is_admin()
        }

    @staticmethod
    def login_payload(user):
        return {
            'message': 'Login successful',
            'uid': user.uid,
            'name': user.name,
            'email': user.email,
            'role': user.role,
            'is_admin': user.is_admin()
        }

    @staticmethod
    def logout_payload(user):
        return {
            'message': 'Logout successful',
            'uid': user.uid
        }

    @staticmethod
    def current_user_payload(user):
        return {
            'uid': user.uid,
            'name': user.name,
            'email': user.email,
            'role': user.role,
            'is_admin': user.is_admin()
        }

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
                      mr.phone, mr.experience, mr.background, mr.piece, mr.availability,
                      mr.video_file, mr.video_link,
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
                      mr.phone, mr.experience, mr.background, mr.piece, mr.availability,
                      mr.video_file, mr.video_link,
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
                SELECT uid, schema_version, xp, completed_quests, metrics, last_updated_at
                FROM user_progression
                WHERE uid = ?
                ''',
                (uid,)
            ).fetchone()

            if record is None:
                default_state = PSOAuthService.progression_default_state()
                connection.execute(
                    '''
                    INSERT INTO user_progression (uid, schema_version, xp, completed_quests, metrics, last_updated_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ''',
                    (
                        uid,
                        default_state['schemaVersion'],
                        default_state['xp'],
                        json.dumps(default_state['completedQuests']),
                        json.dumps(default_state['metrics']),
                        default_state['lastUpdatedAt'],
                    )
                )
                connection.commit()
                return default_state

            if int(record['schema_version'] or 0) != PSOAuthService.PROGRESSION_SCHEMA_VERSION:
                default_state = PSOAuthService.progression_default_state()
                connection.execute(
                    '''
                    UPDATE user_progression
                    SET schema_version = ?, xp = ?, completed_quests = ?, metrics = ?, last_updated_at = ?
                    WHERE uid = ?
                    ''',
                    (
                        default_state['schemaVersion'],
                        default_state['xp'],
                        json.dumps(default_state['completedQuests']),
                        json.dumps(default_state['metrics']),
                        default_state['lastUpdatedAt'],
                        uid,
                    )
                )
                connection.commit()
                return default_state

        return PSOAuthService.progression_payload(record)

    @staticmethod
    def save_progression(uid, body):
        normalized_payload, error_body, status_code = PSOAuthService.normalize_progression_payload_v2(body)
        if error_body:
            return None, error_body, status_code

        PSOAuthService.ensure_database()
        with PSOAuthService.get_connection() as connection:
            connection.execute(
                '''
                INSERT INTO user_progression (uid, schema_version, xp, completed_quests, metrics, last_updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(uid) DO UPDATE SET
                    schema_version = excluded.schema_version,
                    xp = excluded.xp,
                    completed_quests = excluded.completed_quests,
                    metrics = excluded.metrics,
                    last_updated_at = excluded.last_updated_at
                ''',
                (
                    uid,
                    normalized_payload['schemaVersion'],
                    normalized_payload['xp'],
                    json.dumps(normalized_payload['completedQuests']),
                    json.dumps(normalized_payload['metrics']),
                    normalized_payload['lastUpdatedAt'],
                )
            )
            connection.commit()

        return normalized_payload, None, 200

    @staticmethod
    def submit_member_request(
        uid,
        name,
        email,
        instrument,
        section,
        phone=None,
        experience=None,
        background=None,
        piece=None,
        availability=None,
        video_file=None,
        video_link=None,
    ):
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

        # NEW: admins should not request membership
        if user.is_admin():
            return None, {'message': 'Admins already have orchestra access and cannot submit a membership request.'}, 409

        if PSOAuthService.is_member(uid):
            return None, {'message': 'User is already registered as a member'}, 409

        latest_request = PSOAuthService.get_latest_member_request(uid)
        normalized_name = str(name).strip()
        normalized_instrument = str(instrument).strip()
        normalized_section = str(section).strip()
        normalized_email = str(email).strip().lower()
        normalized_phone = None if phone is None else str(phone).strip()
        normalized_experience = None if experience is None else str(experience).strip()
        normalized_background = None if background is None else str(background).strip()
        normalized_piece = None if piece is None else str(piece).strip()
        normalized_availability = None if availability is None else str(availability).strip()
        normalized_video_file = None if video_file is None else str(video_file).strip()
        normalized_video_link = None if video_link is None else str(video_link).strip()

        updated, error_body, status_code = PSOAuthService.update_user_email(uid, normalized_email)
        if not updated:
            return None, error_body, status_code

        user = PSOAuthService.find_user_by_uid(uid)

        if latest_request is not None and latest_request['status'] == 'pending':
            update_fields = {
                'name': normalized_name,
                'email': user.email,
                'instrument': normalized_instrument,
                'section': normalized_section,
            }

            if normalized_phone is not None:
                update_fields['phone'] = normalized_phone
            if normalized_experience is not None:
                update_fields['experience'] = normalized_experience
            if normalized_background is not None:
                update_fields['background'] = normalized_background
            if normalized_piece is not None:
                update_fields['piece'] = normalized_piece
            if normalized_availability is not None:
                update_fields['availability'] = normalized_availability
            if normalized_video_file is not None:
                update_fields['video_file'] = normalized_video_file
            if normalized_video_link is not None:
                update_fields['video_link'] = normalized_video_link

            assignments = ', '.join([f"{key} = ?" for key in update_fields.keys()])
            parameters = list(update_fields.values()) + [latest_request['id']]

            with PSOAuthService.get_connection() as connection:
                connection.execute(
                    f'UPDATE member_requests SET {assignments} WHERE id = ?',
                    tuple(parameters)
                )
                connection.commit()

            return PSOAuthService.get_member_request_by_id(latest_request['id']), None, 200

        with PSOAuthService.get_connection() as connection:
            cursor = connection.execute(
                '''
                INSERT INTO member_requests (
                    user_id, uid, name, email, instrument, section,
                    phone, experience, background, piece, availability,
                    video_file, video_link, status
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')
                ''',
                (
                    user.id,
                    user.uid,
                    normalized_name,
                    user.email,
                    normalized_instrument,
                    normalized_section,
                    normalized_phone,
                    normalized_experience,
                    normalized_background,
                    normalized_piece,
                    normalized_availability,
                    normalized_video_file,
                    normalized_video_link,
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
            'mr.phone, mr.experience, mr.background, mr.piece, mr.availability, '
            'mr.video_file, mr.video_link, '
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
    def get_admin_member_request_detail(request_id):
        request_row = PSOAuthService.get_member_request_by_id(request_id)
        if request_row is None:
            return None, {'message': 'Request not found'}, 404

        request_uid = str(request_row['uid'] or '').strip()
        target_user = PSOAuthService.find_user_by_uid(request_uid)
        target_member = PSOAuthService.get_member_by_uid(request_uid)

        return {
            'request': request_row,
            'user': target_user.read() if target_user is not None else None,
            'member': target_member,
            'messages': PSOAuthService.list_chat_messages(request_uid)
        }, None, 200

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
    def list_member_cards(current_user=None, family=None, section_id=None):
        PSOAuthService.ensure_database()

        query = (
            'SELECT id, owner_uid, owner_name, family, section_id, instrument_title, '
            'image_url, bio, created_by_uid, created_at, updated_at '
            'FROM member_cards '
        )
        parameters = []
        filters = []

        normalized_family = str(family or '').strip()
        if normalized_family:
            filters.append('family = ?')
            parameters.append(normalized_family)

        normalized_section_id = str(section_id or '').strip()
        if normalized_section_id:
            filters.append('section_id = ?')
            parameters.append(normalized_section_id)

        if filters:
            query += 'WHERE ' + ' AND '.join(filters) + ' '

        query += 'ORDER BY updated_at DESC, id DESC'

        with PSOAuthService.get_connection() as connection:
            records = connection.execute(query, tuple(parameters)).fetchall()

        return [dict(record) for record in records]

    @staticmethod
    def list_progression_leaderboard(limit=25):
        PSOAuthService.ensure_database()

        try:
            normalized_limit = max(1, min(int(limit or 25), 100))
        except (TypeError, ValueError):
            normalized_limit = 25

        with PSOAuthService.get_connection() as connection:
            records = connection.execute(
                '''
                SELECT up.uid, up.xp, up.completed_quests, up.last_updated_at,
                       u.name, u.email, u.role,
                       om.instrument, om.section
                FROM user_progression up
                JOIN users u ON u.uid = up.uid
                LEFT JOIN orchestra_members om ON om.uid = up.uid
                ORDER BY up.xp DESC, up.last_updated_at DESC, u.name COLLATE NOCASE ASC, up.uid COLLATE NOCASE ASC
                LIMIT ?
                ''',
                (normalized_limit,)
            ).fetchall()

        leaderboard = []
        for index, record in enumerate(records, start=1):
            try:
                completed_quests = json.loads(record['completed_quests'] or '[]')
            except (TypeError, ValueError, json.JSONDecodeError):
                completed_quests = []

            leaderboard.append({
                'rank': index,
                'uid': record['uid'],
                'name': record['name'],
                'email': record['email'],
                'role': record['role'],
                'xp': PSOAuthService.normalize_progression_xp(record['xp']),
                'completed_quest_count': len(PSOAuthService.normalize_completed_quests(completed_quests)),
                'instrument': record['instrument'],
                'section': record['section'],
                'last_updated_at': record['last_updated_at'],
            })

        return leaderboard

    # ------------------------------
    # NEW CHAT HELPERS
    # ------------------------------

    @staticmethod
    def serialize_chat_message(record):
        return {
            'id': record['id'],
            'thread_uid': record['thread_uid'],
            'sender_uid': record['sender_uid'],
            'sender_name': record['sender_name'],
            'sender_role': record['sender_role'],
            'text': record['text'],
            'created_at': record['created_at'],
        }

    @staticmethod
    def list_chat_messages(thread_uid):
        PSOAuthService.ensure_database()
        with PSOAuthService.get_connection() as connection:
            records = connection.execute(
                '''
                SELECT id, thread_uid, sender_uid, sender_name, sender_role, text, created_at
                FROM membership_chat_messages
                WHERE thread_uid = ?
                ORDER BY id ASC
                ''',
                (thread_uid,)
            ).fetchall()

        return [PSOAuthService.serialize_chat_message(record) for record in records]

    @staticmethod
    def send_chat_message(thread_uid, sender_user, text):
        PSOAuthService.ensure_database()

        normalized_thread_uid = str(thread_uid or '').strip()
        normalized_text = str(text or '').strip()
        if not normalized_thread_uid:
            return None, {'message': 'thread_uid is required'}, 400
        if not normalized_text:
            return None, {'message': 'Message text is required'}, 400

        recipient = PSOAuthService.find_user_by_uid(normalized_thread_uid)
        if recipient is None:
            return None, {'message': 'Chat recipient not found'}, 404

        with PSOAuthService.get_connection() as connection:
            cursor = connection.execute(
                '''
                INSERT INTO membership_chat_messages (thread_uid, sender_uid, sender_name, sender_role, text)
                VALUES (?, ?, ?, ?, ?)
                ''',
                (
                    normalized_thread_uid,
                    sender_user.uid,
                    sender_user.name,
                    str(sender_user.role or 'user').lower(),
                    normalized_text,
                )
            )
            connection.commit()

            record = connection.execute(
                '''
                SELECT id, thread_uid, sender_uid, sender_name, sender_role, text, created_at
                FROM membership_chat_messages
                WHERE id = ?
                ''',
                (cursor.lastrowid,)
            ).fetchone()

        return PSOAuthService.serialize_chat_message(record), None, 201

    @staticmethod
    def send_chat_message_for_request(request_id, sender_user, text):
        request_row = PSOAuthService.get_member_request_by_id(request_id)
        if request_row is None:
            return None, {'message': 'Request not found'}, 404

        return PSOAuthService.send_chat_message(
            thread_uid=request_row['uid'],
            sender_user=sender_user,
            text=text
        )

    @staticmethod
    def get_chat_thread_for_user(current_user, thread_uid=None):
        target_uid = str(thread_uid or current_user.uid).strip()
        if not current_user.is_admin() and target_uid != current_user.uid:
            return None, {'message': 'Forbidden'}, 403

        target_user = PSOAuthService.find_user_by_uid(target_uid)
        if target_user is None:
            return None, {'message': 'User not found'}, 404

        target_request = PSOAuthService.get_latest_member_request(target_uid)
        target_member = PSOAuthService.get_member_by_uid(target_uid)

        return {
            'uid': target_uid,
            'name': target_user.name,
            'email': target_user.email,
            'role': target_user.role,
            'request': target_request,
            'member': target_member,
            'messages': PSOAuthService.list_chat_messages(target_uid)
        }, None, 200

    @staticmethod
    def list_chat_threads_for_admin():
        PSOAuthService.ensure_database()

        with PSOAuthService.get_connection() as connection:
            records = connection.execute(
                '''
                SELECT DISTINCT
                    u.uid,
                    u.name,
                    u.email,
                    u.role,
                    mr.status AS request_status,
                    mr.instrument AS request_instrument,
                    mr.section AS request_section,
                    last_msg.text AS latest_text,
                    last_msg.created_at AS latest_created_at
                FROM users u
                JOIN membership_chat_messages m ON m.thread_uid = u.uid
                LEFT JOIN member_requests mr
                    ON mr.id = (
                        SELECT mr2.id
                        FROM member_requests mr2
                        WHERE mr2.uid = u.uid
                        ORDER BY mr2.id DESC
                        LIMIT 1
                    )
                LEFT JOIN membership_chat_messages last_msg
                    ON last_msg.id = (
                        SELECT m2.id
                        FROM membership_chat_messages m2
                        WHERE m2.thread_uid = u.uid
                        ORDER BY m2.id DESC
                        LIMIT 1
                    )
                ORDER BY last_msg.id DESC
                '''
            ).fetchall()

        output = []
        for record in records:
            output.append({
                'uid': record['uid'],
                'name': record['name'],
                'email': record['email'],
                'role': record['role'],
                'request_status': record['request_status'],
                'request_instrument': record['request_instrument'],
                'request_section': record['request_section'],
                'latest_text': record['latest_text'],
                'latest_created_at': record['latest_created_at'],
            })
        return output