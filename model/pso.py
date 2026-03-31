import os
import sqlite3

import jwt
from flask import current_app, request
from werkzeug.security import check_password_hash, generate_password_hash

from __init__ import app


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
    def authenticate(uid, password):
        PSOAuthService.ensure_database()

        if uid is None or len(str(uid).strip()) == 0:
            return None, {'message': 'User ID is missing'}, 401

        if password is None or len(str(password)) == 0:
            return None, {'message': 'Password is missing'}, 401

        user = PSOAuthService.find_user_by_uid(uid)
        if user is None or not check_password_hash(user.password_hash, password):
            return None, {'message': 'Invalid user id or password'}, 401

        return user, None, None

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
        latest_request = PSOAuthService.get_latest_member_request(uid)
        if latest_request is not None:
            return latest_request['status']
        if PSOAuthService.is_member(uid):
            return 'approved'
        return 'none'

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