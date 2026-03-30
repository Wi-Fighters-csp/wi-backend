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
        self.role = record['role']
        self.password_hash = record['password_hash']

    def is_admin(self):
        return self.role == 'Admin'

    def is_teacher(self):
        return self.role == 'Teacher'

    def read(self):
        return {
            'id': self.id,
            'uid': self.uid,
            'name': self.name,
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
        return connection

    @staticmethod
    def ensure_database():
        with PSOAuthService.get_connection() as connection:
            connection.execute(
                '''
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    uid TEXT NOT NULL UNIQUE,
                    name TEXT NOT NULL,
                    password_hash TEXT NOT NULL,
                    role TEXT NOT NULL DEFAULT 'User',
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                '''
            )
            connection.commit()

        PSOAuthService.seed_default_user()

    @staticmethod
    def seed_default_user():
        default_uid = os.environ.get('PSO_UID') or app.config.get('ADMIN_UID', 'admin')
        default_name = os.environ.get('PSO_NAME') or app.config.get('ADMIN_USER', 'PSO Admin')
        default_password = os.environ.get('PSO_PASSWORD') or app.config.get('ADMIN_PASSWORD') or app.config.get('DEFAULT_PASSWORD', 'password')
        default_role = os.environ.get('PSO_ROLE') or 'Admin'

        with PSOAuthService.get_connection() as connection:
            existing = connection.execute(
                'SELECT id FROM users WHERE uid = ?',
                (default_uid,)
            ).fetchone()
            if existing is None:
                connection.execute(
                    'INSERT INTO users (uid, name, password_hash, role) VALUES (?, ?, ?, ?)',
                    (
                        default_uid,
                        default_name,
                        generate_password_hash(default_password, 'pbkdf2:sha256', salt_length=10),
                        default_role,
                    )
                )
                connection.commit()

    @staticmethod
    def find_user_by_uid(uid):
        PSOAuthService.ensure_database()
        with PSOAuthService.get_connection() as connection:
            record = connection.execute(
                'SELECT id, uid, name, password_hash, role FROM users WHERE uid = ?',
                (uid,)
            ).fetchone()

        if record is None:
            return None

        return PSOUser(record)

    @staticmethod
    def create_user(name, uid, password, role='User'):
        PSOAuthService.ensure_database()

        if name is None or len(str(name).strip()) < 2:
            return None, {'message': 'Name is missing or less than 2 characters'}, 400

        if uid is None or len(str(uid).strip()) < 2:
            return None, {'message': 'User ID is missing or less than 2 characters'}, 400

        if password is None or len(str(password)) < 8:
            return None, {'message': 'Password must be at least 8 characters'}, 400

        normalized_name = str(name).strip()
        normalized_uid = str(uid).strip()

        existing_user = PSOAuthService.find_user_by_uid(normalized_uid)
        if existing_user is not None:
            return None, {'message': f'User {normalized_uid} already exists'}, 409

        with PSOAuthService.get_connection() as connection:
            cursor = connection.execute(
                'INSERT INTO users (uid, name, password_hash, role) VALUES (?, ?, ?, ?)',
                (
                    normalized_uid,
                    normalized_name,
                    generate_password_hash(password, 'pbkdf2:sha256', salt_length=10),
                    role,
                )
            )
            connection.commit()

            record = connection.execute(
                'SELECT id, uid, name, password_hash, role FROM users WHERE id = ?',
                (cursor.lastrowid,)
            ).fetchone()

        return PSOUser(record), None, 201

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
        payload['is_admin'] = user.is_admin()
        payload['is_teacher'] = user.is_teacher()
        return payload

    @staticmethod
    def login_payload(user):
        return {
            'message': f'Authentication for {user.uid} successful',
            'user': {
                'uid': user.uid,
                'name': user.name,
                'role': user.role,
                'class': getattr(user, '_class', None) or []
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
                'role': user.role
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


PSOAuthService.ensure_database()