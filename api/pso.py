from flask import Blueprint, jsonify, request
from flask_restful import Api, Resource

from model.pso import PSOAuthService


pso_api = Blueprint('pso_api', __name__, url_prefix='/api')
api = Api(pso_api)


class PSOAPI:
    class _Signup(Resource):
        def post(self):
            body = request.get_json()
            if not body:
                return {
                    'message': 'Please provide user details',
                    'data': None,
                    'error': 'Bad request'
                }, 400

            user, error_body, status_code = PSOAuthService.create_user(
                body.get('name'),
                body.get('uid'),
                body.get('password')
            )
            if error_body:
                return error_body, status_code

            response = jsonify(PSOAuthService.signup_payload(user))
            response.status_code = status_code
            return PSOAuthService.attach_login_cookie(response, user)

    class _Authenticate(Resource):
        def post(self):
            body = request.get_json()
            if not body:
                return {
                    'message': 'Please provide user details',
                    'data': None,
                    'error': 'Bad request'
                }, 400

            user, error_body, status_code = PSOAuthService.authenticate(
                body.get('uid'),
                body.get('password')
            )
            if error_body:
                return error_body, status_code

            response = jsonify(PSOAuthService.login_payload(user))
            return PSOAuthService.attach_login_cookie(response, user)

        def delete(self):
            current_user, error_body, status_code = PSOAuthService.authenticate_request()
            if error_body:
                return error_body, status_code

            response = jsonify(PSOAuthService.logout_payload(current_user))
            return PSOAuthService.clear_login_cookie(response)

    class _Identity(Resource):
        def get(self):
            current_user, error_body, status_code = PSOAuthService.authenticate_request()
            if error_body:
                return error_body, status_code

            return jsonify(PSOAuthService.current_user_payload(current_user))


api.add_resource(PSOAPI._Signup, '/pso/signup')
api.add_resource(PSOAPI._Authenticate, '/authenticate')
api.add_resource(PSOAPI._Identity, '/id')