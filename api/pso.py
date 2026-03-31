from flask import Blueprint, jsonify, request
from flask_restful import Api, Resource

from model.pso import PSOAuthService


pso_api = Blueprint('pso_api', __name__, url_prefix='/api')
api = Api(pso_api)


class PSOAPI:
    class _AdminMixin:
        @staticmethod
        def require_admin(user):
            if not user.is_admin():
                return {
                    'message': 'Admin access required',
                    'data': None,
                    'error': 'Forbidden'
                }, 403
            return None, None

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
                body.get('email'),
                body.get('password')
            )
            if error_body:
                return error_body, status_code

            response = jsonify(PSOAuthService.signup_payload(user))
            response.status_code = status_code
            return PSOAuthService.attach_login_cookie(response, user)

    class _Authenticate(Resource):
        def options(self):
            return jsonify({'message': 'OK'})

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

    class _MemberRegister(Resource):
        def post(self):
            current_user, error_body, status_code = PSOAuthService.authenticate_request()
            if error_body:
                return error_body, status_code

            body = request.get_json() or {}
            member_request, error_body, status_code = PSOAuthService.submit_member_request(
                current_user.uid,
                body.get('name') or current_user.name,
                body.get('email'),
                body.get('instrument'),
                body.get('section')
            )
            if error_body:
                return error_body, status_code

            response = jsonify({
                'message': 'Member request submitted',
                'request': member_request
            })
            response.status_code = status_code
            return response

    class _MemberRequestStatus(Resource):
        def get(self):
            current_user, error_body, status_code = PSOAuthService.authenticate_request()
            if error_body:
                return error_body, status_code

            latest_request = PSOAuthService.get_latest_member_request(current_user.uid)
            return jsonify({
                'uid': current_user.uid,
                'is_member': PSOAuthService.is_member(current_user.uid),
                'member_request_status': PSOAuthService.get_member_request_status(current_user.uid),
                'request': latest_request
            })

    class _MemberProfile(Resource):
        def get(self):
            current_user, error_body, status_code = PSOAuthService.authenticate_request()
            if error_body:
                return error_body, status_code

            member = PSOAuthService.get_member_by_uid(current_user.uid)
            if member is None:
                return jsonify({
                    'is_member': False,
                    'uid': current_user.uid,
                    'name': current_user.name,
                    'email': current_user.email,
                    'member_request_status': PSOAuthService.get_member_request_status(current_user.uid)
                })

            member_payload = dict(member)
            member_payload['is_member'] = True
            member_payload['member_request_status'] = 'approved'
            return jsonify(member_payload)

        def put(self):
            current_user, error_body, status_code = PSOAuthService.authenticate_request()
            if error_body:
                return error_body, status_code

            body = request.get_json() or {}
            updated, error_body, status_code = PSOAuthService.update_member_profile(
                current_user.uid,
                body.get('instrument'),
                body.get('section'),
                body.get('practice_time')
            )
            if not updated:
                return error_body, status_code

            return jsonify({
                'message': 'Member profile updated',
                'member': PSOAuthService.get_member_by_uid(current_user.uid)
            })

    class _AdminMemberRequests(Resource, _AdminMixin):
        def get(self):
            current_user, error_body, status_code = PSOAuthService.authenticate_request()
            if error_body:
                return error_body, status_code

            admin_error, admin_status = self.require_admin(current_user)
            if admin_error:
                return admin_error, admin_status

            status = request.args.get('status', 'pending')
            return jsonify({
                'requests': PSOAuthService.list_member_requests(status=status)
            })

    class _AdminApproveMemberRequest(Resource, _AdminMixin):
        def post(self, request_id):
            current_user, error_body, status_code = PSOAuthService.authenticate_request()
            if error_body:
                return error_body, status_code

            admin_error, admin_status = self.require_admin(current_user)
            if admin_error:
                return admin_error, admin_status

            approved_request, error_body, status_code = PSOAuthService.approve_member_request(request_id, current_user)
            if error_body:
                return error_body, status_code

            return jsonify({
                'message': 'Member request approved',
                'request': approved_request,
                'member': PSOAuthService.get_member_by_uid(approved_request['uid'])
            })

    class _AdminRejectMemberRequest(Resource, _AdminMixin):
        def post(self, request_id):
            current_user, error_body, status_code = PSOAuthService.authenticate_request()
            if error_body:
                return error_body, status_code

            admin_error, admin_status = self.require_admin(current_user)
            if admin_error:
                return admin_error, admin_status

            rejected_request, error_body, status_code = PSOAuthService.reject_member_request(request_id, current_user)
            if error_body:
                return error_body, status_code

            return jsonify({
                'message': 'Member request rejected',
                'request': rejected_request
            })

    class _AdminMembers(Resource, _AdminMixin):
        def get(self):
            current_user, error_body, status_code = PSOAuthService.authenticate_request()
            if error_body:
                return error_body, status_code

            admin_error, admin_status = self.require_admin(current_user)
            if admin_error:
                return admin_error, admin_status

            return jsonify({
                'members': PSOAuthService.list_members()
            })

    class _AdminAccess(Resource, _AdminMixin):
        def get(self):
            current_user, error_body, status_code = PSOAuthService.authenticate_request()
            if error_body:
                return error_body, status_code

            admin_error, admin_status = self.require_admin(current_user)
            if admin_error:
                return admin_error, admin_status

            return jsonify({
                'allowed': True,
                'uid': current_user.uid,
                'role': current_user.role
            })


api.add_resource(PSOAPI._Signup, '/pso/signup')
api.add_resource(PSOAPI._Authenticate, '/authenticate')
api.add_resource(PSOAPI._Identity, '/id')
api.add_resource(PSOAPI._MemberRegister, '/pso/member-request', endpoint='pso_member_request')
api.add_resource(PSOAPI._MemberRegister, '/pso/member/register', endpoint='pso_member_register_legacy')
api.add_resource(PSOAPI._MemberRequestStatus, '/pso/member-request/status')
api.add_resource(PSOAPI._MemberProfile, '/pso/member/profile')
api.add_resource(PSOAPI._AdminMemberRequests, '/pso/admin/member-requests')
api.add_resource(PSOAPI._AdminApproveMemberRequest, '/pso/admin/member-requests/<int:request_id>/approve')
api.add_resource(PSOAPI._AdminRejectMemberRequest, '/pso/admin/member-requests/<int:request_id>/reject')
api.add_resource(PSOAPI._AdminMembers, '/pso/admin/members')
api.add_resource(PSOAPI._AdminAccess, '/pso/admin/access')