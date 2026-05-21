from flask import Blueprint, jsonify, request
from flask_cors import CORS
from flask_restful import Api, Resource

from model.pso import PSOAuthService


pso_api = Blueprint('pso_api', __name__, url_prefix='/api')
CORS(
    pso_api,
    supports_credentials=True,
    methods=['GET', 'POST', 'PATCH', 'PUT', 'DELETE', 'OPTIONS'],
    allow_headers=['Content-Type', 'X-Origin', 'Authorization'],
)
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
            body = request.get_json() or {}
            if not body:
                return {
                    'message': 'Please provide user details',
                    'data': None,
                    'error': 'Bad request'
                }, 400

            uid = (
                body.get('uid')
                or body.get('username')
                or body.get('userId')
                or body.get('user_id')
                or body.get('login')
            )
            name = (
                body.get('name')
                or body.get('full_name')
                or body.get('fullName')
                or body.get('display_name')
                or body.get('displayName')
                or uid
            )
            email = body.get('email')
            if not email and uid:
                email = PSOAuthService.default_email_for_uid(uid)

            password = (
                body.get('password')
                or body.get('newPassword')
                or body.get('new_password')
            )

            user, error_body, status_code = PSOAuthService.create_user(
                name,
                uid,
                email,
                password
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
            body = request.get_json() or {}
            if not body:
                return {
                    'message': 'Please provide user details',
                    'data': None,
                    'error': 'Bad request'
                }, 400

            identifier = (
                body.get('uid')
                or body.get('username')
                or body.get('userId')
                or body.get('user_id')
                or body.get('login')
                or body.get('email')
                or body.get('name')
            )

            password = (
                body.get('password')
                or body.get('newPassword')
                or body.get('new_password')
            )

            user, error_body, status_code = PSOAuthService.authenticate(
                identifier,
                password
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
                body.get('section'),
                body.get('phone') or body.get('phoneNumber') or body.get('phone_number'),
                body.get('experience')
                or body.get('years')
                or body.get('yearsOfExperience')
                or body.get('years_of_experience'),
                body.get('background')
                or body.get('bio')
                or body.get('shortBackground')
                or body.get('short_background')
                or body.get('musicalBackground')
                or body.get('musical_background'),
                body.get('piece') or body.get('auditionPiece') or body.get('audition_piece'),
                body.get('availability'),
                body.get('video') or body.get('videoFile') or body.get('video_file'),
                body.get('videoLink') or body.get('video_link') or body.get('videoUrl') or body.get('video_url')
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
                    'member_request_status': PSOAuthService.get_member_request_status(current_user.uid),
                    **PSOAuthService.member_profile_payload(current_user.uid)
                })

            return jsonify(PSOAuthService.member_profile_payload(current_user.uid))

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
                'member': PSOAuthService.member_profile_payload(current_user.uid)
            })

    class _Progression(Resource):
        def get(self):
            current_user, error_body, status_code = PSOAuthService.authenticate_request()
            if error_body:
                return error_body, status_code

            return jsonify(PSOAuthService.get_progression(current_user.uid))

        def put(self):
            current_user, error_body, status_code = PSOAuthService.authenticate_request()
            if error_body:
                return error_body, status_code

            progression, error_body, status_code = PSOAuthService.save_progression(current_user.uid, request.get_json() or {})
            if error_body:
                return error_body, status_code

            return jsonify(progression)

    class _MemberCards(Resource):
        def get(self):
            family = request.args.get('family')
            section_id = request.args.get('section_id') or request.args.get('sectionId')
            return jsonify({
                'cards': PSOAuthService.list_member_cards(
                    current_user=None,
                    family=family,
                    section_id=section_id
                )
            })

    class _AdminMemberCards(Resource, _AdminMixin):
        def post(self):
            current_user, error_body, status_code = PSOAuthService.authenticate_request()
            if error_body:
                return error_body, status_code

            admin_error, admin_status = self.require_admin(current_user)
            if admin_error:
                return admin_error, admin_status

            card, error_body, status_code = PSOAuthService.create_member_card(current_user, request.get_json() or {})
            if error_body:
                return error_body, status_code

            response = jsonify({
                'message': 'Member card created.',
                'card': card
            })
            response.status_code = status_code
            return response

    class _MemberCardDetail(Resource):
        def options(self, card_id):
            response = jsonify({'message': 'OK'})
            response.headers['Access-Control-Allow-Methods'] = 'GET, PATCH, PUT, DELETE, OPTIONS'
            response.headers['Access-Control-Allow-Headers'] = 'Content-Type, X-Origin, Authorization'
            return response

        def get(self, card_id):
            card = PSOAuthService.get_member_card_by_id(card_id)
            if card is None:
                return {'message': 'Member card not found'}, 404

            return jsonify({
                'card': card
            })

        def patch(self, card_id):
            current_user, error_body, status_code = PSOAuthService.authenticate_request()
            if error_body:
                return error_body, status_code

            updated_card, error_body, status_code = PSOAuthService.update_member_card(
                card_id,
                current_user,
                request.get_json() or {}
            )
            if error_body:
                return error_body, status_code

            return jsonify({
                'message': 'Member card updated.',
                'card': updated_card
            })

        def put(self, card_id):
            return self.patch(card_id)

        def delete(self, card_id):
            current_user, error_body, status_code = PSOAuthService.authenticate_request()
            if error_body:
                return error_body, status_code

            deleted, error_body, status_code = PSOAuthService.delete_member_card(card_id, current_user)
            if not deleted:
                return error_body, status_code

            return jsonify({'message': 'Member card deleted.'})

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

            response = jsonify({
                'message': 'Request approved successfully.',
                'request': approved_request,
                'member': PSOAuthService.member_profile_payload(approved_request['uid'])
            })
            response.status_code = status_code
            return response

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

            response = jsonify({
                'message': 'Request rejected successfully.',
                'request': rejected_request
            })
            response.status_code = status_code
            return response

    class _AdminMemberRequestDetail(Resource, _AdminMixin):
        def get(self, request_id):
            current_user, error_body, status_code = PSOAuthService.authenticate_request()
            if error_body:
                return error_body, status_code

            admin_error, admin_status = self.require_admin(current_user)
            if admin_error:
                return admin_error, admin_status

            detail, error_body, status_code = PSOAuthService.get_admin_member_request_detail(request_id)
            if error_body:
                return error_body, status_code

            return jsonify(detail)

    class _AdminMemberRequestChatMessages(Resource, _AdminMixin):
        def post(self, request_id):
            current_user, error_body, status_code = PSOAuthService.authenticate_request()
            if error_body:
                return error_body, status_code

            admin_error, admin_status = self.require_admin(current_user)
            if admin_error:
                return admin_error, admin_status

            body = request.get_json() or {}
            message, error_body, status_code = PSOAuthService.send_chat_message_for_request(
                request_id=request_id,
                sender_user=current_user,
                text=body.get('text')
            )
            if error_body:
                return error_body, status_code

            response = jsonify({
                'message': 'Chat message sent.',
                'chat_message': message
            })
            response.status_code = status_code
            return response

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

    # NEW: user/admin shared chat endpoints
    class _MembershipChatThread(Resource):
        def get(self):
            current_user, error_body, status_code = PSOAuthService.authenticate_request()
            if error_body:
                return error_body, status_code

            thread_uid = request.args.get('thread_uid') or request.args.get('uid')
            thread, error_body, status_code = PSOAuthService.get_chat_thread_for_user(current_user, thread_uid=thread_uid)
            if error_body:
                return error_body, status_code

            return jsonify(thread)

    class _MembershipChatMessages(Resource):
        def post(self):
            current_user, error_body, status_code = PSOAuthService.authenticate_request()
            if error_body:
                return error_body, status_code

            body = request.get_json() or {}
            thread_uid = body.get('thread_uid') or current_user.uid

            # non-admin users can only send to their own thread
            if not current_user.is_admin() and str(thread_uid) != str(current_user.uid):
                return {'message': 'Forbidden'}, 403

            message, error_body, status_code = PSOAuthService.send_chat_message(
                thread_uid=thread_uid,
                sender_user=current_user,
                text=body.get('text')
            )
            if error_body:
                return error_body, status_code

            response = jsonify({
                'message': 'Chat message sent.',
                'chat_message': message
            })
            response.status_code = status_code
            return response

    class _AdminChatThreads(Resource, _AdminMixin):
        def get(self):
            current_user, error_body, status_code = PSOAuthService.authenticate_request()
            if error_body:
                return error_body, status_code

            admin_error, admin_status = self.require_admin(current_user)
            if admin_error:
                return admin_error, admin_status

            return jsonify({
                'threads': PSOAuthService.list_chat_threads_for_admin()
            })


api.add_resource(PSOAPI._Signup, '/pso/signup')
api.add_resource(PSOAPI._Authenticate, '/authenticate')
api.add_resource(PSOAPI._Identity, '/id')
api.add_resource(PSOAPI._MemberRegister, '/pso/member-request', endpoint='pso_member_request')
api.add_resource(PSOAPI._MemberRegister, '/pso/member/register', endpoint='pso_member_register_legacy')
api.add_resource(PSOAPI._MemberRequestStatus, '/pso/member-request/status')
api.add_resource(PSOAPI._MemberProfile, '/pso/member/profile')
api.add_resource(PSOAPI._Progression, '/pso/progression')
api.add_resource(PSOAPI._MemberCards, '/pso/member-cards')
api.add_resource(PSOAPI._AdminMemberCards, '/pso/admin/member-cards')
api.add_resource(PSOAPI._MemberCardDetail, '/pso/member-cards/<int:card_id>')
api.add_resource(PSOAPI._AdminMemberRequests, '/pso/admin/member-requests')
api.add_resource(PSOAPI._AdminApproveMemberRequest, '/pso/admin/member-requests/<int:request_id>/approve')
api.add_resource(PSOAPI._AdminRejectMemberRequest, '/pso/admin/member-requests/<int:request_id>/reject')
api.add_resource(PSOAPI._AdminMemberRequestDetail, '/pso/admin/member-requests/<int:request_id>')
api.add_resource(PSOAPI._AdminMemberRequestChatMessages, '/pso/admin/member-requests/<int:request_id>/chat/messages')
api.add_resource(PSOAPI._AdminMembers, '/pso/admin/members')
api.add_resource(PSOAPI._AdminAccess, '/pso/admin/access')

# NEW
api.add_resource(PSOAPI._MembershipChatThread, '/pso/chat/thread')
api.add_resource(PSOAPI._MembershipChatMessages, '/pso/chat/messages')
api.add_resource(PSOAPI._AdminChatThreads, '/pso/admin/chat/threads')