from flask import Blueprint, jsonify, request
from flask_restful import Api, Resource

from model.titanic import TitanicModel


titanic_api = Blueprint('titanic_api', __name__, url_prefix='/api/titanic')
api = Api(titanic_api)


class TitanicAPI:
    class _Predict(Resource):
        def post(self):
            passenger = request.get_json(silent=True) or {}

            try:
                response = TitanicModel.get_instance().predict(passenger)
                return jsonify(response)
            except ValueError as err:
                return {'message': str(err)}, 400
            except RuntimeError as err:
                return {'message': str(err)}, 503

    class _Meta(Resource):
        def get(self):
            try:
                return jsonify(TitanicModel.get_instance().metadata())
            except RuntimeError as err:
                return {'message': str(err)}, 503


api.add_resource(TitanicAPI._Predict, '/predict')
api.add_resource(TitanicAPI._Meta, '/meta')