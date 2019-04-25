import logging
import json

import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    # if the invocation method is OPTION, just respond with 200 to let the client know the endpoint is working
    if req.method == 'OPTIONS':
        return func.HttpResponse(
                headers={
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'OPTIONS, POST, GET',
                    'Access-Control-Allow-Headers': 'Content-Type',
                    'Access-Control-Max-Age': '3600'
                },
                status_code=200
            )

    # otherwise, if the request method is POST, get to work!
    elif req.method == 'GET':

        reach_id = req.params.get('reachid')
        if not reach_id:
            reach_id = req.params.get('reach_id')

        if reach_id:
            pass

        else:
            return func.HttpResponse(
                body=json.dumps({'errors': [
                    {
                        'title': 'missing reachid query parameter',
                        'desription': 'A reach id must be povided as a query parameter in the request - http://url?reachid=3306'
                    }
                ]}),
                headers={'content-type': 'application/json'},
                status_code=422
            )
