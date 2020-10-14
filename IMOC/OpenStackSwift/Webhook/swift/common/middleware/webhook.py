from swift.common.http import is_success
from swift.common.request_helpers import get_sys_meta_prefix
from swift.common.swob import wsgify
from swift.common.utils import get_logger
from swift.common.utils import register_swift_info
from swift.common.utils import split_path
from swift.proxy.controllers.base import get_container_info

from eventlet.green import urllib2
from eventlet import Timeout

import base64
import json
import ssl
import urlparse

# Container system metadata
# x-container-sysmeta-webhookk & x-container-sysmeta-webhook-auth
# Add x-container-sysmeta-webhook-get for hooks called during get requests
SYSMETA_WEBHOOK = get_sys_meta_prefix('container') + 'webhook'
SYSMETA_WEBHOOK_AUTH = get_sys_meta_prefix('container') + 'webhook-auth'
SYSMETA_WEBHOOK_GET = get_sys_meta_prefix('container') + 'webhook-get'
SYSMETA_WEBHOOK_GET_AUTH = get_sys_meta_prefix('container') + 'webhook-get-auth'


class WebhookMiddleware(object):

    def __init__(self, app, conf):
        self.app = app
        self.logger = get_logger(conf, log_route='webhook')

    @wsgify
    def __call__(self, req):
        obj = None
        container = None
        try:
            (version, account, container, obj) = \
                split_path(req.path_info, 3, 4, True)
        except ValueError:
            # not an object request
            pass

        # Sets webhook metadata
        if 'x-webhook' in req.headers and req.method == 'PUT':
            # Check if webhook-auth is provided before creating webhook
            if 'x-webhook-auth' in req.headers:
                # translate user's request header to sysmeta
                # Add callback URL (webhook) to container System Metadata
                req.headers[SYSMETA_WEBHOOK] = req.headers['x-webhook']
                self.logger.info(
                    'webhook.py: Adding webhook to container "%s": %s'
                    % (container, req.headers['x-webhook'])
                )

                # Add auth credentials to system metadata
                req.headers[SYSMETA_WEBHOOK_AUTH] = \
                    req.headers['x-webhook-auth']
                self.logger.info(
                    'webhook.py: Adding webhook credentials saved successfully'
                )

            else:
                # Do we fail since there is no authentication?
                self.logger.info(
                    'webhook.py: '
                    'ERROR: no webhook credentials provided; '
                    'Webhook not created'
                )

        else:
            # Webhook NOT in header
            pass

        # Removes webhook metadata
        if 'x-remove-webhook' in req.headers:
            # empty value will tombstone sysmeta
            req.headers[SYSMETA_WEBHOOK] = ''
            req.headers[SYSMETA_WEBHOOK_AUTH] = ''
            self.logger.info(
                'webhook.py: Removing webhook from container "%s"'
                % container
            )

        ################### Webhook-Get creation and delete code ##########################
        # Sets webhook metadata
        if 'x-webhook-get' in req.headers and req.method == 'PUT':
            # Check if webhook-auth is provided before creating webhook
            if 'x-webhook-get-auth' in req.headers:
                # translate user's request header to sysmeta
                # Add callback URL (webhook) to container System Metadata
                req.headers[SYSMETA_WEBHOOK_GET] = req.headers['x-webhook-get']
                self.logger.info(
                    'webhook.py: Adding webhook-get to container "%s": %s'
                    % (container, req.headers['x-webhook-get'])
                )

                # Add auth credentials to system metadata
                req.headers[SYSMETA_WEBHOOK_GET_AUTH] = \
                    req.headers['x-webhook-get-auth']
                self.logger.info(
                    'webhook.py: Adding webhook-get credentials saved successfully'
                )

            else:
                # Do we fail since there is no authentication?
                self.logger.info(
                    'webhook.py: '
                    'ERROR: no webhook-get credentials provided; '
                    'Webhook-get not created'
                )

        else:
            # Webhook NOT in header
            pass

        # Removes webhook metadata
        if 'x-remove-webhook-get' in req.headers:
            # empty value will tombstone sysmeta
            req.headers[SYSMETA_WEBHOOK_GET] = ''
            req.headers[SYSMETA_WEBHOOK_GET_AUTH] = ''
            self.logger.info(
                'webhook.py: Removing webhook-get from container "%s"'
                % container
            )

        ######################################################################################

        # Account and object storage will ignore x-container-sysmeta-*
        # Process request downstream...
        resp = req.get_response(self.app)

        # ...if successful, call webhook
        
        # Update hook so that hooks should also be called when we access an object
        # Need to check in handler if req.method != 'GET' before executing

        if obj and is_success(resp.status_int) and req.method != 'GET':
            # Get container info
            container_info = get_container_info(req.environ, self.app)

            # assign webook from sysmeta
            webhook_get = container_info['sysmeta'].get('webhook-get')

            if webhook_get and req.method == 'GET':
                self.logger.info(
                    'webhook.py: webhook-get found for container %s: %s'
                    % (container, webhook_get)
                )

                # Webhook-get detected. This means the current version of the data
                # is not updated from the cache. Call the action which tries to 
                # push the action to migrate this particular object.

                if resp.headers['r-data-migrate'] != 'ok':

                    ## Call the webhook-get parameter and fire the post
                    webhook_req = urllib2.Request(webhook_get)
                    creds = base64.b64encode(
                        container_info['sysmeta'].get('webhook-get-auth')
                    )
                    auth = "Basic %s" % creds 
                    webhook_req.add_header('Authorization',auth)

                    webhook_req.add_header('Content-Type','application/json')

                    # Forward the url back in order to get it by the wake_up_migration action

                    # Add response data
                    reqURL = urlparse.urlparse(req.url)
                    path = reqURL.path[0:reqURL.path.index('/', 4)]
                    swiftURL = reqURL.scheme + '://' + reqURL.netloc + path

                    mesg = {
                    "swiftObj": {
                        "url": swiftURL,
                        "token": req.headers['x-auth-token'],
                        "container": container,
                        "object": obj,
                        "method": req.method
                        }
                    }
                    self.logger.info(
                        'webhook.py: mesg body:  %s'
                        % mesg
                    )

                    webhook_req.add_data(json.dumps(mesg))

                    # TODO(): make request asynchronous
                    with Timeout(20):
                        try:
                            # make request
                            webhook_resp = urllib2.urlopen(webhook_req, context=ssl.SSLContext(ssl.PROTOCOL_SSLv23)).read()

                        except (Exception, Timeout):
                            self.logger.exception(
                                'failed POST to webhook-get %s' % webhook_get
                            )
                        else:
                            self.logger.info(
                                'successfully called webhook-get: %s \n%s'
                                % (webhook_get, webhook_resp)
                            )

            #### Normal Webhook behaviour 
            
            webhook = container_info['sysmeta'].get('webhook')

            if webhook:
                self.logger.info(
                    'webhook.py: webhook found for container %s: %s'
                    % (container, webhook)
                )

                # create a POST request with obj name as body
                webhook_req = urllib2.Request(webhook)

                # Get credentials from sysmeta and base64 encode
                creds = base64.b64encode(
                    container_info['sysmeta'].get('webhook-auth')
                )
                # Add credentials to headr
                auth = "Basic %s" % creds
                webhook_req.add_header('Authorization', auth)

                # Specify content type
                webhook_req.add_header('Content-Type', 'application/json')

                # Add response data
                reqURL = urlparse.urlparse(req.url)
                path = reqURL.path[0:reqURL.path.index('/', 4)]
                swiftURL = reqURL.scheme + '://' + reqURL.netloc + path

                mesg = {
                    "swiftObj": {
                        "url": swiftURL,
                        "token": req.headers['x-auth-token'],
                        "container": container,
                        "object": obj,
                        "method": req.method
                    }
                }
                self.logger.info(
                    'webhook.py: mesg body:  %s'
                    % mesg
                )

                webhook_req.add_data(json.dumps(mesg))

                # TODO(): make request asynchronous
                with Timeout(20):
                    try:
                        # make request
                        webhook_resp = urllib2.urlopen(webhook_req, context=ssl.SSLContext(ssl.PROTOCOL_SSLv23)).read()

                    except (Exception, Timeout):
                        self.logger.exception(
                            'failed POST to webhook %s' % webhook
                        )
                    else:
                        self.logger.info(
                            'successfully called webhook: %s \n%s'
                            % (webhook, webhook_resp)
                        )
            else:
                # No webhook associated with container
                self.logger.info(
                    'webhook.py: webhook not specified for container: %s'
                    % container
                )

        # Response back to user
        if 'x-container-sysmeta-webhook' in resp.headers:
            # translate sysmeta from the backend resp to
            # user-visible client resp header
            resp.headers['x-webhook'] = resp.headers[SYSMETA_WEBHOOK]
        return resp


def webhook_factory(global_conf, **local_conf):
    register_swift_info('webhook')

    conf = global_conf.copy()
    conf.update(local_conf)

    def webhook_filter(app):
        return WebhookMiddleware(app, conf)

    return webhook_filter
