#!/usr/bin/env bash
set -euo pipefail

# This script will test if the webhook middleware is working correctly.

webhook="https://slack.com/api/api.test"

# Test that the webhooks work
echo "Testing webhook middleware"
echo "--------------------------"

# get creds
echo -n "Acquiring an authentication token from OpenStack Swift... "
auth_token=$(curl -sSLi \
    -H 'X-Storage-User: test:tester' \
    -H 'X-Storage-Pass: testing' \
    http://127.0.0.1:8080/auth/v1.0 \
    | grep 'X-Auth-Token:' | awk '{print $2}' | tr -d '\r'
) && echo "OK" || { echo "ERROR: failed to generate auth token"; exit 1; }
#echo "Token: ${auth_token}"

echo ""
echo -n "Getting storage URL... "
storage_url=$(curl -sSLi \
    -H 'X-Storage-User: test:tester' \
    -H 'X-Storage-Pass: testing' \
    http://127.0.0.1:8080/auth/v1.0 \
    | grep 'X-Storage-Url:' | awk '{print $2}' | tr -d '\r'
) && echo "${storage_url}" || { echo "ERROR: failed to obtain storage url"; exit 1; }
echo ""

# validate callback url is available
echo -n "Checking if test webhook [${webhook}] is valid... "
response_code=$(curl -sSLD - ${webhook} -o /dev/null | head -n 1 | awk '{print $2}') || { echo "ERROR: Site not available"; exit 1; }
[ ${response_code} -ge 200 ] && [ ${response_code} -lt 300 ] && echo "OK" || { echo "ERROR: Bad response code: ${response_code}"; exit 1; }
echo ""

# create random container with webhook
container_name=$(openssl rand -hex 32)
webhook_auth=$(openssl rand -hex 32)

echo -n "Creating container [${container_name}]... "
create_container=$(curl -X PUT \
     -H "X-Auth-Token: ${auth_token}" \
     -H "X-Webhook: ${webhook}" \
     -H "X-Webhook-Auth: ${webhook_auth}" \
     -sSLD - ${storage_url}/${container_name} -o /dev/null \
     | head -n 1 | awk '{print $2}'
) || { echo "ERROR: Failed to make request"; exit 1; }
[ ${create_container} -ge 200 ] && [ ${create_container} -lt 300 ] && echo "OK" || { echo "ERROR: FAILED"; exit 1; }
echo ""

# View container
echo -n "Checking for container webhook... "
container_webhook=$(curl -X GET \
     -H "X-Auth-Token: ${auth_token}" \
     -sSLD - ${storage_url}/${container_name} -o /dev/null \
    | grep -i 'X-Webhook:' | tr -d '\r'
) && { echo "${container_webhook}"; } || { echo "ERROR: NOT FOUND"; exit 1; }
echo ""

# Delete webhook
echo -n "Attempting to delete container webhook... "
delete_webhook=$(curl -X PUT \
     -H "X-Auth-Token: ${auth_token}" \
     -H "X-Remove-Webhook: ${webhook}" \
     -sSLD - ${storage_url}/${container_name} -o /dev/null \
     | head -n 1 | awk '{print $2}'
)
[ ${delete_webhook} -ge 200 ] && [ ${delete_webhook} -lt 300 ] && echo "OK" || { echo "FAILED"; exit 1; }
echo -n "Verifying webhook was deleted successfully... "
container_webhook=$(curl -X GET \
     -H "X-Auth-Token: ${auth_token}" \
     -sSLD - ${storage_url}/${container_name} -o /dev/null \
    | grep -i 'X-Webhook:' | tr -d '\r'
) && { echo "ERROR: webhookk was not deleted."; exit 1; } || echo "OK"
echo ""

# Delete container
echo -n "Deleting test container [${container_name}]... "
delete_container=$(curl -X DELETE \
     -H "X-Auth-Token: ${auth_token}" \
     -sSLD - ${storage_url}/${container_name} -o /dev/null \
     | head -n 1 | awk '{print $2}'
) && [ ${delete_container} -lt 300 ] && echo "OK" || { echo "FAILED"; exit 1; }
echo ""

echo "Webhook test complete: PASS"
echo ""

exit 0
