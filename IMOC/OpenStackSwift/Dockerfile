# Uses OpenStack Swift All-In-One image (SAIO) and adds webhook capabilities as middleware
# http://docs.openstack.org/developer/swift/development_saio.html

FROM stmuraka/saio
MAINTAINER Shaun Murakami (stmuraka@us.ibm.com)

# Copy in webhook files; backup originals
RUN mv /etc/swift/proxy-server.conf /etc/swift/proxy-server.conf.orig
COPY Webhook/etc/swift/proxy-server.conf /etc/swift/
COPY Webhook/swift/common/middleware/webhook.py /home/${SWIFT_USER}/swift/swift/common/middleware/
COPY Webhook/webhook_test.sh /home/${SWIFT_USER}/swift/.webhooktests

# Add webhook tests
RUN echo '# Run webhook tests\n\
echo "---------------------------------------------------"\n\
echo "Running Swift Webhook Tests..."\n\
echo "---------------------------------------------------"\n\
${HOME}/swift/.webhooktests\n\
echo "Swift Webhook test complete"\n\
echo ""\n'\
>> /home/${SWIFT_USER}/runTests.sh

# Fix ownership
RUN sudo chown ${SWIFT_USER}.${SWIFT_USER} /etc/swift/proxy-server.conf \
 && sudo chown ${SWIFT_USER}.${SWIFT_USER} /home/${SWIFT_USER}/swift/swift/common/middleware/webhook.py \
 && sudo chown ${SWIFT_USER}.${SWIFT_USER} /home/${SWIFT_USER}/swift/.webhooktests
