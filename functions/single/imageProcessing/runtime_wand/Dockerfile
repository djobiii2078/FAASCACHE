FROM openwhisk/python3action:latest

RUN apk add --update imagemagick imagemagick-dev

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt
RUN pip install python-swiftclient

RUN export MAGICK_HOME=/usr

ENV AWS_ACCESS_KEY_ID=AKIA56WI7EZGDCE3CQVR
ENV AWS_SECRET_ACCESS_KEY=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
