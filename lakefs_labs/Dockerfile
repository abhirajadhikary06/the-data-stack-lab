FROM treeverse/lakefs:latest

COPY requirements.txt /tmp/requirements.txt

USER root

RUN apk add --no-cache python3 py3-pip \
	&& pip3 install --no-cache-dir --break-system-packages -r /tmp/requirements.txt

USER lakefs

EXPOSE 8000
