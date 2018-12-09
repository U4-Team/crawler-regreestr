FROM python:3.7
ENV PYTHONUNBUFFERED 1
ARG REQUIREMENTS_FILE=requirements.txt

RUN mkdir /crawler-yandex-search
WORKDIR /crawler-yandex-search

ADD ${REQUIREMENTS_SRC_PATH} /crawler-template/
RUN pip install -e .
RUN pip install --no-cache-dir -r ${REQUIREMENTS_FILE}
ADD . /crawler-template/
