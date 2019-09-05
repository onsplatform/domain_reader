FROM domain-base

RUN git clone https://github.com/onsplatform/domain_reader.git /src

WORKDIR src
RUN pip install pipenv

RUN pipenv install --system --clear

EXPOSE 80
CMD gunicorn domain.app:api --bind 0.0.0.0:80
