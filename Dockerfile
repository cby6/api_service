FROM continuumio/anaconda3:latest
COPY ./test.py /usr/local/python/
COPY ./service_account.json /usr/local/python/
COPY ./requirements.txt /usr/local/python/
EXPOSE 7000
WORKDIR /usr/local/python/
RUN pip install -r requirements.txt
CMD python main.py

