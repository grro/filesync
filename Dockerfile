FROM python:3-alpine

ENV dir "/etc/filesync"


RUN cd /etc
RUN mkdir app
WORKDIR /etc/app
ADD *.py /etc/app/
ADD requirements.txt /etc/app/.
RUN pip install -r requirements.txt

CMD python /etc/app/filesync_service.py $dir



