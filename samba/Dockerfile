FROM python:3.11-bullseye

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install samba -y
RUN mkdir -p /usr/local/samba/var/
ADD smb.conf /etc/samba/smb.conf

RUN pip install redis

RUN adduser --disabled-password --gecos "" rtfmri
RUN mkdir /mnt/scanner
RUN chown -R rtfmri:rtfmri /mnt/scanner
RUN mkdir /logs
RUN chown -R rtfmri:rtfmri /logs

EXPOSE 139
EXPOSE 445

WORKDIR /
ADD run.sh run.sh
RUN chmod u+x run.sh

ADD detect_dicoms.py detect_dicoms.py
CMD ["/run.sh"]
