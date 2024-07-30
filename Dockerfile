# Based on Ubuntu 18.04, Python 3.6.9
FROM afni/afni_make_build

WORKDIR /
USER root
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get install -y wget gnupg libxml2 iproute2 && \
    apt-get install -y dcm2niix && \
    apt-get install -y inkscape && \
    apt-get remove -y wget && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

USER $CONTAINER_USER
RUN echo "user $USER uid $UID"
RUN python3 -m pip install --upgrade pip
RUN python3 -m pip install pipenv setuptools
RUN which pip
RUN python3 -m pip show pipenv
RUN bash -c "ls /opt/user_pip_packages/lib/python3.6/site-packages|grep pip"

WORKDIR /app/realtimefmri

COPY Makefile /app/realtimefmri
COPY Pipfile /app/realtimefmri
COPY Pipfile.lock /app/realtimefmri

ENV PIPENV_DONT_USE_PYENV 1
ENV PIPENV_SYSTEM 1

RUN make requirements

# install master from pycortex github (otherwise, 915ed736 works)
#RUN pip3 install git+https://github.com/gallantlab/pycortex.git
# `pip3 install` tries to install into system path, which does not work as a
# regular user. Build manually until then.
RUN mkdir /tmp/pycortex_compile && cd /tmp/pycortex_compile && git clone --depth=1 --filter=blob:none -q https://github.com/gallantlab/pycortex.git && cd pycortex && pip3 install -r requirements.txt && python3 setup.py build && python3 setup.py install --prefix "$PYTHONUSERBASE" && cd "$HOME" && rm -r /tmp/pycortex_compile
RUN mkdir -p $HOME/.config/pycortex
RUN python3 -c "import cortex"
COPY data/pycortex-options.cfg $HOME/.config/pycortex/options.cfg
RUN pip3 install tornado==4.3

EXPOSE 8050

COPY docker-entrypoint.sh /app/realtimefmri

USER 0
ENTRYPOINT ["/app/realtimefmri/docker-entrypoint.sh"]
