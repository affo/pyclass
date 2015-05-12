FROM python:2.7-onbuild
ENV PYTHONPATH /usr/src/app
CMD [ "python", "distop/callee.py", "172.17.42.1" ]