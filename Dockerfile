FROM node:0.12.0
MAINTAINER Meteorhacks

COPY . /app
RUN cd /app && npm install

CMD ["node", "/app/index.js"]
