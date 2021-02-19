FROM ubuntu:xenial

RUN groupadd -g 1001 services
RUN useradd  -g 1001 -u 1001 -m services

COPY output /home/services/fb_page_server
RUN chown -R services:services /home/services/fb_page_server

WORKDIR /home/services/fb_page_server/bin

USER services
