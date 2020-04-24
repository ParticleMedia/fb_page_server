FROM ubuntu:xenial

RUN groupadd -g 1001 services
RUN useradd  -g 1001 -u 1001 -m services

COPY output /home/services/nonlocal-indexer
RUN chown -R services:services /home/services/nonlocal-indexer

WORKDIR /home/services/nonlocal-indexer/bin

USER services
