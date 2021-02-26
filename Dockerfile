FROM scratch
COPY main .
COPY data .
WORKDIR /data

CMD ["/main"]