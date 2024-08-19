
# Doesn't working
#FROM golang:1.20 as build

#COPY . /cmd

#WORKDIR /cmd

#RUN CGO_ENABLED=0 GOOS=linux go build -o kvs

FROM scratch

COPY --from=build /cmd/kvs .

COPY --from=build /cmd/*.pem .

EXPOSE 8080

CMD ["/kvs"]
