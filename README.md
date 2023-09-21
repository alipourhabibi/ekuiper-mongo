# MongoDB plugin for ekuiper
This project contains some(one) MongoDB portable plugin(s) for [ekuiper](https://github.com/lf-edge/ekuiper).

## Note
- in rules set Send single to False!

## Install
The pre-build zip file available at releases.

## Build yourself
clone the project. \
Build the project with:
```sh
go build -o mongo .
```
Zip the files:
```sh
zip mongo.zip mongo mongo.json sinks -r
```
Serve the file:
```sh
python -m http.server
```
And then in ekuiper dashboard install this portable plugin using the following url:
```
http://${YOURIP}:8000/mongo.zip
```
And set up the rules and here you go...!
