#!/bin/bash
# java -classpath WeatherStation2MQTT.jar com.uwingstech.Main -b test.mosquitto.org -t "FORMOSA00001/WeatherStation01/rx"

java -jar WeatherStation2MQTT.jar -b test.mosquitto.org -t "FORMOSA00001/WeatherStation01/rx"
