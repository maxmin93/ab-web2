# AgensBrowser-web version

AgensBrowser-web applicaiton :

Web DB Client and Visualizaiton Tool for AgensGraph v1.3

## Prerequisites

* Java SE 8 / Java 1.8
* Maven 3

## Download source

git clone -b prod_2.0 https://github.com/bitnine-oss/agens-browser-web prod_2.0

cd prod_2.0

## Integration Build (Backend+Frontend)

mvn clean install -DskipTests

## Running the app

cd backend\target
vi agens-config.yml
```
## Edit information for connecting AgensGraph DB
server:
  port: 8085
agens:
  outer:
	datasource:
	  url: jdbc:postgresql://<host>:<port>/<db>
	  username: <user_id>
	  password: <user_pw>
```
java -jar agensbrowser-web-2.0.jar --spring.config.name=agens-config

## open WebBrowser

http://localhost:8085

## Built With

* [Spring Boot](https://projects.spring.io/spring-boot/)
* [Maven](https://maven.apache.org/)
