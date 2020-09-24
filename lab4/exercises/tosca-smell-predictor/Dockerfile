FROM tomcat:8.0.51-jre8-alpine

COPY ./bug-predictor-api.war /usr/local/tomcat/webapps/bug-predictor-api.war
EXPOSE 8080
ENV graphdb http://graph-db:7200

CMD ["catalina.sh","run"]