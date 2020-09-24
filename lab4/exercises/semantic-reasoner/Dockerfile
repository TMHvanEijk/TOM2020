FROM tomcat:7.0.103-jdk8-corretto

COPY ./reasoner-api.war /usr/local/tomcat/webapps/reasoner-api.war
EXPOSE 8080
ENV environment docker

ENV graphdb  http://graph-db:7200
ENV bugPredictorServer  http://tosca-smell:8080/
ENV ansiblePath  /usr/local/tomcat/webapps/Ansibles/
ENV reasonerServer  http://semantic-web:8080/


RUN mkdir -p /usr/local/tomcat/webapps/Ansibles

CMD ["catalina.sh","run"]