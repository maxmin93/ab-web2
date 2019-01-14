** Package (compressed)
- agens-browser-web-1.0.jar (ver 1.0, compiled at 2018-03-02)
- agens-browser.config.yml
- readme.txt

** Requirement
- JRE 8 or over (JAVA 1.8)
- AgensGraph 1.2 or over (try connection test by your ID/PW to JDBC-URL and graph_path)
- Web browser : Chrome, Firefox, Safari, Edge (except IE)
- check server.port in 'agens-browser.config.yml' (default port: 8085)

** Manual
http://bitnine.net/documentations/agensbrowser-manual-1.0-en.html


====================================
** How to run executable jar file
====================================

# for Linux/Mac
1. change directory which was decompressed zip-file
2. type command directly
    $ java -jar agens-browser-web-1.0.jar --spring.config.name=agens-browser.config
3. open web-browser and link 'http://localhost:<port>'

# for Windows
1. open cmd window (Cmd+R and type 'cmd')
2. change directory which was decompressed zip-file
3. type command directly
    $ java -jar agens-browser-web-1.0.jar --spring.config.name=agens-browser.config
4. open web-browser and link 'http://localhost:<port>'
