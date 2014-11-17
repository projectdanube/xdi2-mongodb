<a href="http://projectdanube.org/" target="_blank"><img src="http://projectdanube.github.com/xdi2/images/projectdanube_logo.png" align="right"></a>
<img src="http://projectdanube.github.com/xdi2/images/logo64.png"><br>

This is a project to use the [MongoDB](http://mongodb.org/) document database as backend storage for the [XDI2](http://github.com/projectdanube/xdi2) server.

### Information

* [Code Example](https://github.com/projectdanube/xdi2-mongodb/wiki/Code%20Example)
* [Server Configuration Example](https://github.com/projectdanube/xdi2-mongodb/wiki/Server%20Configuration%20Example)
* [Graph Factory Flags](https://github.com/projectdanube/xdi2-mongodb/wiki/Graph%20Factory%20Flags)

### How to build

First, you need to build the main [XDI2](http://github.com/projectdanube/xdi2) project.

After that, just run

    mvn clean install

To build the XDI2 plugin.

### How to run as standalone web application

    mvn clean install jetty:run -P war

Then access the web interface at

	http://localhost:9992/

Or access the server's status page at

	http://localhost:9992/xdi

Or use an XDI client to send XDI messages to

    http://localhost:9992/xdi/graph

### Maven Dependency

&lt;dependency&gt;
    &lt;groupId&gt;xdi2&lt;/groupId&gt;
    &lt;artifactId&gt;xdi2-mongodb&lt;/artifactId&gt;
    &lt;version&gt;${xdi2-mongodb-version}&lt;/version&gt;
    &lt;scope&gt;compile&lt;/scope&gt;
&lt;/dependency&gt;

### Community

Google Group: http://groups.google.com/group/xdi2

IRC: irc://irc.freenode.net:6667/xdi
