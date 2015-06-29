# test-data-control

Imported from code.google.com/p/test-data-control

### Introduction

test-data-control is initially supposed to provide round-trip tools that will help you get data out of database and back in again, in JSON format (www.json.org). At present, it rests on top of dbunit (www.dbunit.org).
It may be combined with other tools, such as XStream or even xstream-java-code-writer (http://code.google.com/p/xstream-java-code-writer/) to further enhance and speed up your tests.
With time, I will add more components that is related to controlling test data and speeding up automated tests.


### How to Use

If you need to know how to use the library, you're best advised to look at the unit tests, in particular:
  * *`DbWrapperTest`*: here's how you'd import/export/delete database data, JSON and XML
  * *`DbWrapperJavaTest` / `JavaDemoTest`*: example use of several classes - how you'd import/export/delete database data


### More on testing

I have used the wiki here to put down some thoughts on testing, and a couple of presentations. I have put some links into [http://www.delicious.com/ParaTom/testing delicious]

## Procedure for releasing.

Deploying snapshots and releasing is done through:
  * Maven [http://maven.apache.org/plugins/maven-release-plugin/examples/prepare-release.html release] plugin.
  * Sonatype OSS Repository (see [https://docs.sonatype.org/display/Repository/Sonatype+OSS+Maven+Repository+Usage+Guide Usage guide])
A boiled-down, simple procedure with some additional notes is found below

### Details

Start with a tested, checked-in, clean project.

Useful commands:
  * mvn release:clean
  * mvn release:prepare -Dpassword=(...) -Dusername=(...)
  * mvn release:perform

*NB* separate [https://code.google.com/hosting/settings googlecode.com password] (under "Source" tab).

If anything goes wrong - remember that pom is changed and checked in, so you need to revert version and scm settings at least, you may try release:rollback.

If all is well, you should try and test the thing before you go on to the Sonatype [https://oss.sonatype.org/ staging repository] to *[https://docs.sonatype.org/display/Repository/Sonatype+OSS+Maven+Repository+Usage+Guide#SonatypeOSSMavenRepositoryUsageGuide-8.ReleaseIt close and release]* the staging repository
Once released it will be synched to Maven central (hourly)
