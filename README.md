Delta Lake I/O Utilities
========================

# References
* [Delta Lake](https://delta.io)
* [How to install Python virtual environments with Pyenv and `pipenv`](http://github.com/machine-learning-helpers/induction-python/tree/master/installation/virtual-env)
* [Induction to Delta Lake](https://github.com/bom4v/induction-delta-lake)

# Overview
[That repository](https://github.com/bom4v/delta-lake-io) aims to provide
simple utilities to store and retrieve data from/into Delta Lake
into/from CSV data files.

# Installation
A convenient way to get the Spark ecosystem and CLI (command-line interface)
tools (_e.g._, `spark-submit`, `spark-shell`, `spark-sql`, `beeline`,
`pyspark` and `sparkR`) is through
[PySpark](https://spark.apache.org/docs/latest/api/python/pyspark.html).
PySpark is a Python wrapper around Spark libraries, run through
a Java Virtual Machine (JVM) handily provided by
[OpenJDK](https://openjdk.java.net).

To guarantee full reproducibility with the Python stack, `pyenv`
and `pipenv` are used here.
Also, `.python_version` and `Pipfile` are part of the project.
The user has therefore just to install `pyenv` and `pipenv`,
and then all the commands described in this document become easily
accessible and reproducible.

Follow the instructions on
[how-to install Python and Java for Spark](http://github.com/machine-learning-helpers/induction-python/tree/master/installation/virtual-env)
for more details. Basically:
* The `pyenv` utility is installed from
  [GitHub](https://github.com/pyenv/pyenv.git)
* Specific versions of Python (namely, at the time of writing, 2.7.15
  and 3.7.2) are installed in the user account file-system.
  Those specific Python frameworks provide the `pip` package management tool.
  often, the version of `pip` coming with the Python framework is outdated
  and needs to be upgraded; the `-U` option in the `pip install` command
  line is intended to install (if not already installed) or upgrade
  (if an older version is already present)
* The Python `pipenv` package is installed thanks to `pip`
* The `.python_version`, `Pipfile` and `Pipfile.lock` files, specific
  per project folder, fully drive the versions of all the Python packages
  and of Python itself, so as to guarantee full reproducibility
  on all the platforms

## Clone the Git repository
```bash
$ mkdir -p ~/dev/infra && cd ~/dev/infra
$ git clone https://github.com/bom4v/delta-lake-io.git
$ cd ~/dev/infra/delta-lake-io
```

## Utilities to analyze JSON content

### Debian/Ubuntu
```bash
$ sudo aptitude -y install jq
```

### Fedora/CentOS/RedHat
```bash
$ sudo dnf -y install jq
```

### MacOS
```bash
$ brew install jq
```

## Java
* Once an OpenJDK JVM has been installed, specify `JAVA_HOME` accordingly
  in `~/.bashrc`
  
* Maven also needs to be installed

### Debian/Ubuntu
```bash
$ sudo aptitude -y install openjdk-8-jdk maven
$ export JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64"
```

### Fedora/CentOS/RedHat
```bash
$ sudo dnf -y install java-1.8.0-openjdk maven
$ export JAVA_HOME="/usr/lib/jvm/java-1.8.0-openjdk"
```

### MacOS
* Visit https://jdk.java.net/8/, download and install the MacOS package
```bash
$ export JAVA_HOME=$(/usr/libexec/java_home -v 1.8)
$ brew install maven
```

## Utilities to analyze Parquet files

### Debian/Ubuntu
```bash
$ sudo aptitude -y install parquet-tools
```

### Fedora/CentOS/RedHat
```bash
$ sudo dnf -y install parquet-tools
```

### MacOS
```bash
$ brew install parquet-tools
```

## SBT
[Download and install SBT](https://www.scala-sbt.org/download.html)

## Python-related dependencies
* `pyenv`:
```bash
$ git clone https://github.com/pyenv/pyenv.git ${HOME}/.pyenv
$ cat >> ~/.bashrc << _EOF

# Pyenv
# git clone https://github.com/pyenv/pyenv.git \${HOME}/.pyenv
export PATH=\${PATH}:\${HOME}/.pyenv/bin
if command -v pyenv 1>/dev/null 2>&1
then
  eval "\$(pyenv init -)"
fi

_EOF
$ . ~/.bashrc
$ pyenv install 2.7.15 && pyenv install 3.7.2
```

* `pip` and `pipenv`:
```bash
$ cd ~/dev/infra/delta-lake-io
$ pyenv versions
  system
  2.7.15
* 3.7.2 (set by ~/dev/infra/delta-lake-io/.python-version)
$ python --version
Python 3.7.2
$ pip install -U pip pipenv
$ pipenv --rm
$ pipenv install
Creating a virtualenv for this project...
Pipfile: ~/dev/infra/delta-lake-io/Pipfile
Using ~/.pyenv/versions/3.7.2/bin/python (3.7.2) to create virtualenv...
⠇ Creating virtual environment...Using base prefix '~/.pyenv/versions/3.7.2'
New python executable in ~/.local/share/virtualenvs/delta-lake-io-nFz46YtK/bin/python
Installing setuptools, pip, wheel...
done.
Running virtualenv with interpreter ~/.pyenv/versions/3.7.2/bin/python
✔ Successfully created virtual environment! 
Virtualenv location: ~/.local/share/virtualenvs/delta-lake-io-nFz46YtK
Installing dependencies from Pipfile.lock (d2363d)...
  �   ▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉ 2/2 — 00:00:20
To activate this project's virtualenv, run pipenv shell.
Alternatively, run a command inside the virtualenv with pipenv run.
```

# Use the command-line script
* Download the application JAR artefact from a
  [Maven repository](https://repo1.maven.org/maven2/):
```bash
$ wget https://oss.sonatype.org/content/groups/public/org/bom4v/ti/delta-lake-io_2.11/0.0.1-spark2.4/delta-lake-io_2.11-0.0.1-spark2.4.jar
```

* Launch the JAR application:
```bash
$ spark-submit --packages io.delta:delta-core_2.11:0.1.0 --master yarn --deploy-mode client --class org.bom4v.ti.DeltaLakeTutorial delta-lake-io_2.11-0.0.1-spark2.4.jar
```

# Contribute to that project

## Simple test
* Compile and package the SQL-based data extractor:
```bash
$ sbt 'set isSnapshot := true' compile package publishM2 publishLocal
[info] Loading settings for project global-plugins from gpg.sbt ...
[info] Loading global plugins from ~/.sbt/1.0/plugins
[info] Loading settings for project delta-lake-io-build from plugins.sbt ...
[info] Loading project definition from ~/dev/infra/delta-lake-io/project
[info] Loading settings for project delta-lake-io from build.sbt ...
[info] Set current project to delta-lake-io (in build file:~/dev/infra/delta-lake-io/)
[info] Executing in batch mode. For better performance use sbt's shell
[info] Defining isSnapshot
[info] The new value will be used by makeIvyXmlConfiguration, makeIvyXmlLocalConfiguration and 5 others.
[info] 	Run `last` for details.
[info] Reapplying settings...
[info] Set current project to delta-lake-io (in build file:~/dev/infra/delta-lake-io/)
[success] Total time: 1 s, completed Apr 25, 2019 6:59:10 PM
[success] Total time: 0 s, completed Apr 25, 2019 6:59:10 PM
[info] Packaging ~/dev/infra/delta-lake-io/target/scala-2.11/delta-lake-io_2.11-0.0.1-spark2.4-sources.jar ...
[info] Done packaging.
[info] Wrote ~/dev/infra/delta-lake-io/target/scala-2.11/delta-lake-io_2.11-0.0.1-spark2.4.pom
[info] Main Scala API documentation to ~/dev/infra/delta-lake-io/target/scala-2.11/api...
model contains 5 documentable templates
[info] Main Scala API documentation successful.
[info] Packaging ~/dev/infra/delta-lake-io/target/scala-2.11/delta-lake-io_2.11-0.0.1-spark2.4-javadoc.jar ...
[info] Done packaging.
[info] 	published delta-lake-io_2.11 to file:~/.m2/repository/org/bom4v/ti/delta-lake-io_2.11/0.0.1-spark2.4/delta-lake-io_2.11-0.0.1-spark2.4.pom
[info] 	published delta-lake-io_2.11 to file:~/.m2/repository/org/bom4v/ti/delta-lake-io_2.11/0.0.1-spark2.4/delta-lake-io_2.11-0.0.1-spark2.4.jar
[info] 	published delta-lake-io_2.11 to file:~/.m2/repository/org/bom4v/ti/delta-lake-io_2.11/0.0.1-spark2.4/delta-lake-io_2.11-0.0.1-spark2.4-sources.jar
[info] 	published delta-lake-io_2.11 to file:~/.m2/repository/org/bom4v/ti/delta-lake-io_2.11/0.0.1-spark2.4/delta-lake-io_2.11-0.0.1-spark2.4-javadoc.jar
[success] Total time: 3 s, completed Apr 25, 2019 6:59:13 PM
[info] Wrote ~/dev/infra/delta-lake-io/target/scala-2.11/delta-lake-io_2.11-0.0.1-spark2.4.pom
[info] Main Scala API documentation to ~/dev/infra/delta-lake-io/target/scala-2.11/api...
model contains 5 documentable templates
[info] Main Scala API documentation successful.
[info] Packaging ~/dev/infra/delta-lake-io/target/scala-2.11/delta-lake-io_2.11-0.0.1-spark2.4-javadoc.jar ...
[info] Done packaging.
[info] :: delivering :: org.bom4v.ti#delta-lake-io_2.11;0.0.1-spark2.4 :: 0.0.1-spark2.4 :: integration :: Thu Apr 25 18:59:15 EEST 2019
[info] 	delivering ivy file to ~/dev/infra/delta-lake-io/target/scala-2.11/ivy-0.0.1-spark2.4.xml
[info] 	published delta-lake-io_2.11 to ~/.ivy2/local/org.bom4v.ti/delta-lake-io_2.11/0.0.1-spark2.4/poms/delta-lake-io_2.11.pom
[info] 	published delta-lake-io_2.11 to ~/.ivy2/local/org.bom4v.ti/delta-lake-io_2.11/0.0.1-spark2.4/jars/delta-lake-io_2.11.jar
[info] 	published delta-lake-io_2.11 to ~/.ivy2/local/org.bom4v.ti/delta-lake-io_2.11/0.0.1-spark2.4/srcs/delta-lake-io_2.11-sources.jar
[info] 	published delta-lake-io_2.11 to ~/.ivy2/local/org.bom4v.ti/delta-lake-io_2.11/0.0.1-spark2.4/docs/delta-lake-io_2.11-javadoc.jar
[info] 	published ivy to ~/.ivy2/local/org.bom4v.ti/delta-lake-io_2.11/0.0.1-spark2.4/ivys/ivy.xml
[success] Total time: 1 s, completed Apr 25, 2019 6:59:15 PM
```

* The above command generates JAR artefacts (mainly
  `delta-lake-io_2.11-0.0.1-spark2.4.jar`) locally in the project
  `target` directory, as well as in the Maven and Ivy2 user repositories
  (`~/.m2` and `~/.ivy2` respectively).

* The `set isSnapshot := true` option allows to silently override
  any previous versions of the JAR artefacts in the Maven and Ivy2 repositories
  
* Check that the artefacts have been produced
  + Locally (`package` command):
```bash
$ ls -laFh target/scala-2.11/delta-lake-io_2.11-0.0.1-spark2.4.jar
-rw-r--r-- 1 user group 26K Jun 3 12:16 target/scala-2.11/delta-lake-io_2.11-0.0.1-spark2.4.jar
```

  + In the local Maven repository (`publishM2` task):
```bash
$ ls -laFh ~/.m2/repository/org/bom4v/ti/delta-lake-io_2.11/0.0.1-spark2.4/delta-lake-io_2.11-0.0.1-spark2.4.jar
-rw-r--r-- 1 user group 26K Jun 3 12:16 ~/.m2/repository/org/bom4v/ti/delta-lake-io_2.11/0.0.1-spark2.4/delta-lake-io_2.11-0.0.1-spark2.4.jar
```

  + In the local Ivy2 repository (`publishLocal` task):
```bash
$ ls -laFh ~/.ivy2/local/org.bom4v.ti/delta-lake-io_2.11/0.0.1-spark2.4/jars/delta-lake-io_2.11.jar
-rw-r--r-- 1 user group 26K Jun 3 12:16 ~/.ivy2/local/org.bom4v.ti/delta-lake-io_2.11/0.0.1-spark2.4/jars/delta-lake-io_2.11.jar
```

* Clean any previous data:
```bash
$ mkdir -p /tmp/delta-lake && rm -rf /tmp/delta-lake/table.dlk
```

* The `DeltaLakeStorer` class takes two parameters:
  + A CSV input data file, by default
    [`data/cdr_example.csv`](https://github.com/bom4v/delta-lake-io/blob/master/data/cdr_example.csv).
	Spark reads that CSV data file, fills a
	[Spark DataFrame](https://spark.apache.org/docs/latest/sql-programming-guide.html)
	and stores the result into the Delta Lake
  + The file-path of the Delta Lake for those data, by default
    `/tmp/delta-lake/table.dlk`. Note that the `.dlk` extension is needed
	when passing that file-path as a command-line parameter, as it so far
	corresponds to the way that class parses the command-line to retrieve
	that parameter.
  + If called without parameters, the default values, as specified above,
    are used

* Launch the storing job in the SBT JVM:
```bash
$ ls -lFh data/cdr_example.csv
-rw-r--r--  1 user  staff  26K Jun  3 19:08 data/cdr_example.csv
$ sbt "runMain org.bom4v.ti.DeltaLakeStorer"
[info] Loading settings for project global-plugins from gpg.sbt ...
[info] Loading global plugins from ~/.sbt/1.0/plugins
[info] Loading settings for project delta-lake-io-build from plugins.sbt ...
[info] Loading project definition from ~/dev/infra/delta-lake-io/project
[info] Loading settings for project delta-lake-io from build.sbt ...
[info] Set current project to delta-lake-io (in build file:~/dev/infra/delta-lake-io/)
[warn] Multiple main classes detected.  Run 'show discoveredMainClasses' to see the list
[info] Running org.bom4v.ti.DeltaLakeStorer 
Spark: 2.4.3
Scala: version 2.11.12
File-path for the CSV data file: data/cdr_example.csv
File-path for the Delta Lake table/data-frame: /tmp/delta-lake/table.dlk
...
[success] Total time: 19 s, completed Jun 3, 2019 7:16:41 PM
```

* It generates parquet files in the `/tmp/delta-lake/table.dlk` directory:
```bash
$ ls -laFh /tmp/delta-lake/table.dlk
total 32
drwxr-xr-x  5 yser  group   160B Jun  3 19:16 ./
drwxr-xr-x  3 yser  group    96B Jun  3 19:16 ../
-rw-r--r--  1 yser  group    80B Jun  3 19:16 .part-00000-f4e1f062-63d3-488e-a04a-ae7282d43348-c000.snappy.parquet.crc
drwxr-xr-x  4 yser  group   128B Jun  3 19:16 _delta_log/
-rw-r--r--  1 yser  group   9.0K Jun  3 19:16 part-00000-f4e1f062-63d3-488e-a04a-ae7282d43348-c000.snappy.parquet
```

* Launch the job with `spark-submit`
  + In local mode (for instance, on a laptop; that mode may not always work
    on the Spark/Hadoop clusters):
```bash
$ mkdir -p /tmp/delta-lake && rm -rf /tmp/delta-lake/table.dlk
$ pipenv run spark-submit --packages io.delta:delta-core_2.11:0.1.0 --master local --class org.bom4v.ti.DeltaLakeStorer target/scala-2.11/delta-lake-io_2.11-0.0.1-spark2.4.jar
2019-06-03 20:22:46 INFO  SparkContext:54 - Running Spark version 2.4.3
2019-06-03 20:22:46 INFO  SparkContext:54 - Submitted application: StandaloneQuerylauncher
...
Spark: 2.4.3
Scala: version 2.11.12
...
2019-06-03 20:22:47 INFO  SparkContext:54 - Successfully stopped SparkContext
2019-06-03 20:22:47 INFO  ShutdownHookManager:54 - Shutdown hook called
...
```

  + In Yarn cluster client mode with the standalone version (that method
    is basically the same as above):
```bash
$ pipenv run spark-submit --packages io.delta:delta-core_2.11:0.1.0 --num-executors 1 --executor-memory 512m --master yarn --deploy-mode client --class org.bom4v.ti.DeltaLakeStorer target/scala-2.11/delta-lake-io_2.11-0.0.1-spark2.4.jar
...
Spark: 2.4.3
Scala: version 2.11.12
...
```

* The `DeltaLakeRetriever` class takes two parameters:
  + The file-path of the Delta Lake for the data to be retrieved, by default
    `/tmp/delta-lake/table.dlk`. Note that the `.dlk` extension is needed
	when passing that file-path as a command-line parameter, as it so far
	corresponds to the way that class parses the command-line to retrieve
	that parameter.
  + A CSV output data file, by default `data-extract.csv`. Note that this
    parameter is so far ignored. Indeed, Spark creates a full directory,
    `delta-extract-tmp` here (hard-coded in the class), containing CSV
	chunk files, which all together	represent the retrieved data
	from the Delta Lake.
	Those CSV chunk files may be re-assembled by a script, for instance
    [`tools/lakeRetrieve.sh`](https://github.com/bom4v/delta-lake-io/blob/master/tools/lakeRetrieve.sh)
	(not fully working yet).
  + If called without parameters, the default values, as specified above,
    are used

* Launch the retrieval job in the SBT JVM, generating chunk CSV files
  in the `delta-extract-tmp` directory:
```bash
$ sbt "runMain org.bom4v.ti.DeltaLakeRetriever"
[info] Loading settings for project global-plugins from gpg.sbt ...
[info] Loading global plugins from ~/.sbt/1.0/plugins
[info] Loading settings for project delta-lake-io-build from plugins.sbt ...
[info] Loading project definition from ~/dev/infra/delta-lake-io/project
[info] Loading settings for project delta-lake-io from build.sbt ...
[info] Set current project to delta-lake-io (in build file:~/dev/infra/delta-lake-io/)
[warn] Multiple main classes detected.  Run 'show discoveredMainClasses' to see the list
[info] Running org.bom4v.ti.DeltaLakeRetriever 
Spark: 2.4.3
Scala: version 2.11.12
File-path for the Delta Lake table/data-frame: /tmp/delta-lake/table.dlk
File-path for the expected CSV file: delta-extract.csv
[success] Total time: 27 s, completed Jun 3, 2019 7:28:53 PM
$ ls -lFh delta-extract-tmp/
total 56
-rw-r--r--   1 user  staff     0B Jun  3 19:58 _SUCCESS
-rw-r--r--   1 user  staff    28K Jun  3 20:00 part-00000-xxx-c000.csv
$ sed -ie s/\"\"//g delta-extract-tmp/part-00000-xxx-c000.csv
$ wc -l delta-extract-tmp/part-00000-xxx-c000.csv data/cdr_example.csv 
     157 delta-extract-tmp/part-00000-xxx-c000.csv
     156 data/cdr_example.csv
     313 total
$ rm -rf delta-extract-tmp
```

* Launch the job with `spark-submit`
  + In local mode (for instance, on a laptop; that mode may not always work
    on the Spark/Hadoop clusters):
```bash
$ mkdir -p /tmp/delta-lake && rm -rf /tmp/delta-lake/table.dlk
$ pipenv run spark-submit --packages io.delta:delta-core_2.11:0.1.0 --master local --class org.bom4v.ti.DeltaLakeRetriever target/scala-2.11/delta-lake-io_2.11-0.0.1-spark2.4.jar
2019-06-03 20:22:46 INFO  SparkContext:54 - Running Spark version 2.4.3
2019-06-03 20:22:46 INFO  SparkContext:54 - Submitted application: StandaloneQuerylauncher
...
Spark: 2.4.3
Scala: version 2.11.12
...
2019-06-03 20:22:47 INFO  SparkContext:54 - Successfully stopped SparkContext
2019-06-03 20:22:47 INFO  ShutdownHookManager:54 - Shutdown hook called
...
```

  + In Yarn cluster client mode with the standalone version (that method
    is basically the same as above):
```bash
$ pipenv run spark-submit --packages io.delta:delta-core_2.11:0.1.0 --num-executors 1 --executor-memory 512m --master yarn --deploy-mode client --class org.bom4v.ti.DeltaLakeRetriever target/scala-2.11/delta-lake-io_2.11-0.0.1-spark2.4.jar
...
Spark: 2.4.3
Scala: version 2.11.12
...
```

* Playing with Python on PySpark:
```bash
$ pipenv run pyspark --packages io.delta:delta-core_2.11:0.1.0
Python 3.7.3 (default, Mar 27 2019, 09:23:15) 
[Clang 10.0.1 (clang-1001.0.46.3)] on darwin
Type "help", "copyright", "credits" or "license" for more information.
Ivy Default Cache set to: ~/.ivy2/cache
The jars for the packages stored in: ~/.ivy2/jars
:: loading settings :: url = jar:file:~/.local/share/virtualenvs/delta-lake-io-x9Z2QpTc/lib/python3.7/site-packages/pyspark/jars/ivy-2.4.0.jar!/org/apache/ivy/core/settings/ivysettings.xml
io.delta#delta-core_2.11 added as a dependency
:: resolving dependencies :: org.apache.spark#spark-submit-parent-29330cf6-fe6e-49c1-95c1-a988cbf2b4af;1.0
	confs: [default]
	found io.delta#delta-core_2.11;0.1.0 in spark-list
:: resolution report :: resolve 171ms :: artifacts dl 3ms
	:: modules in use:
	io.delta#delta-core_2.11;0.1.0 from spark-list in [default]
	---------------------------------------------------------------------
	|                  |            modules            ||   artifacts   |
	|       conf       | number| search|dwnlded|evicted|| number|dwnlded|
	---------------------------------------------------------------------
	|      default     |   1   |   0   |   0   |   0   ||   1   |   0   |
	---------------------------------------------------------------------
:: retrieving :: org.apache.spark#spark-submit-parent-29330cf6-fe6e-49c1-95c1-a988cbf2b4af
	confs: [default]
	0 artifacts copied, 1 already retrieved (0kB/3ms)
2019-06-03 20:08:40,075 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 2.4.3
      /_/

Using Python version 3.7.3 (default, Mar 27 2019 09:23:15)
SparkSession available as 'spark'.
>>> df = spark.read.format("delta").load("/tmp/delta-lake/table.dlk")
>>> df.show(3)
+--------------------------+--------------------+--------+----------------------+-----------------+------+---------+--------------+---------------+---------+---------------+---------------+-----------------------+-------------+-----------------+-------------------+-----------------+-----------------+------------------+------------------+-----------+-----------+----------+------------+---------------+-----------------+------------------------+-------------+---------------+----------------+-------------+-----------+-------------+------------+------+------+--------------+
|specificationVersionNumber|releaseVersionNumber|fileName|fileAvailableTimeStamp|fileUtcTimeOffset|sender|recipient|sequenceNumber|callEventsCount|eventType|           imsi|           imei|callEventStartTimeStamp|utcTimeOffset|callEventDuration|causeForTermination|accessPointNameNI|accessPointNameOI|dataVolumeIncoming|dataVolumeOutgoing|sgsnAddress|ggsnAddress|chargingId|chargeAmount|teleServiceCode|bearerServiceCode|supplementaryServiceCode|dialledDigits|connectedNumber|thirdPartyNumber|callingNumber|recEntityId|callReference|locationArea|cellId|msisdn|servingNetwork|
+--------------------------+--------------------+--------+----------------------+-----------------+------+---------+--------------+---------------+---------+---------------+---------------+-----------------------+-------------+-----------------+-------------------+-----------------+-----------------+------------------+------------------+-----------+-----------+----------+------------+---------------+-----------------+------------------------+-------------+---------------+----------------+-------------+-----------+-------------+------------+------+------+--------------+
|                         2|                   1|    null|   2017-04-26 12:11:29|             -400| FRAKS|    ITAUT|        304561|           null|      mtc|250209890003854|355587045959660|    2017-04-26 19:01:54|          300|                0|                  0|             null|             null|              null|              null|       null|       null|      null|           0|             21|             null|                    null|         null|           null|            null|  39043490004|33672054372|         null|        null|  null|  null|          null|
|                         2|                   1|    null|   2017-04-26 12:11:29|             -400| FRAKS|    ITAUT|        304561|           null|      mtc|250209890003854|355587045959660|    2017-04-26 19:02:09|          300|                0|                  0|             null|             null|              null|              null|       null|       null|      null|           0|             21|             null|                    null|         null|           null|            null|  39043490004|33672054372|         null|        null|  null|  null|          null|
|                         2|                   1|    null|   2017-04-26 12:11:29|             -400| FRAKS|    ITAUT|        304561|           null|      mtc|250209890003854|355587045959660|    2017-04-26 19:02:19|          300|                0|                  0|             null|             null|              null|              null|       null|       null|      null|           0|             21|             null|                    null|         null|           null|            null|  39043490004|33672054372|         null|        null|  null|  null|          null|
+--------------------------+--------------------+--------+----------------------+-----------------+------+---------+--------------+---------------+---------+---------------+---------------+-----------------------+-------------+-----------------+-------------------+-----------------+-----------------+------------------+------------------+-----------+-----------+----------+------------+---------------+-----------------+------------------------+-------------+---------------+----------------+-------------+-----------+-------------+------------+------+------+--------------+
only showing top 3 rows

>>> df = spark.read.format("delta").option("versionAsOf", 0).load("/tmp/delta-lake/table.dlk")
>>> df.show(3)
+--------------------------+--------------------+--------+----------------------+-----------------+------+---------+--------------+---------------+---------+---------------+---------------+-----------------------+-------------+-----------------+-------------------+-----------------+-----------------+------------------+------------------+-----------+-----------+----------+------------+---------------+-----------------+------------------------+-------------+---------------+----------------+-------------+-----------+-------------+------------+------+------+--------------+
|specificationVersionNumber|releaseVersionNumber|fileName|fileAvailableTimeStamp|fileUtcTimeOffset|sender|recipient|sequenceNumber|callEventsCount|eventType|           imsi|           imei|callEventStartTimeStamp|utcTimeOffset|callEventDuration|causeForTermination|accessPointNameNI|accessPointNameOI|dataVolumeIncoming|dataVolumeOutgoing|sgsnAddress|ggsnAddress|chargingId|chargeAmount|teleServiceCode|bearerServiceCode|supplementaryServiceCode|dialledDigits|connectedNumber|thirdPartyNumber|callingNumber|recEntityId|callReference|locationArea|cellId|msisdn|servingNetwork|
+--------------------------+--------------------+--------+----------------------+-----------------+------+---------+--------------+---------------+---------+---------------+---------------+-----------------------+-------------+-----------------+-------------------+-----------------+-----------------+------------------+------------------+-----------+-----------+----------+------------+---------------+-----------------+------------------------+-------------+---------------+----------------+-------------+-----------+-------------+------------+------+------+--------------+
|                         2|                   1|    null|   2017-04-26 12:11:29|             -400| FRAKS|    ITAUT|        304561|           null|      mtc|250209890003854|355587045959660|    2017-04-26 19:01:54|          300|                0|                  0|             null|             null|              null|              null|       null|       null|      null|           0|             21|             null|                    null|         null|           null|            null|  39043490004|33672054372|         null|        null|  null|  null|          null|
|                         2|                   1|    null|   2017-04-26 12:11:29|             -400| FRAKS|    ITAUT|        304561|           null|      mtc|250209890003854|355587045959660|    2017-04-26 19:02:09|          300|                0|                  0|             null|             null|              null|              null|       null|       null|      null|           0|             21|             null|                    null|         null|           null|            null|  39043490004|33672054372|         null|        null|  null|  null|          null|
|                         2|                   1|    null|   2017-04-26 12:11:29|             -400| FRAKS|    ITAUT|        304561|           null|      mtc|250209890003854|355587045959660|    2017-04-26 19:02:19|          300|                0|                  0|             null|             null|              null|              null|       null|       null|      null|           0|             21|             null|                    null|         null|           null|            null|  39043490004|33672054372|         null|        null|  null|  null|          null|
+--------------------------+--------------------+--------+----------------------+-----------------+------+---------+--------------+---------------+---------+---------------+---------------+-----------------------+-------------+-----------------+-------------------+-----------------+-----------------+------------------+------------------+-----------+-----------+----------+------------+---------------+-----------------+------------------------+-------------+---------------+----------------+-------------+-----------+-------------+------------+------+------+--------------+
only showing top 3 rows

>>> quit()
```

* House keeping:
```bash
$ rm -rf /tmp/delta-lake/table.dlk
$ rm -rf delta-extract-tmp
```

