
# AutoComplete

## Setup Hadoop Environment

### Option 1: Docker 

Install Docker first.

```sh
$ sudo apt-get update
$ sudo apt-get install wget
$ sudo curl -sSL https://get.daocloud.io/docker | sh
$ sudo docker info
```

Install Hadoop in Docker

```sh
$ mkdir Project
$ cd Project
$ sudo docker pull joway/hadoop-cluster # pull docker image from dockerhub
$ git clone https://github.com/joway/hadoop-cluster-docker # clone code from github
$ sudo docker network create --driver=bridge hadoop #create hadoop network
$ cd hadoop-cluster-docker
```

Running Docker

```sh
$ sudo ./start-container.sh
$ sudo ./start-hadoop.sh
```


### Option 2: VMWare WorkStation Pro with CentOS images

You can refer [Tutorial](https://www.bilibili.com/video/BV1Qp4y1n7EN?p=18) for more details.


## MySQL

+ Install MySQL
+ Create database

```sql
create database test;
use test;
-- Be very careful with the following field names. The field names should be consistent with your JAVA code.
create table output(starting_phrase VARCHAR(250), following_word VARCHAR(250), count INT);
```

+ Setup Privileges

```sql
GRANT ALL PRIVILEGES ON *.* TO 'root'@'%';
FLUSH PRIVILEGES;
```

+ Checkout the MySQL port

```sql
show variables where variable_name = 'port';   -- in my case, it is 3306
```

+ Test connection with MySQL

Intellij support for setting up and testing database connections in the IDE.


![img](https://i.imgur.com/x12CGhV.png)

You can refer [Tutorial](https://www.bilibili.com/video/BV1pK4y1U7CK?from=search&seid=12979324692000101371) for more details.









