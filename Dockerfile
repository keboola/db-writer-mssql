FROM quay.io/keboola/docker-base-php56:0.0.2
MAINTAINER Miroslav Cillik <miro@keboola.com>

RUN curl https://packages.microsoft.com/config/rhel/7/prod.repo > /etc/yum.repos.d/mssql-release.repo
RUN yum -y update
RUN yum -y remove unixODBC-utf16 unixODBC-utf16-devel
RUN ACCEPT_EULA=Y yum -y install msodbcsql-13.1.4.0-1
RUN ACCEPT_EULA=Y yum -y install mssql-tools unixODBC-devel
RUN yum -y --enablerepo=epel,remi,remi-php56 install wget php-mssql php-common php-pecl-xdebug

# Fix locale
ENV LC_ALL "en_US.UTF-8"
RUN localedef -v -c -i en_US -f UTF-8 en_US.UTF-8; exit 0

# FreeTDS driver
ADD driver/freetds.conf /etc/freetds.conf

# Initialize
ADD . /code
WORKDIR /code

RUN composer selfupdate
RUN composer install --no-interaction

# Path
ENV PATH $PATH:/opt/mssql-tools/bin

CMD php ./run.php --data=/data
