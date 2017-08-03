FROM ubuntu:16.10
MAINTAINER Miroslav Cillik <miro@keboola.com>

# apt-get and system utilities
RUN apt-get update && apt-get install -y \
    curl wget apt-utils apt-transport-https debconf-utils gcc build-essential g++-5 openssh-server git\
    && rm -rf /var/lib/apt/lists/*

# adding custom MS repository
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/ubuntu/16.10/prod.list > /etc/apt/sources.list.d/mssql-release.list

# install SQL Server drivers
RUN apt-get update && ACCEPT_EULA=Y apt-get install -y unixodbc-dev msodbcsql

# install SQL Server tools
RUN apt-get update && ACCEPT_EULA=Y apt-get install -y mssql-tools
RUN echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc
RUN /bin/bash -c "source ~/.bashrc"

# php libraries
RUN apt-get update && apt-get install -y \
    php7.0 libapache2-mod-php7.0 mcrypt php7.0-mcrypt php-mbstring php-pear php7.0-dev php-curl \
    --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# install necessary locales
RUN apt-get update && apt-get install -y locales \
    && echo "en_US.UTF-8 UTF-8" > /etc/locale.gen \
    && locale-gen

# install SQL Server PHP connector module
RUN pecl install sqlsrv pdo_sqlsrv

# initial configuration of SQL Server PHP connector
RUN echo "extension=/usr/lib/php/20151012/sqlsrv.so" >> /etc/php/7.0/cli/php.ini
RUN echo "extension=/usr/lib/php/20151012/pdo_sqlsrv.so" >> /etc/php/7.0/cli/php.ini

# Composer
WORKDIR /root
RUN cd \
  && curl -sS https://getcomposer.org/installer | php \
  && ln -s /root/composer.phar /usr/local/bin/composer

# Initialize
ADD . /code
WORKDIR /code
RUN echo "memory_limit = -1" >> /etc/php.ini
RUN composer selfupdate && composer install --no-interaction

# Path
ENV PATH $PATH:/opt/mssql-tools/bin

CMD php ./run.php --data=/data
