FROM php:7.0
ENV DEBIAN_FRONTEND noninteractive
ENV COMPOSER_ALLOW_SUPERUSER=1

RUN apt-get update -q \
  && apt-get install unzip git apt-transport-https wget ssh libxml2-dev -y --no-install-recommends

RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
  && curl https://packages.microsoft.com/config/debian/8/prod.list > /etc/apt/sources.list.d/mssql-release.list \
  && apt-get update -q \
  && ACCEPT_EULA=Y apt-get install msodbcsql mssql-tools -y \
  && apt-get install unixodbc-dev -y

RUN pecl install sqlsrv \
  && pecl install pdo_sqlsrv \
  && docker-php-ext-enable sqlsrv pdo_sqlsrv \
  && docker-php-ext-install xml

RUN echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc \
  && /bin/bash -c "source ~/.bashrc"

RUN echo "memory_limit = -1" >> /usr/local/etc/php/php.ini

WORKDIR /root

RUN curl -sS https://getcomposer.org/installer | php \
  && mv composer.phar /usr/local/bin/composer

ADD . /code
WORKDIR /code

RUN composer install --no-interaction

ENV PATH $PATH:/opt/mssql-tools/bin

CMD php ./run.php --data=/data
