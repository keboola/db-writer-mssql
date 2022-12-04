FROM keboola/db-component-ssh-proxy:latest AS sshproxy
FROM php:7.4-cli-buster
ARG DEBIAN_FRONTEND=noninteractive
ARG COMPOSER_FLAGS="--prefer-dist --no-interaction"
ENV COMPOSER_ALLOW_SUPERUSER=1
ENV COMPOSER_PROCESS_TIMEOUT 3600

RUN apt-get update -q \
  && apt-get install -y --no-install-recommends \
  unzip git apt-transport-https wget ssh libxml2-dev gnupg2 unixodbc unixodbc-dev libgss3

RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
  && curl https://packages.microsoft.com/config/debian/9/prod.list > /etc/apt/sources.list.d/mssql-release.list \
  && apt-get update \
  && ACCEPT_EULA=Y apt-get install -y msodbcsql17=17.5.2.1-1 mssql-tools=17.5.2.1-1

RUN pecl install pdo_sqlsrv sqlsrv \
  && docker-php-ext-enable sqlsrv pdo_sqlsrv \
  && docker-php-ext-install xml

RUN echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc \
  && /bin/bash -c "source ~/.bashrc"

ENV PATH $PATH:/opt/mssql-tools/bin

# Fix SSL configuration to be compatible with older servers
RUN \
    # https://wiki.debian.org/ContinuousIntegration/TriagingTips/openssl-1.1.1
    sed -i 's/CipherString\s*=.*/CipherString = DEFAULT@SECLEVEL=1/g' /etc/ssl/openssl.cnf \
    # https://stackoverflow.com/questions/53058362/openssl-v1-1-1-ssl-choose-client-version-unsupported-protocol
    && sed -i 's/MinProtocol\s*=.*/MinProtocol = TLSv1/g' /etc/ssl/openssl.cnf

RUN echo "memory_limit = -1" >> /usr/local/etc/php/php.ini

WORKDIR /root

RUN curl -sS https://getcomposer.org/installer | php \
  && mv composer.phar /usr/local/bin/composer

WORKDIR /code

## Composer - deps always cached unless changed
# First copy only composer files
COPY composer.* /code/
# Download dependencies, but don't run scripts or init autoloaders as the app is missing
RUN composer install $COMPOSER_FLAGS --no-scripts --no-autoloader
# copy rest of the app
COPY . /code/
# run normal composer - all deps are cached already
RUN composer install $COMPOSER_FLAGS

COPY --from=sshproxy /root/.ssh /root/.ssh
CMD php ./run.php --data=/data
