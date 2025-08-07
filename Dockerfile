FROM php:8.2-cli-bookworm

ARG DEBIAN_FRONTEND=noninteractive
ARG COMPOSER_FLAGS="--prefer-dist --no-interaction"
ENV COMPOSER_ALLOW_SUPERUSER=1
ENV COMPOSER_PROCESS_TIMEOUT=3600

WORKDIR /code/

COPY docker/php-prod.ini /usr/local/etc/php/php.ini
COPY docker/composer-install.sh /tmp/composer-install.sh

# Install basic dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    locales \
    unzip \
    ssh \
    apt-transport-https \
    wget \
    gnupg2 \
    libgss3 \
    && rm -rf /var/lib/apt/lists/*

# Install build dependencies including unixODBC first
RUN apt-get update && apt-get install -y --no-install-recommends \
    libxml2-dev \
    unixodbc \
    unixodbc-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Microsoft ODBC Driver (Driver 18 - only version available for Debian 12)
RUN curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends \
    msodbcsql18=18.5.1.1-1 \
    mssql-tools18=18.4.1.1-1 \
    && rm -rf /var/lib/apt/lists/* \
    && cp /opt/microsoft/msodbcsql18/etc/odbcinst.ini /etc/odbcinst.ini

# Install PHP extensions (officially supported for PHP 8.2)
RUN pecl install pdo_sqlsrv-5.11.1 sqlsrv-5.11.1 \
  && docker-php-ext-enable sqlsrv pdo_sqlsrv \
  && docker-php-ext-install xml

# Set path (mssql-tools18 for Driver 18)
ENV PATH="$PATH:/opt/mssql-tools18/bin"

# Fix SSL configuration
RUN \
    sed -i 's/CipherString\s*=.*/CipherString = DEFAULT@SECLEVEL=1/g' /etc/ssl/openssl.cnf \
    && sed -i 's/MinProtocol\s*=.*/MinProtocol = TLSv1/g' /etc/ssl/openssl.cnf

# Composer
COPY composer.* /code/
RUN chmod +x /tmp/composer-install.sh && /tmp/composer-install.sh
RUN composer install $COMPOSER_FLAGS --no-scripts --no-autoloader
COPY . /code/
RUN composer install $COMPOSER_FLAGS

CMD ["php", "./src/run.php"]
