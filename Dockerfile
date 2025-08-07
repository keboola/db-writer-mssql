FROM php:8.2-cli-bullseye

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

# Microsoft ODBC installation using EXACT approach from Microsoft docs
RUN curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/11/prod bullseye main" > /etc/apt/sources.list.d/mssql-release.list

# Install dependencies in correct order per Microsoft docs
RUN apt-get update && apt-get install -y --no-install-recommends \
    libxml2-dev \
    unixodbc-dev \
    && rm -rf /var/lib/apt/lists/*

# Install ODBC driver - try 18, fallback to 17 per Microsoft docs
RUN apt-get update \
    && (ACCEPT_EULA=Y apt-get install -y msodbcsql18 || ACCEPT_EULA=Y apt-get install -y msodbcsql17) \
    && (ACCEPT_EULA=Y apt-get install -y mssql-tools18 || ACCEPT_EULA=Y apt-get install -y mssql-tools) \
    && apt-get install -y unixodbc-dev \
    && rm -rf /var/lib/apt/lists/*

# Verify ODBC installation as per Microsoft troubleshooting guide
RUN odbcinst -j \
    && odbcinst -q -d \
    && (ls -la /opt/microsoft/msodbcsql18/lib64/ || ls -la /opt/microsoft/msodbcsql17/lib64/) \
    && (ldd /opt/microsoft/msodbcsql18/lib64/libmsodbcsql-18.*.so.*.* || ldd /opt/microsoft/msodbcsql17/lib64/libmsodbcsql-17.*.so.*.*) || true

# Install PHP extensions (latest version for PHP 8.2)
RUN pecl install pdo_sqlsrv-5.12.0 sqlsrv-5.12.0 \
  && docker-php-ext-enable sqlsrv pdo_sqlsrv \
  && docker-php-ext-install xml

# Set path for both possible tool versions
ENV PATH="$PATH:/opt/mssql-tools18/bin:/opt/mssql-tools/bin"

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
