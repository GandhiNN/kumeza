FROM public.ecr.aws/lambda/python:3.9 as builder

RUN yum update -y
RUN yum install wget tar gzip zip gcc make gcc gcc-c++ unixODBC unixODBC-devel -y

# Dependencies of freeTDS
RUN yum install gnutls ncurses-compat-libs -y
RUN curl https://packages.microsoft.com/config/rhel/7/prod.repo | tee /etc/yum.repos.d/mssql-release.repo
RUN ACCEPT_EULA=Y yum install -y msodbcsql17
RUN ACCEPT_EULA=Y yum install -y mssql-tools

# Download FreeTDS package from EPEL 7 mirror (it's not supported anymore)
RUN wget https://dl.fedoraproject.org/pub/archive/epel/7/x86_64/Packages/f/freetds-1.4.16-1.el7.x86_64.rpm -P /tmp
RUN wget https://dl.fedoraproject.org/pub/archive/epel/7/x86_64/Packages/f/freetds-devel-1.4.16-1.el7.x86_64.rpm -P /tmp
RUN wget https://dl.fedoraproject.org/pub/archive/epel/7/x86_64/Packages/f/freetds-doc-1.4.16-1.el7.noarch.rpm -P /tmp
RUN wget https://dl.fedoraproject.org/pub/archive/epel/7/x86_64/Packages/f/freetds-libs-1.4.16-1.el7.x86_64.rpm -P /tmp

# Build the FreeTDS package
RUN cd /tmp && rpm -i freetds*

# Notes
# Location of build tdsodbc -> /usr/lib64/libtdsodbc.so.0.0.0

# Modified steps:
# docker build -t libtdsodbc .
# docker run --rm --entrypoint bash -v $PWD:/local libtdsodbc -c "cp -R /usr/lib64/libtdsodbc.so.0.0.0 /local"