FROM ubuntu:18.04

# Tools
RUN apt-get update && apt-get install -y \
    gdb \
    git \
    vim \ 
    wget \
    && rm -rf /var/lib/apt/lists/*

# Apache arrow
RUN apt update
RUN apt install -y -V apt-transport-https lsb-release
RUN wget -O /usr/share/keyrings/apache-arrow-keyring.gpg https://dl.bintray.com/apache/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-keyring.gpg
RUN echo 'deb [arch=amd64 signed-by=/usr/share/keyrings/apache-arrow-keyring.gpg] https://dl.bintray.com/apache/arrow/ubuntu/ bionic main' >> /etc/apt/sources.list.d/apache-arrow.list
RUN echo 'deb-src [signed-by=/usr/share/keyrings/apache-arrow-keyring.gpg] https://dl.bintray.com/apache/arrow/ubuntu/ bionic main' >> /etc/apt/sources.list.d/apache-arrow.list

RUN apt update
RUN apt install -y -V libarrow-dev 
RUN apt install -y -V libarrow-glib-dev 
RUN apt install -y -V libplasma-dev 
RUN apt install -y -V libplasma-glib-dev 
RUN apt install -y -V libgandiva-dev 
RUN apt install -y -V libgandiva-glib-dev
RUN apt install -y -V libparquet-dev 
RUN apt install -y -V libparquet-glib-dev

# Go installation
RUN cd /tmp && \
    wget https://dl.google.com/go/go1.12.5.linux-amd64.tar.gz && \
    tar -C /usr/local -xzf go1.12.5.linux-amd64.tar.gz && \
    rm go1.12.5.linux-amd64.tar.gz
ENV PATH="/usr/local/go/bin:${PATH}"


# Python bindings
RUN cd /tmp && \
    wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh && \
    bash Miniconda3-latest-Linux-x86_64.sh -b -p /miniconda && \
    rm Miniconda3-latest-Linux-x86_64.sh
ENV PATH="/miniconda/bin:${PATH}"
RUN conda install -c conda-forge -y pyarrow=0.13.0 numpy Cython ipython

RUN git clone git://github.com/apache/arrow.git /src/arrow
RUN cd /src/arrow && git checkout apache-arrow-0.13.0
WORKDIR /src/carrow
