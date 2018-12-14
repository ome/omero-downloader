# Docker build
# ------------
# This dockerfile splits the build and distribution
# of the Java code to provide a slimmed down final
# image.

# By default, building this dockerfile will use
# the IMAGE argument below for the runtime image.
ARG BUILD_IMAGE=maven:3-jdk-8

# To build code with other runtimes
# pass a build argument, e.g.:
#
#   docker build --build-arg BUILD_IMAGE=openjdk:9 ...
#

# The produced distribution will be copied to the
# RUN_IMAGE for end-use. This value can also be
# set at build time with --build-arg RUN_IMAGE=...
ARG RUN_IMAGE=openjdk:8-slim

FROM ${BUILD_IMAGE} as build
USER root
RUN useradd -ms /bin/bash build
COPY --chown=build:build README.md /home/build/
COPY --chown=build:build LICENSE.txt /home/build/
COPY --chown=build:build pom.xml /home/build/
COPY --chown=build:build src /home/build/src
COPY --chown=build:build download.* /home/build/
USER build
WORKDIR /home/build
RUN mvn dependency:tree
RUN mvn package -DskipTests

FROM ${RUN_IMAGE} as run
RUN useradd -ms /bin/bash omero
USER omero
COPY --from=build /home/build/download.sh /usr/local/bin/omero-downloader
COPY --from=build /home/build/target/downloader-jar-with-dependencies.jar /usr/local/bin
RUN mkdir /tmp/downloads
ENTRYPOINT ["/usr/local/bin/omero-downloader"]
# Environment variable for -d could default to downloads as well
WORKDIR /tmp/downloads
VOLUME /tmp/downloads
